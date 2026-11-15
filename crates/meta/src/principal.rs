//! The cell's principals and their access keys, kept in its root range (docs/design/principals.md;
//! research/91), the control plane's store (architecture §9, item 5). A principal is a user of the
//! cell's account as IAM has one: a name, a unique ID never reused, one identity policy, and at
//! most two access keys, each an id and a secret wrapped under the cell's root key.
//!
//! Every node reads a key's row from here by ReadIndex as a request signed with it arrives, so a
//! key made or revoked by a committed command is the one every node judges by from then on:
//! a revocation takes effect at once across the cell, never one configuration at a time.
//!
//! The commands come from the node that took an operator's request, which checked the name's form
//! and the policy (`mantle_s3::policy::identity`) and wrapped the secret; applying them checks
//! the bounds IAM states, so the rows stay within them whatever a command carries.

use crate::engine::{Rows, Write};
use crate::error::{MetaError, reserved};
use crate::key;
use crate::record::{self, AccessKeyRow, Principal};
use mantle_s3::account::{MAX_KEYS, MAX_PRINCIPALS, valid_key_id, valid_name};

/// A principal's identity policy as a row keeps it at most, in octets: its compact form's 2,048
/// characters (`mantle_s3::policy::IDENTITY_CHARS`), each at most four octets in UTF-8 (RFC 3629
/// §3), as an escape in the document may stand for any character.
pub const MAX_POLICY: usize = 4 * mantle_s3::policy::IDENTITY_CHARS;

/// A principal made: its name, its unique ID and its identity policy's compact form; it is made
/// at the entry's time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewPrincipal {
    pub name: String,
    pub id: String,
    pub policy: String,
    pub at_ns: u64,
}

/// An access key made for `principal`: its id, its secret wrapped under the root key's
/// `generation`; with `rotating`, only while the principal holds exactly one key, the one it
/// replaces. It is made at the entry's time.
#[derive(Clone, PartialEq, Eq)]
pub struct NewKey {
    pub principal: String,
    pub id: String,
    pub generation: u32,
    pub wrapped: [u8; mantle_s3::seal::WRAPPED_SECRET],
    pub rotating: bool,
    pub at_ns: u64,
}

impl std::fmt::Debug for NewKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NewKey")
            .field("principal", &self.principal)
            .field("id", &self.id)
            .field("generation", &self.generation)
            .field("rotating", &self.rotating)
            .finish_non_exhaustive()
    }
}

/// The commands on the account's principals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// IAM's CreateUser with its inline policy (PutUserPolicy).
    Create(NewPrincipal),
    /// IAM's PutUserPolicy: the principal's identity policy replaced.
    Policy { name: String, policy: String },
    /// IAM's DeleteUser, refused while it holds a key.
    Delete { name: String },
    /// IAM's CreateAccessKey.
    Key(NewKey),
    /// IAM's DeleteAccessKey: the key ends at once, for every node.
    Revoke { id: String },
}

/// What became of a command on the principals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The principal was made.
    Made,
    /// The name, case aside, is a principal's already; or the key's id is a key's already.
    Taken,
    /// The account holds as many principals as IAM allows one, or the principal as many keys
    /// (IAM's LimitExceeded): nothing changed.
    Full,
    /// The principal's policy was replaced.
    Set,
    /// The principal or the key was removed.
    Removed,
    /// No principal of that name, or no key of that id: nothing changed.
    Unknown,
    /// The principal holds keys, which must be revoked before it is deleted (IAM's
    /// DeleteConflict): nothing changed.
    Held,
    /// The key was made; a rotation names the key it replaces, which stays until it is revoked.
    KeyMade { replacing: Option<String> },
    /// A rotation of a principal holding no key, which has nothing to rotate: nothing changed.
    Unrotatable,
    /// A name, an id or a policy out of its form or past its bound: nothing changed.
    Invalid,
}

/// The command's outcome and the writes it makes.
pub(crate) fn run<E: Rows>(
    engine: &mut E,
    command: &Command,
) -> Result<(Outcome, Vec<Write>), MetaError> {
    match command {
        Command::Create(made) => create(engine, made),
        Command::Policy { name, policy } => set_policy(engine, name, policy),
        Command::Delete { name } => delete(engine, name),
        Command::Key(made) => create_key(engine, made),
        Command::Revoke { id } => revoke(engine, id),
    }
}

fn refused(outcome: Outcome) -> Result<(Outcome, Vec<Write>), MetaError> {
    Ok((outcome, Vec::new()))
}

/// How many principals the account holds.
pub fn count<E: Rows>(engine: &mut E) -> Result<u32, MetaError> {
    match engine.get(&key::principals())? {
        None => Ok(0),
        Some(bytes) => {
            let n = record::decode_number(&bytes, "principals")?;
            u32::try_from(n).map_err(|_| MetaError::Corrupt)
        }
    }
}

fn create<E: Rows>(
    engine: &mut E,
    made: &NewPrincipal,
) -> Result<(Outcome, Vec<Write>), MetaError> {
    if !valid_name(&made.name) || !valid_key_id(&made.id) || made.policy.len() > MAX_POLICY {
        return refused(Outcome::Invalid);
    }
    if engine.get(&key::principal(&made.name))?.is_some() {
        return refused(Outcome::Taken);
    }
    let held = count(engine)?;
    if held >= MAX_PRINCIPALS {
        return refused(Outcome::Full);
    }
    let principal = Principal {
        name: made.name.clone(),
        id: made.id.clone(),
        created_ns: made.at_ns,
        policy: made.policy.clone(),
        keys: Vec::new(),
    };
    let held = held.checked_add(1).ok_or(MetaError::Corrupt)?;
    let mut writes = reserved(2)?;
    writes.push(Write::Put(key::principal(&made.name), principal.encode()?));
    writes.push(Write::Put(
        key::principals(),
        record::encode_number(u64::from(held)),
    ));
    Ok((Outcome::Made, writes))
}

/// Principal `name`'s row, if it holds one.
pub fn principal<E: Rows>(engine: &mut E, name: &str) -> Result<Option<Principal>, MetaError> {
    match engine.get(&key::principal(name))? {
        None => Ok(None),
        Some(bytes) => Ok(Some(Principal::decode(&bytes)?)),
    }
}

fn set_policy<E: Rows>(
    engine: &mut E,
    name: &str,
    policy: &str,
) -> Result<(Outcome, Vec<Write>), MetaError> {
    if policy.len() > MAX_POLICY {
        return refused(Outcome::Invalid);
    }
    let Some(mut held) = principal(engine, name)? else {
        return refused(Outcome::Unknown);
    };
    policy.clone_into(&mut held.policy);
    let mut writes = reserved(1)?;
    writes.push(Write::Put(key::principal(name), held.encode()?));
    Ok((Outcome::Set, writes))
}

fn delete<E: Rows>(engine: &mut E, name: &str) -> Result<(Outcome, Vec<Write>), MetaError> {
    let Some(held) = principal(engine, name)? else {
        return refused(Outcome::Unknown);
    };
    if !held.keys.is_empty() {
        return refused(Outcome::Held);
    }
    let count = count(engine)?.checked_sub(1).ok_or(MetaError::Corrupt)?;
    let mut writes = reserved(2)?;
    writes.push(Write::Delete(key::principal(name)));
    writes.push(Write::Put(
        key::principals(),
        record::encode_number(u64::from(count)),
    ));
    Ok((Outcome::Removed, writes))
}

fn create_key<E: Rows>(engine: &mut E, made: &NewKey) -> Result<(Outcome, Vec<Write>), MetaError> {
    if !valid_key_id(&made.id) {
        return refused(Outcome::Invalid);
    }
    let Some(mut held) = principal(engine, &made.principal)? else {
        return refused(Outcome::Unknown);
    };
    if engine.get(&key::access_key(&made.id))?.is_some() {
        return refused(Outcome::Taken);
    }
    let replacing = match (made.rotating, held.keys.as_slice()) {
        (_, keys) if keys.len() >= MAX_KEYS => return refused(Outcome::Full),
        (true, []) => return refused(Outcome::Unrotatable),
        (true, [only]) => Some(only.clone()),
        (true, _) => return refused(Outcome::Full),
        (false, _) => None,
    };
    held.keys.push(made.id.clone());
    let row = AccessKeyRow {
        principal: made.principal.to_ascii_lowercase(),
        generation: made.generation,
        wrapped: made.wrapped,
        created_ns: made.at_ns,
    };
    let mut writes = reserved(2)?;
    writes.push(Write::Put(key::access_key(&made.id), row.encode()?));
    writes.push(Write::Put(key::principal(&made.principal), held.encode()?));
    Ok((Outcome::KeyMade { replacing }, writes))
}

/// Access key `id`'s row, if the account holds the key.
pub fn access_key<E: Rows>(engine: &mut E, id: &str) -> Result<Option<AccessKeyRow>, MetaError> {
    match engine.get(&key::access_key(id))? {
        None => Ok(None),
        Some(bytes) => Ok(Some(AccessKeyRow::decode(&bytes)?)),
    }
}

fn revoke<E: Rows>(engine: &mut E, id: &str) -> Result<(Outcome, Vec<Write>), MetaError> {
    let Some(row) = access_key(engine, id)? else {
        return refused(Outcome::Unknown);
    };
    // A key's principal holds it until it is revoked, and is deleted only once it holds none.
    let mut held = principal(engine, &row.principal)?.ok_or(MetaError::Corrupt)?;
    let before = held.keys.len();
    held.keys.retain(|key| key != id);
    if held.keys.len() == before {
        return Err(MetaError::Corrupt);
    }
    let mut writes = reserved(2)?;
    writes.push(Write::Delete(key::access_key(id)));
    writes.push(Write::Put(key::principal(&row.principal), held.encode()?));
    Ok((Outcome::Removed, writes))
}

/// A key as a request signed with it is judged: its row, and its principal's.
pub fn signer<E: Rows>(
    engine: &mut E,
    id: &str,
) -> Result<Option<(AccessKeyRow, Principal)>, MetaError> {
    let Some(row) = access_key(engine, id)? else {
        return Ok(None);
    };
    let held = principal(engine, &row.principal)?.ok_or(MetaError::Corrupt)?;
    Ok(Some((row, held)))
}

/// A principal as an operator lists it: its row and each of its keys' ids with when each was made.
pub type Listed = (Principal, Vec<(String, u64)>);

/// The principals after `after`, or from the first, in the order of their names without regard
/// to case, at most `max`, each with its keys; and whether more follow.
pub fn page<E: Rows>(
    engine: &mut E,
    after: Option<&str>,
    max: usize,
) -> Result<(Vec<Listed>, bool), MetaError> {
    let (mut from, to) = key::principal_rows(after);
    let mut out: Vec<Listed> = Vec::new();
    loop {
        let Some((k, v)) = engine.next(&from, &to)? else {
            return Ok((out, false));
        };
        if out.len() >= max {
            return Ok((out, true));
        }
        let held = Principal::decode(&v)?;
        let mut keys = reserved(held.keys.len())?;
        for id in &held.keys {
            let row = access_key(engine, id)?.ok_or(MetaError::Corrupt)?;
            keys.push((id.clone(), row.created_ns));
        }
        out.push((held, keys));
        from = k;
        from.push(0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Model;
    use crate::root;

    const POLICY: &str = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}"#;

    fn apply(engine: &mut Model, index: u64, command: Command) -> Outcome {
        match root::apply(engine, index, &root::Command::Principal(command)).unwrap() {
            root::Outcome::Principal(outcome) => outcome,
            other => panic!("{other:?}"),
        }
    }

    fn made(name: &str, id: &str) -> Command {
        Command::Create(NewPrincipal {
            name: name.into(),
            id: id.into(),
            policy: POLICY.into(),
            at_ns: 7,
        })
    }

    fn key(principal: &str, id: &str, rotating: bool) -> Command {
        Command::Key(NewKey {
            principal: principal.into(),
            id: id.into(),
            generation: 1,
            wrapped: [9; mantle_s3::seal::WRAPPED_SECRET],
            rotating,
            at_ns: 11,
        })
    }

    fn id(n: char) -> String {
        n.to_string().repeat(mantle_s3::account::KEY_ID_CHARS)
    }

    /// A principal made reads back with its policy; its name is one whatever its case; a key made
    /// for it reads back as its signer, and a key revoked is no signer at once.
    #[test]
    fn principals_and_keys_read_back_and_a_revoked_key_is_gone() {
        let mut engine = Model::default();
        assert_eq!(
            apply(&mut engine, 1, made("Runner-7", &id('P'))),
            Outcome::Made
        );
        assert_eq!(
            apply(&mut engine, 2, made("runner-7", &id('Q'))),
            Outcome::Taken
        );
        assert_eq!(count(&mut engine).unwrap(), 1);
        let held = principal(&mut engine, "RUNNER-7").unwrap().unwrap();
        assert_eq!(
            (held.name.as_str(), held.id.as_str(), held.created_ns),
            ("Runner-7", id('P').as_str(), 7)
        );
        assert_eq!(held.policy, POLICY);

        assert_eq!(
            apply(&mut engine, 3, key("runner-7", &id('A'), false)),
            Outcome::KeyMade { replacing: None }
        );
        let (row, of) = signer(&mut engine, &id('A')).unwrap().unwrap();
        assert_eq!(
            (row.principal.as_str(), row.generation, row.created_ns),
            ("runner-7", 1, 11)
        );
        assert_eq!(row.wrapped, [9; mantle_s3::seal::WRAPPED_SECRET]);
        assert_eq!(of.keys, [id('A')]);
        assert_eq!(
            apply(&mut engine, 4, key("runner-7", &id('A'), false)),
            Outcome::Taken
        );

        assert_eq!(
            apply(&mut engine, 5, Command::Revoke { id: id('A') }),
            Outcome::Removed
        );
        assert_eq!(signer(&mut engine, &id('A')).unwrap(), None);
        assert!(
            principal(&mut engine, "runner-7")
                .unwrap()
                .unwrap()
                .keys
                .is_empty()
        );
        assert_eq!(
            apply(&mut engine, 6, Command::Revoke { id: id('A') }),
            Outcome::Unknown
        );
    }

    /// IAM's two keys a user, and a rotation's overlap (research/91 §4): a second key while the
    /// first is held, both signers, then a third refused until one is revoked; a rotation names
    /// the one key it replaces and is refused with none or two.
    #[test]
    fn a_principal_holds_two_keys_and_a_rotation_replaces_its_one() {
        let mut engine = Model::default();
        apply(&mut engine, 1, made("svc", &id('P')));
        assert_eq!(
            apply(&mut engine, 2, key("svc", &id('A'), true)),
            Outcome::Unrotatable
        );
        assert_eq!(
            apply(&mut engine, 3, key("svc", &id('A'), false)),
            Outcome::KeyMade { replacing: None }
        );
        assert_eq!(
            apply(&mut engine, 4, key("svc", &id('B'), true)),
            Outcome::KeyMade {
                replacing: Some(id('A'))
            }
        );
        assert!(signer(&mut engine, &id('A')).unwrap().is_some());
        assert!(signer(&mut engine, &id('B')).unwrap().is_some());
        assert_eq!(
            apply(&mut engine, 5, key("svc", &id('C'), false)),
            Outcome::Full
        );
        assert_eq!(
            apply(&mut engine, 6, key("svc", &id('C'), true)),
            Outcome::Full
        );
        assert_eq!(
            apply(&mut engine, 7, Command::Revoke { id: id('A') }),
            Outcome::Removed
        );
        assert_eq!(
            principal(&mut engine, "svc").unwrap().unwrap().keys,
            [id('B')]
        );
        assert_eq!(
            apply(&mut engine, 8, key("svc", &id('C'), false)),
            Outcome::KeyMade { replacing: None }
        );
        assert_eq!(
            apply(&mut engine, 9, key("nobody", &id('D'), false)),
            Outcome::Unknown
        );
    }

    /// A principal holding a key is not deleted (IAM's DeleteConflict); one holding none is, and
    /// its name is free again; its policy is replaced in place.
    #[test]
    fn a_principal_is_deleted_only_once_it_holds_no_key() {
        let mut engine = Model::default();
        apply(&mut engine, 1, made("svc", &id('P')));
        apply(&mut engine, 2, key("svc", &id('A'), false));
        let other = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"s3:*","Resource":"*"}]}"#;
        assert_eq!(
            apply(
                &mut engine,
                3,
                Command::Policy {
                    name: "SVC".into(),
                    policy: other.into()
                }
            ),
            Outcome::Set
        );
        assert_eq!(
            principal(&mut engine, "svc").unwrap().unwrap().policy,
            other
        );
        assert_eq!(
            apply(&mut engine, 4, Command::Delete { name: "svc".into() }),
            Outcome::Held
        );
        apply(&mut engine, 5, Command::Revoke { id: id('A') });
        assert_eq!(
            apply(&mut engine, 6, Command::Delete { name: "svc".into() }),
            Outcome::Removed
        );
        assert_eq!(count(&mut engine).unwrap(), 0);
        assert_eq!(
            apply(&mut engine, 7, Command::Delete { name: "svc".into() }),
            Outcome::Unknown
        );
        assert_eq!(
            apply(
                &mut engine,
                8,
                Command::Policy {
                    name: "svc".into(),
                    policy: other.into()
                }
            ),
            Outcome::Unknown
        );
        // A name freed is taken again, under a new unique ID.
        assert_eq!(apply(&mut engine, 9, made("svc", &id('R'))), Outcome::Made);
        assert_eq!(principal(&mut engine, "svc").unwrap().unwrap().id, id('R'));
    }

    /// Out of form or past a bound, a command changes nothing; the account's principals stop at
    /// IAM's 5,000.
    #[test]
    fn what_is_out_of_form_or_past_iams_bounds_changes_nothing() {
        let mut engine = Model::default();
        assert_eq!(
            apply(&mut engine, 1, made("a b", &id('P'))),
            Outcome::Invalid
        );
        assert_eq!(apply(&mut engine, 2, made("ok", "short")), Outcome::Invalid);
        let long = Command::Create(NewPrincipal {
            name: "ok".into(),
            id: id('P'),
            policy: "x".repeat(MAX_POLICY + 1),
            at_ns: 1,
        });
        assert_eq!(apply(&mut engine, 3, long), Outcome::Invalid);
        assert_eq!(
            apply(&mut engine, 4, key("ok", "short", false)),
            Outcome::Invalid
        );
        assert_eq!(count(&mut engine).unwrap(), 0);
        engine
            .apply(
                5,
                vec![Write::Put(
                    key::principals(),
                    record::encode_number(u64::from(MAX_PRINCIPALS)),
                )],
            )
            .unwrap();
        assert_eq!(
            apply(&mut engine, 6, made("one-more", &id('P'))),
            Outcome::Full
        );
        assert!(principal(&mut engine, "one-more").unwrap().is_none());
    }

    /// Pages list every principal once, in the order of their names without regard to case, each
    /// with its keys, resuming after the last one listed.
    #[test]
    fn pages_list_each_principal_once_in_order_with_its_keys() {
        let mut engine = Model::default();
        for (at, name) in ["c", "A", "b", "D"].iter().enumerate() {
            apply(&mut engine, at as u64 + 1, made(name, &id('P')));
        }
        apply(&mut engine, 10, key("b", &id('K'), false));
        let (first, more) = page(&mut engine, None, 3).unwrap();
        let names: Vec<&str> = first.iter().map(|(p, _)| p.name.as_str()).collect();
        assert_eq!((names, more), (vec!["A", "b", "c"], true));
        assert_eq!(first[1].1, [(id('K'), 11)]);
        let (rest, more) = page(&mut engine, Some("C"), 3).unwrap();
        let names: Vec<&str> = rest.iter().map(|(p, _)| p.name.as_str()).collect();
        assert_eq!((names, more), (vec!["D"], false));
        assert_eq!(
            page(&mut engine, Some("d"), 3).unwrap(),
            (Vec::new(), false)
        );
    }
}
