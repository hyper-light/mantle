//! `mantle principal` and `mantle key` (docs/design/principals.md): the cell's principals and
//! their access keys, made, listed, rotated and revoked by an operator, asked of the cell over its
//! native protocol as the client of `--client`, which acts for the account's root (node.md §3.6).
//! Every node judges by what the root range holds from the reply on: a key revoked is refused at
//! every node at once.
//!
//! **A policy** is IAM's JSON for a user's inline policy (research/91 §3), read from a file of at
//! most IAM's 131,072 characters and checked here as the node checks it
//! (`mantle_s3::policy::identity`), so a refusal names its rule before anything is asked.
//!
//! **A key's secret** leaves the cell once, in the reply that made the key, and is written here
//! only to the file the operator names, as AWS's shared credentials file under the profile
//! `mantle init` names, owner-only, written whole and durable (`crate::files`): never to standard
//! output or a log. A key is made only into a file that does not exist yet, and a rotation writes
//! its new key over the file of the key it replaces, which stays valid until it is revoked, so
//! clients move to the new key with no request refused (research/91 §4).

use std::io::{Read as _, Write};
use std::path::{Path, PathBuf};

use mantle_client::Client;
use mantle_protocol::object::{Issued, Listing, Op, PrincipalListed, Reply, Status};

use crate::client::{Error, NO_DEADLINE, date, line};

/// What an operator asks of the account's principals.
pub enum Command {
    /// A principal made with the identity policy in `policy`.
    Create { name: String, policy: PathBuf },
    /// A principal's identity policy replaced with the one in `policy`.
    Policy { name: String, policy: PathBuf },
    /// A principal deleted, once it holds no key.
    Delete(String),
    /// The account's principals, each with its keys.
    List,
    /// A principal, its policy and its keys.
    Show(String),
    /// A key made for `principal`, written to `out`, which must not exist; with `rotating`, the
    /// principal's second key, written over `out`, replacing its one.
    Key {
        principal: String,
        out: PathBuf,
        rotating: bool,
    },
    /// The account's keys, each with its principal.
    Keys,
    /// A key revoked.
    Revoke(String),
}

/// What a node answered, done or the status it answered, as IAM names it, the error.
fn done(what: &str, reply: Reply) -> Result<Reply, Error> {
    if reply.status == Status::Done {
        Ok(reply)
    } else {
        Err(Error::Answered(what.to_owned(), reply.status))
    }
}

/// The policy document in `path`, as written: at most IAM's 131,072 characters, each at most two
/// octets (`mantle_s3::policy::DOCUMENT_BYTES`), read no further; checked as the node checks it.
fn policy(path: &Path) -> Result<String, Error> {
    let file = std::fs::File::open(path).map_err(|e| Error::File(path.to_path_buf(), e))?;
    let most = u64::try_from(mantle_s3::policy::DOCUMENT_BYTES).unwrap_or(u64::MAX);
    let mut document = Vec::new();
    file.take(most.saturating_add(1))
        .read_to_end(&mut document)
        .map_err(|e| Error::File(path.to_path_buf(), e))?;
    if u64::try_from(document.len()).unwrap_or(u64::MAX) > most {
        return Err(Error::Usage(format!(
            "{}: longer than IAM's 131,072 characters for a policy",
            path.display()
        )));
    }
    mantle_s3::policy::identity(&document)
        .map_err(|e| Error::Usage(format!("{}: {e}", path.display())))?;
    String::from_utf8(document).map_err(|_| Error::Usage(format!("{}: not UTF-8", path.display())))
}

/// Runs `command` against the cell through `client`, its lines to `out`.
pub async fn run(client: &mut Client, command: Command, out: &mut impl Write) -> Result<(), Error> {
    match command {
        Command::Create { name, policy: path } => {
            let op = Op::CreatePrincipal {
                name: name.clone(),
                policy: policy(&path)?,
            };
            done("CreatePrincipal", client.call(&op, NO_DEADLINE).await?)?;
            line(out, format_args!("made principal {name}"))
        }
        Command::Policy { name, policy: path } => {
            let op = Op::PutPolicy {
                name: name.clone(),
                policy: policy(&path)?,
            };
            done("PutPolicy", client.call(&op, NO_DEADLINE).await?)?;
            line(out, format_args!("set the policy of {name}"))
        }
        Command::Delete(name) => {
            let op = Op::DeletePrincipal { name: name.clone() };
            done("DeletePrincipal", client.call(&op, NO_DEADLINE).await?)?;
            line(out, format_args!("deleted principal {name}"))
        }
        Command::List => {
            every(client, |p| {
                let keys: Vec<&str> = p.keys.iter().map(|(id, _)| id.as_str()).collect();
                line(
                    out,
                    format_args!(
                        "{} {} {} keys {}",
                        date(p.created),
                        p.name,
                        p.id,
                        if keys.is_empty() {
                            "none".to_owned()
                        } else {
                            keys.join(",")
                        }
                    ),
                )
            })
            .await
        }
        Command::Keys => {
            every(client, |p| {
                for (id, created) in &p.keys {
                    line(out, format_args!("{} {id} {}", date(*created), p.name))?;
                }
                Ok(())
            })
            .await
        }
        Command::Show(name) => {
            let op = Op::GetPrincipal { name };
            let reply = done("GetPrincipal", client.call(&op, NO_DEADLINE).await?)?;
            let Some(Listing::Principals(page)) = reply.listing else {
                return Err(Error::Answered("GetPrincipal".into(), reply.status));
            };
            for p in &page.principals {
                line(out, format_args!("principal {}", p.name))?;
                line(out, format_args!("id {}", p.id))?;
                line(out, format_args!("created {}", date(p.created)))?;
                for (id, created) in &p.keys {
                    line(out, format_args!("key {id} {}", date(*created)))?;
                }
                if let Some(policy) = &p.policy {
                    line(out, format_args!("policy {policy}"))?;
                }
            }
            Ok(())
        }
        Command::Key {
            principal,
            out: file,
            rotating,
        } => key(client, (&principal, rotating), &file, out).await,
        Command::Revoke(id) => {
            let op = Op::RevokeKey { id: id.clone() };
            done("RevokeKey", client.call(&op, NO_DEADLINE).await?)?;
            line(out, format_args!("revoked {id}"))
        }
    }
}

/// Each of the account's principals, a page at a time, given to `each`.
async fn every(
    client: &mut Client,
    mut each: impl FnMut(&PrincipalListed) -> Result<(), Error>,
) -> Result<(), Error> {
    let mut after = None;
    loop {
        let op = Op::ListPrincipals {
            after: after.take(),
            max: u32::try_from(mantle_protocol::object::MAX_PAGE_PRINCIPALS).unwrap_or(u32::MAX),
        };
        let reply = done("ListPrincipals", client.call(&op, NO_DEADLINE).await?)?;
        let Some(Listing::Principals(page)) = reply.listing else {
            return Err(Error::Answered("ListPrincipals".into(), reply.status));
        };
        for p in &page.principals {
            each(p)?;
        }
        match page.next {
            Some(next) => after = Some(next),
            None => return Ok(()),
        }
    }
}

/// A key made for `principal` and its secret written to `file`: a new key into a file that does
/// not exist, so no credentials are overwritten; a rotation's over the file of the key it
/// replaces. A secret the cell gave that cannot be written is lost, and the key, named, is the
/// operator's to revoke.
async fn key(
    client: &mut Client,
    (principal, rotating): (&str, bool),
    file: &Path,
    out: &mut impl Write,
) -> Result<(), Error> {
    if !rotating && file.exists() {
        return Err(Error::Usage(format!(
            "{}: exists; a key is written only to a new file, a rotation over its key's",
            file.display()
        )));
    }
    let op = Op::CreateKey {
        principal: principal.to_owned(),
        rotating,
    };
    let reply = done("CreateKey", client.call(&op, NO_DEADLINE).await?)?;
    let Some(Listing::Issued(issued)) = reply.listing else {
        return Err(Error::Answered("CreateKey".into(), reply.status));
    };
    let Issued {
        id,
        secret,
        replacing,
    } = *issued;
    let credentials = mantle_s3::account::Secret::new(format!(
        "[{}]\naws_access_key_id = {id}\naws_secret_access_key = {}\n",
        crate::init::PROFILE,
        secret.as_str()
    ));
    crate::files::write_secret(file, credentials.as_bytes()).map_err(|e| {
        Error::Usage(format!(
            "key {id} made, its secret not written to {}: {e}; revoke it with `mantle key revoke {id}`",
            file.display()
        ))
    })?;
    match replacing {
        Some(old) => line(
            out,
            format_args!(
                "rotated {principal} from {old} to {id} in {}; {old} stays valid until `mantle key revoke {old}`",
                file.display()
            ),
        ),
        None => line(
            out,
            format_args!("made key {id} for {principal} in {}", file.display()),
        ),
    }
}
