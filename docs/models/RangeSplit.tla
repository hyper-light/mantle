------------------------------- MODULE RangeSplit -------------------------------
(***************************************************************************)
(* mantle's Name ranges splitting and merging while a bucket is written   *)
(* and deleted (docs/design/metadata.md §2-§3; architecture.md §5-§6).     *)
(*                                                                         *)
(* A range holds a span of the bucket's keys, a descriptor generation, the *)
(* keys it holds a version of, and the bucket's gate.  A split is one      *)
(* command in the parent's log: the parent keeps the low part, a child     *)
(* takes the high part with its keys and the parent's gate, and both take  *)
(* the next generation.  No range's id is used twice.  The directory       *)
(* learns the new descriptors later.                                       *)
(*                                                                         *)
(* A merge joins a range to the range just below it across the two ranges' *)
(* logs.  A driver reads the lower range's descriptor and freezes the      *)
(* higher range for a merge into it at that generation: the frozen range   *)
(* takes no step.  The lower range decides the merge in its own log, and   *)
(* only at that generation: it takes the frozen range's span and keys,     *)
(* keeping its own gate, if it holds no merge not yet resolved, and        *)
(* otherwise refuses it; either way its generation moves on, so the       *)
(* decision is made once and every command for the merge that comes later, *)
(* however late, changes nothing.  A driver abandons a merge not yet       *)
(* decided by moving the lower range's generation on.  A taken merge ends  *)
(* the frozen range and is then resolved; one never to be taken thaws it:  *)
(* the lower range's generation past the merge's and the merge not the one *)
(* it holds, or the lower range ended.  A range holding a merge not yet    *)
(* resolved may not be frozen, so it cannot end with the merge unresolved. *)
(* Drivers stop and are replaced at any point, and their commands arrive   *)
(* late.                                                                   *)
(*                                                                         *)
(* A writer routes by the descriptors it cached.  A range takes a write    *)
(* only for a key in its span, through an open gate, and not while it is  *)
(* frozen.  To a request routed by a descriptor it no longer matches, a    *)
(* range answers with its own descriptor, that of the child its last split *)
(* made and of the range it is merging or merged into, and never with      *)
(* data: it keeps one child, so what it keeps is bounded, and a sender     *)
(* whose descriptors no longer cover the keys reads the directory.         *)
(*                                                                         *)
(* A create attempt opens the gate of every range it knows, then           *)
(* activates the bucket.  A delete attempt closes the gate of every range  *)
(* it knows, reads each for versions, and deletes the bucket if none holds *)
(* one, or reopens the gates and restores it.  Each step of an attempt     *)
(* names the descriptor it read, and a range refuses a step whose          *)
(* generation it no longer has; the attempt then learns the range's        *)
(* answer and starts its phase again.  A gate refuses a step older than    *)
(* the attempt that last moved it, so a later attempt that takes over      *)
(* stops an earlier one.                                                   *)
(*                                                                         *)
(* FENCED FALSE drops the generation check, the rule this model exists to  *)
(* justify.  RangeSplitUnfenced.cfg must then find a write lost to a       *)
(* delete, and RangeSplitUnfencedCreate.cfg an active bucket with a range  *)
(* whose gate never opened.  DECIDEONCE FALSE lets a refusal leave the     *)
(* lower range's generation where it was, a driver thawing on the answer,  *)
(* and ONEROLE FALSE lets a range holding a merge be frozen:               *)
(* RangeMergeUnrecorded.cfg and RangeMergeTwoRoles.cfg must then each find *)
(* two ranges owning one key.                                              *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Keys,      \* the bucket's keys, as numbers in their order
          MaxSplits, \* splits the model takes
          MaxMerges, \* merges the model begins
          Creates,   \* create attempts, and
          Deletes,   \* delete attempts, as numbers in the order they begin
          FENCED,    \* whether ranges refuse steps of an older generation
          DECIDEONCE, \* whether a refusal moves the lower range's generation on
          ONEROLE    \* whether a range holding a merge not yet resolved may not be frozen

Attempts == Creates \cup Deletes

Bottom == CHOOSE k \in Keys : \A j \in Keys : k <= j
End == (CHOOSE k \in Keys : \A j \in Keys : j <= k) + 1
Ids == 1..(MaxSplits + 1)
Mids == 1..MaxMerges

VARIABLES ranges,    \* id -> the range, or one not yet made
          directory, \* the descriptors the directory holds
          cache,     \* the writer's descriptors
          bucket,    \* "none", "creating", "active", "deleting" or "deleted"
          owner,     \* the attempt that last moved the bucket's row
          coord,     \* each attempt: where it is, and what it knows
          acked,     \* keys whose last acknowledged write made a version
          splits,
          mids,      \* merges begun, each named by its count
          freezes,   \* merge -> the frozen range, its span, and what its driver read
          committed, \* merges a lower range took
          refused    \* merges a lower range answered with a refusal

vars == <<ranges, directory, cache, bucket, owner, coord, acked, splits, mids, freezes,
          committed, refused>>
merging == <<mids, freezes, committed, refused>>

Span(r) == {k \in Keys : r.lo <= k /\ k < r.hi}
Desc(i) == [id |-> i, lo |-> ranges[i].lo, hi |-> ranges[i].hi, gen |-> ranges[i].gen]
Live == {i \in Ids : ranges[i].live}
Serving(i) == ranges[i].live /\ ranges[i].frozen = 0
Covers(ds) == \A k \in Keys : \E d \in ds : k \in Span(d)

\* What a range answers a request routed by a descriptor it no longer matches: its own
\* descriptor, unless it has ended, its last child's, and that of the range it is merging or
\* merged into.
Answer(d) == {Desc(i) : i \in (IF ranges[d.id].live THEN {d.id} ELSE {}) \cup
                              ranges[d.id].kids \cup
                              (IF ranges[d.id].into = 0 THEN {} ELSE {ranges[d.id].into})}

\* Each range's newest descriptor among ds.  An attempt keeps no older one beside it: an
\* older descriptor's span can be wider, and would count keys the range no longer holds as
\* covered once the newer one's step is done.
Newest(ds) == {d \in ds : \A e \in ds : e.id = d.id => e.gen <= d.gen}

\* Whether range d.id takes a step named by descriptor d.
Current(d) == Serving(d.id) /\ (~FENCED \/ ranges[d.id].gen = d.gen)

Unmade == [lo |-> 0, hi |-> 0, gen |-> 0, keys |-> {}, gate |-> "none",
           gatt |-> 0, live |-> FALSE, made |-> FALSE, kids |-> {}, into |-> 0,
           frozen |-> 0, pending |-> 0]
Idle == [phase |-> "idle", known |-> {}, done |-> {}]

Init ==
    /\ ranges = [i \in Ids |->
                  IF i = 1 THEN [Unmade EXCEPT !.lo = Bottom, !.hi = End, !.gen = 1,
                                               !.live = TRUE, !.made = TRUE]
                  ELSE Unmade]
    /\ directory = {Desc(1)}
    /\ cache = directory
    /\ bucket = "none"
    /\ owner = 0
    /\ coord = [a \in Attempts |-> Idle]
    /\ acked = {}
    /\ splits = 0
    /\ mids = 0
    /\ freezes = [m \in Mids |-> "none"]
    /\ committed = {}
    /\ refused = {}

(***************************************************************************)
(* Writes.                                                                 *)
(***************************************************************************)

\* A write to key k through the descriptor d the writer holds for it.
Write(k, put) ==
    \E d \in cache :
        /\ k \in Span(d)
        /\ IF Serving(d.id) /\ k \in Span(ranges[d.id])
             THEN /\ ranges[d.id].gate = "open"
                  /\ ranges' = [ranges EXCEPT ![d.id].keys =
                                  IF put THEN @ \cup {k} ELSE @ \ {k}]
                  /\ acked' = IF put THEN acked \cup {k} ELSE acked \ {k}
                  /\ UNCHANGED cache
             ELSE /\ cache' = (cache \ {d}) \cup Answer(d)
                  /\ UNCHANGED <<ranges, acked>>
        /\ UNCHANGED <<directory, bucket, owner, coord, splits, merging>>

Refresh ==
    /\ cache' = directory
    /\ UNCHANGED <<ranges, directory, bucket, owner, coord, acked, splits, merging>>

(***************************************************************************)
(* Splits, and the directory catching up with them.                        *)
(***************************************************************************)

Split(i, m) ==
    /\ splits < MaxSplits
    /\ Serving(i)
    /\ m \in Keys /\ ranges[i].lo < m /\ m < ranges[i].hi
    /\ \E j \in Ids : ~ranges[j].made
    /\ LET c == CHOOSE j \in Ids : ~ranges[j].made
           r == ranges[i]
       IN ranges' = [ranges EXCEPT
            ![i] = [r EXCEPT !.hi = m, !.gen = r.gen + 1,
                             !.keys = {k \in r.keys : k < m},
                             !.kids = {c}],
            ![c] = [Unmade EXCEPT !.lo = m, !.hi = r.hi, !.gen = r.gen + 1,
                    !.keys = {k \in r.keys : m <= k}, !.gate = r.gate,
                    !.gatt = r.gatt, !.live = TRUE, !.made = TRUE]]
    /\ splits' = splits + 1
    /\ UNCHANGED <<directory, cache, bucket, owner, coord, acked, merging>>

Publish ==
    /\ directory' = {Desc(i) : i \in Live}
    /\ UNCHANGED <<ranges, cache, bucket, owner, coord, acked, splits, merging>>

(***************************************************************************)
(* Merges.                                                                 *)
(***************************************************************************)

\* Range j, just above range i as a driver read them, is frozen for a merge into i at the
\* generation the driver read.  Its own generation rises, so a step routed by what it was is
\* refused after it thaws.
Freeze(j, i) ==
    /\ mids < MaxMerges
    /\ Serving(j) /\ (~ONEROLE \/ ranges[j].pending = 0)
    /\ ranges[i].live /\ ranges[i].hi = ranges[j].lo
    /\ LET m == mids + 1
       IN /\ mids' = m
          /\ freezes' = [freezes EXCEPT ![m] =
                 [j |-> j, i |-> i, lo |-> ranges[j].lo, hi |-> ranges[j].hi,
                  gen |-> ranges[j].gen + 1, igen |-> ranges[i].gen]]
          /\ ranges' = [ranges EXCEPT ![j].frozen = m, ![j].gen = @ + 1, ![j].into = i]
    /\ UNCHANGED <<directory, cache, bucket, owner, coord, acked, splits, committed,
                   refused>>

\* Range i decides merge m in its own log when the driver's command arrives at the generation
\* it names.  It takes the frozen range, as the replicas find it then, if it holds no merge
\* not yet resolved, keeping its own gate; otherwise it refuses m.  Either way its generation
\* moves on.  At another generation the command is refused as routed by a descriptor it no
\* longer matches, and changes nothing.
Decide(m) ==
    /\ m \in 1..mids
    /\ LET f == freezes[m]
           r == ranges[f.i]
       IN /\ Serving(f.i) /\ r.gen = f.igen
          /\ IF r.pending = 0 /\ r.hi = f.lo
               THEN /\ ranges' = [ranges EXCEPT
                         ![f.i].hi = f.hi,
                         ![f.i].keys = @ \cup ranges[f.j].keys,
                         ![f.i].gen = (IF @ > f.gen THEN @ ELSE f.gen) + 1,
                         ![f.i].kids = @ \ {f.j},
                         ![f.i].pending = m]
                    /\ committed' = committed \cup {m}
                    /\ UNCHANGED refused
               ELSE /\ ranges' = [ranges EXCEPT ![f.i].gen = IF DECIDEONCE THEN @ + 1 ELSE @]
                    /\ refused' = refused \cup {m}
                    /\ UNCHANGED committed
    /\ UNCHANGED <<directory, cache, bucket, owner, coord, acked, splits, mids, freezes>>

\* A driver abandons merge m before range i decided it, moving i's generation on.
Abandon(m) ==
    /\ m \in 1..mids
    /\ LET f == freezes[m]
       IN /\ Serving(f.i) /\ ranges[f.i].gen = f.igen
          /\ ranges' = [ranges EXCEPT ![f.i].gen = IF DECIDEONCE THEN @ + 1 ELSE @]
          /\ refused' = refused \cup {m}
    /\ UNCHANGED <<directory, cache, bucket, owner, coord, acked, splits, mids, freezes,
                   committed>>

\* What a driver concludes from range i: merge m will never be taken there.  Without the
\* rule that a refusal moves the generation on, a driver takes the refusal's answer instead.
NeverTaken(m) ==
    LET f == freezes[m]
        r == ranges[f.i]
    IN \/ ~r.live
       \/ (r.gen > f.igen /\ r.pending # m)
       \/ (~DECIDEONCE /\ m \in refused)

\* The frozen range ends once its merge was taken, naming the lower range.
EndMerge(m) ==
    /\ m \in committed
    /\ LET j == freezes[m].j
       IN /\ ranges[j].frozen = m
          /\ ranges' = [ranges EXCEPT ![j].live = FALSE, ![j].frozen = 0, ![j].keys = {}]
    /\ UNCHANGED <<directory, cache, bucket, owner, coord, acked, splits, merging>>

\* The frozen range serves again once its merge will never be taken.
Thaw(m) ==
    /\ m \in 1..mids
    /\ LET j == freezes[m].j
       IN /\ ranges[j].frozen = m
          /\ NeverTaken(m)
          /\ ranges' = [ranges EXCEPT ![j].frozen = 0, ![j].gen = @ + 1, ![j].into = 0]
    /\ UNCHANGED <<directory, cache, bucket, owner, coord, acked, splits, merging>>

\* The lower range lets go of merge m once the frozen range has ended.
Resolve(m) ==
    /\ m \in committed
    /\ LET f == freezes[m]
       IN /\ ranges[f.i].pending = m
          /\ ranges[f.j].frozen # m
          /\ ranges' = [ranges EXCEPT ![f.i].pending = 0]
    /\ UNCHANGED <<directory, cache, bucket, owner, coord, acked, splits, merging>>

(***************************************************************************)
(* Attempts.  `known` is the descriptors an attempt works through, `done`  *)
(* the ids it has handled in its current phase.                            *)
(***************************************************************************)

BeginCreate(a) ==
    /\ a \in Creates
    /\ coord[a].phase = "idle"
    /\ bucket = "none"
    /\ a > owner
    /\ bucket' = "creating"
    /\ owner' = a
    /\ coord' = [coord EXCEPT ![a] = [phase |-> "open", known |-> directory, done |-> {}]]
    /\ UNCHANGED <<ranges, directory, cache, acked, splits, merging>>

Begin(a) ==
    /\ a \in Deletes
    /\ coord[a].phase = "idle"
    /\ bucket \in {"active", "deleting"}
    /\ a > owner
    /\ bucket' = "deleting"
    /\ owner' = a
    /\ coord' = [coord EXCEPT ![a] = [phase |-> "close", known |-> directory, done |-> {}]]
    /\ UNCHANGED <<ranges, directory, cache, acked, splits, merging>>

\* The attempt learns a range's answer, and starts its phase again: a create opens
\* again, a delete closes and reads again.
Relearn(a, d) ==
    coord' = [coord EXCEPT ![a] = [phase |-> IF a \in Creates THEN "open" ELSE "close",
                                   known |-> Newest((@.known \ {d}) \cup Answer(d)),
                                   done |-> {}]]

\* Moves the gate of the range d names from one of `from` to `to`.
Move(a, d, from, to) ==
    IF Current(d)
      THEN IF ranges[d.id].gatt <= a /\ ranges[d.id].gate \in from
             THEN /\ ranges' = [ranges EXCEPT ![d.id].gate = to, ![d.id].gatt = a]
                  /\ coord' = [coord EXCEPT ![a].done = @ \cup {d.id}]
             ELSE /\ coord' = [coord EXCEPT ![a].phase = "stopped"]
                  /\ UNCHANGED ranges
      ELSE /\ Relearn(a, d)
           /\ UNCHANGED ranges

Open(a) ==
    /\ coord[a].phase = "open"
    /\ \E d \in coord[a].known :
         /\ d.id \notin coord[a].done
         /\ Move(a, d, {"none", "open"}, "open")
    /\ UNCHANGED <<directory, cache, bucket, owner, acked, splits, merging>>

\* Every range it knows open, and they cover the bucket: it is active.
Activate(a) ==
    /\ coord[a].phase = "open"
    /\ \A d \in coord[a].known : d.id \in coord[a].done
    /\ Covers(coord[a].known)
    /\ IF owner = a /\ bucket = "creating"
         THEN bucket' = "active"
         ELSE UNCHANGED bucket
    /\ coord' = [coord EXCEPT ![a].phase = "finished"]
    /\ UNCHANGED <<ranges, directory, cache, owner, acked, splits, merging>>

Close(a) ==
    /\ coord[a].phase = "close"
    /\ \E d \in coord[a].known :
         /\ d.id \notin coord[a].done
         /\ Move(a, d, {"open", "closed"}, "closed")
    /\ UNCHANGED <<directory, cache, bucket, owner, acked, splits, merging>>

\* Every range it knows closed, and they cover the bucket: read them.
Closed(a) ==
    /\ coord[a].phase = "close"
    /\ \A d \in coord[a].known : d.id \in coord[a].done
    /\ Covers(coord[a].known)
    /\ coord' = [coord EXCEPT ![a].phase = "probe", ![a].done = {}]
    /\ UNCHANGED <<ranges, directory, cache, bucket, owner, acked, splits, merging>>

\* What it knows no longer covers the bucket: it starts over from the directory.
Lost(a) ==
    /\ coord[a].phase \in {"open", "close", "probe", "reopen"}
    /\ ~Covers(coord[a].known)
    /\ coord' = [coord EXCEPT ![a].known = directory, ![a].done = {}]
    /\ UNCHANGED <<ranges, directory, cache, bucket, owner, acked, splits, merging>>

Probe(a) ==
    /\ coord[a].phase = "probe"
    /\ \E d \in coord[a].known :
         /\ d.id \notin coord[a].done
         /\ IF Current(d)
              THEN IF ranges[d.id].keys \cap Span(d) # {}
                     THEN coord' = [coord EXCEPT ![a].phase = "reopen", ![a].done = {}]
                     ELSE coord' = [coord EXCEPT ![a].done = @ \cup {d.id}]
              ELSE Relearn(a, d)
    /\ UNCHANGED <<ranges, directory, cache, bucket, owner, acked, splits, merging>>

\* Nothing found anywhere: the Bucket range deletes the bucket, if no later attempt owns it.
Finish(a) ==
    /\ coord[a].phase = "probe"
    /\ \A d \in coord[a].known : d.id \in coord[a].done
    /\ Covers(coord[a].known)
    /\ IF owner = a /\ bucket = "deleting"
         THEN bucket' = "deleted"
         ELSE UNCHANGED bucket
    /\ coord' = [coord EXCEPT ![a].phase = "finished"]
    /\ UNCHANGED <<ranges, directory, cache, owner, acked, splits, merging>>

Reopen(a) ==
    /\ coord[a].phase = "reopen"
    /\ \E d \in coord[a].known :
         /\ d.id \notin coord[a].done
         /\ Move(a, d, {"closed"}, "open")
    /\ UNCHANGED <<directory, cache, bucket, owner, acked, splits, merging>>

Restore(a) ==
    /\ coord[a].phase = "reopen"
    /\ \A d \in coord[a].known : d.id \in coord[a].done
    /\ Covers(coord[a].known)
    /\ IF owner = a /\ bucket = "deleting"
         THEN bucket' = "active"
         ELSE UNCHANGED bucket
    /\ coord' = [coord EXCEPT ![a].phase = "finished"]
    /\ UNCHANGED <<ranges, directory, cache, owner, acked, splits, merging>>

Next ==
    \/ \E k \in Keys, put \in BOOLEAN : Write(k, put)
    \/ Refresh
    \/ \E i \in Ids, m \in Keys : Split(i, m)
    \/ Publish
    \/ \E i, j \in Ids : Freeze(j, i)
    \/ \E m \in Mids : Decide(m) \/ Abandon(m) \/ EndMerge(m) \/ Thaw(m) \/ Resolve(m)
    \/ \E a \in Attempts :
         \/ BeginCreate(a) \/ Open(a) \/ Activate(a)
         \/ Begin(a) \/ Close(a) \/ Closed(a) \/ Lost(a) \/ Probe(a)
         \/ Finish(a) \/ Reopen(a) \/ Restore(a)

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* What must hold.                                                         *)
(***************************************************************************)

\* The ranges that own their spans: every live range, but a frozen one whose merge was
\* taken, whose span the lower range owns now.
Owners == {i \in Live : ranges[i].frozen \notin committed}

\* The owners divide the keys between them, with none owned twice or by none.
Partition ==
    /\ \A k \in Keys : \E i \in Owners : k \in Span(ranges[i])
    /\ \A i, j \in Owners : i # j => Span(ranges[i]) \cap Span(ranges[j]) = {}

\* A range holds versions only of keys it owns, and every acknowledged version is held.
Held ==
    /\ \A i \in Owners : ranges[i].keys \subseteq Span(ranges[i])
    /\ \A k \in acked : \E i \in Owners : k \in ranges[i].keys

\* No acknowledged write is lost to a delete.
NoLostWrite == bucket = "deleted" => acked = {}

\* An active bucket takes writes to every key: every owner's gate is open.
ActiveOpen == bucket = "active" => \A i \in Owners : ranges[i].gate = "open"

=============================================================================
