------------------------------- MODULE RangeSplit -------------------------------
(***************************************************************************)
(* mantle's Name ranges splitting while a bucket is written and deleted    *)
(* (docs/design/metadata.md §2-§3; docs/design/architecture.md §5-§6).     *)
(*                                                                         *)
(* A range holds a span of the bucket's keys, a descriptor generation, the *)
(* keys it holds a version of, and the bucket's gate.  A split is one      *)
(* command in the parent's log: the parent keeps the low part, a child     *)
(* takes the high part with its keys and the parent's gate, and both take  *)
(* the next generation.  The directory learns the new descriptors later.   *)
(*                                                                         *)
(* A writer routes by the descriptors it cached.  A range takes a write    *)
(* only for a key in its span, through an open gate.  To a request routed  *)
(* by a descriptor it no longer matches, a range answers with its own      *)
(* descriptor and those of the children it made, and never with data.      *)
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
(* whose gate never opened.                                                *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Keys,      \* the bucket's keys, as numbers in their order
          MaxSplits, \* splits the model takes
          Creates,   \* create attempts, and
          Deletes,   \* delete attempts, as numbers in the order they begin
          FENCED     \* whether ranges refuse steps of an older generation

Attempts == Creates \cup Deletes

Bottom == CHOOSE k \in Keys : \A j \in Keys : k <= j
End == (CHOOSE k \in Keys : \A j \in Keys : j <= k) + 1
Ids == 1..(MaxSplits + 1)

VARIABLES ranges,    \* id -> the range, or one not yet made
          directory, \* the descriptors the directory holds
          cache,     \* the writer's descriptors
          bucket,    \* "none", "creating", "active", "deleting" or "deleted"
          owner,     \* the attempt that last moved the bucket's row
          coord,     \* each attempt: where it is, and what it knows
          acked,     \* keys whose last acknowledged write made a version
          splits

vars == <<ranges, directory, cache, bucket, owner, coord, acked, splits>>

Span(r) == {k \in Keys : r.lo <= k /\ k < r.hi}
Desc(i) == [id |-> i, lo |-> ranges[i].lo, hi |-> ranges[i].hi, gen |-> ranges[i].gen]
Live == {i \in Ids : ranges[i].live}
Covers(ds) == \A k \in Keys : \E d \in ds : k \in Span(d)

\* What a range answers a request routed by a descriptor it no longer matches.
Answer(d) == {Desc(i) : i \in {d.id} \cup ranges[d.id].kids}

\* Whether range d.id takes a step named by descriptor d.
Current(d) == ranges[d.id].live /\ (~FENCED \/ ranges[d.id].gen = d.gen)

Unmade == [lo |-> 0, hi |-> 0, gen |-> 0, keys |-> {}, gate |-> "none",
           gatt |-> 0, live |-> FALSE, kids |-> {}]
Idle == [phase |-> "idle", known |-> {}, done |-> {}]

Init ==
    /\ ranges = [i \in Ids |->
         IF i = 1 THEN [lo |-> Bottom, hi |-> End, gen |-> 1, keys |-> {},
                        gate |-> "none", gatt |-> 0, live |-> TRUE, kids |-> {}]
                  ELSE Unmade]
    /\ directory = {Desc(1)}
    /\ cache = directory
    /\ bucket = "none"
    /\ owner = 0
    /\ coord = [a \in Attempts |-> Idle]
    /\ acked = {}
    /\ splits = 0

(***************************************************************************)
(* Writes.                                                                 *)
(***************************************************************************)

\* A write to key k through the descriptor d the writer holds for it.
Write(k, put) ==
    \E d \in cache :
        /\ k \in Span(d)
        /\ IF ranges[d.id].live /\ k \in Span(ranges[d.id])
             THEN /\ ranges[d.id].gate = "open"
                  /\ ranges' = [ranges EXCEPT ![d.id].keys =
                                  IF put THEN @ \cup {k} ELSE @ \ {k}]
                  /\ acked' = IF put THEN acked \cup {k} ELSE acked \ {k}
                  /\ UNCHANGED cache
             ELSE /\ cache' = (cache \ {d}) \cup Answer(d)
                  /\ UNCHANGED <<ranges, acked>>
        /\ UNCHANGED <<directory, bucket, owner, coord, splits>>

Refresh ==
    /\ cache' = directory
    /\ UNCHANGED <<ranges, directory, bucket, owner, coord, acked, splits>>

(***************************************************************************)
(* Splits, and the directory catching up with them.                        *)
(***************************************************************************)

Split(i, m) ==
    /\ splits < MaxSplits
    /\ ranges[i].live
    /\ m \in Keys /\ ranges[i].lo < m /\ m < ranges[i].hi
    /\ LET c == CHOOSE j \in Ids : ~ranges[j].live
           r == ranges[i]
       IN ranges' = [ranges EXCEPT
            ![i] = [r EXCEPT !.hi = m, !.gen = r.gen + 1,
                             !.keys = {k \in r.keys : k < m},
                             !.kids = r.kids \cup {c}],
            ![c] = [lo |-> m, hi |-> r.hi, gen |-> r.gen + 1,
                    keys |-> {k \in r.keys : m <= k}, gate |-> r.gate,
                    gatt |-> r.gatt, live |-> TRUE, kids |-> {}]]
    /\ splits' = splits + 1
    /\ UNCHANGED <<directory, cache, bucket, owner, coord, acked>>

Publish ==
    /\ directory' = {Desc(i) : i \in Live}
    /\ UNCHANGED <<ranges, cache, bucket, owner, coord, acked, splits>>

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
    /\ UNCHANGED <<ranges, directory, cache, acked, splits>>

Begin(a) ==
    /\ a \in Deletes
    /\ coord[a].phase = "idle"
    /\ bucket \in {"active", "deleting"}
    /\ a > owner
    /\ bucket' = "deleting"
    /\ owner' = a
    /\ coord' = [coord EXCEPT ![a] = [phase |-> "close", known |-> directory, done |-> {}]]
    /\ UNCHANGED <<ranges, directory, cache, acked, splits>>

\* The attempt learns a range's answer, and starts its phase again: a create opens
\* again, a delete closes and reads again.
Relearn(a, d) ==
    coord' = [coord EXCEPT ![a] = [phase |-> IF a \in Creates THEN "open" ELSE "close",
                                   known |-> (@.known \ {d}) \cup Answer(d),
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
    /\ UNCHANGED <<directory, cache, bucket, owner, acked, splits>>

\* Every range it knows open, and they cover the bucket: it is active.
Activate(a) ==
    /\ coord[a].phase = "open"
    /\ \A d \in coord[a].known : d.id \in coord[a].done
    /\ Covers(coord[a].known)
    /\ IF owner = a /\ bucket = "creating"
         THEN bucket' = "active"
         ELSE UNCHANGED bucket
    /\ coord' = [coord EXCEPT ![a].phase = "finished"]
    /\ UNCHANGED <<ranges, directory, cache, owner, acked, splits>>

Close(a) ==
    /\ coord[a].phase = "close"
    /\ \E d \in coord[a].known :
         /\ d.id \notin coord[a].done
         /\ Move(a, d, {"open", "closed"}, "closed")
    /\ UNCHANGED <<directory, cache, bucket, owner, acked, splits>>

\* Every range it knows closed, and they cover the bucket: read them.
Closed(a) ==
    /\ coord[a].phase = "close"
    /\ \A d \in coord[a].known : d.id \in coord[a].done
    /\ Covers(coord[a].known)
    /\ coord' = [coord EXCEPT ![a].phase = "probe", ![a].done = {}]
    /\ UNCHANGED <<ranges, directory, cache, bucket, owner, acked, splits>>

\* What it knows no longer covers the bucket: it starts over from the directory.
Lost(a) ==
    /\ coord[a].phase \in {"open", "close", "probe", "reopen"}
    /\ ~Covers(coord[a].known)
    /\ coord' = [coord EXCEPT ![a].known = directory, ![a].done = {}]
    /\ UNCHANGED <<ranges, directory, cache, bucket, owner, acked, splits>>

Probe(a) ==
    /\ coord[a].phase = "probe"
    /\ \E d \in coord[a].known :
         /\ d.id \notin coord[a].done
         /\ IF Current(d)
              THEN IF ranges[d.id].keys \cap Span(d) # {}
                     THEN coord' = [coord EXCEPT ![a].phase = "reopen", ![a].done = {}]
                     ELSE coord' = [coord EXCEPT ![a].done = @ \cup {d.id}]
              ELSE Relearn(a, d)
    /\ UNCHANGED <<ranges, directory, cache, bucket, owner, acked, splits>>

\* Nothing found anywhere: the Bucket range deletes the bucket, if no later attempt owns it.
Finish(a) ==
    /\ coord[a].phase = "probe"
    /\ \A d \in coord[a].known : d.id \in coord[a].done
    /\ Covers(coord[a].known)
    /\ IF owner = a /\ bucket = "deleting"
         THEN bucket' = "deleted"
         ELSE UNCHANGED bucket
    /\ coord' = [coord EXCEPT ![a].phase = "finished"]
    /\ UNCHANGED <<ranges, directory, cache, owner, acked, splits>>

Reopen(a) ==
    /\ coord[a].phase = "reopen"
    /\ \E d \in coord[a].known :
         /\ d.id \notin coord[a].done
         /\ Move(a, d, {"closed"}, "open")
    /\ UNCHANGED <<directory, cache, bucket, owner, acked, splits>>

Restore(a) ==
    /\ coord[a].phase = "reopen"
    /\ \A d \in coord[a].known : d.id \in coord[a].done
    /\ Covers(coord[a].known)
    /\ IF owner = a /\ bucket = "deleting"
         THEN bucket' = "active"
         ELSE UNCHANGED bucket
    /\ coord' = [coord EXCEPT ![a].phase = "finished"]
    /\ UNCHANGED <<ranges, directory, cache, owner, acked, splits>>

Next ==
    \/ \E k \in Keys, put \in BOOLEAN : Write(k, put)
    \/ Refresh
    \/ \E i \in Ids, m \in Keys : Split(i, m)
    \/ Publish
    \/ \E a \in Attempts :
         \/ BeginCreate(a) \/ Open(a) \/ Activate(a)
         \/ Begin(a) \/ Close(a) \/ Closed(a) \/ Lost(a) \/ Probe(a)
         \/ Finish(a) \/ Reopen(a) \/ Restore(a)

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* What must hold.                                                         *)
(***************************************************************************)

\* The live ranges divide the keys between them, with none held twice or by none.
Partition ==
    /\ \A k \in Keys : \E i \in Live : k \in Span(ranges[i])
    /\ \A i, j \in Live : i # j => Span(ranges[i]) \cap Span(ranges[j]) = {}

\* A range holds versions only of keys it owns, and every acknowledged version is held.
Held ==
    /\ \A i \in Live : ranges[i].keys \subseteq Span(ranges[i])
    /\ \A k \in acked : \E i \in Live : k \in ranges[i].keys

\* No acknowledged write is lost to a delete.
NoLostWrite == bucket = "deleted" => acked = {}

\* An active bucket takes writes to every key: every range's gate is open.
ActiveOpen == bucket = "active" => \A i \in Live : ranges[i].gate = "open"

=============================================================================
