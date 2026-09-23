--------------------------- MODULE AuthorityKernel ---------------------------
EXTENDS Naturals, TLC

CONSTANTS Candidates, MaxEpoch

ASSUME Candidates /= {}
ASSUME "none" \notin Candidates
ASSUME MaxEpoch \in Nat \ {0}

Phases == {"gas", "liquid", "critical", "crystal", "glass", "jammed"}
OpStates == {"idle", "pending", "in_doubt", "committed"}
CandidateOrNone == Candidates \cup {"none"}

VARIABLES
  epoch,
  boundEpoch,
  frozen,
  evidence,
  approved,
  published,
  opState,
  opEpoch,
  lastExternalEpoch,
  phase,
  authoritySource

vars ==
  << epoch, boundEpoch, frozen, evidence, approved, published,
     opState, opEpoch, lastExternalEpoch, phase, authoritySource >>

Init ==
  /\ epoch = 1
  /\ boundEpoch = 0
  /\ frozen = "none"
  /\ evidence = "none"
  /\ approved = "none"
  /\ published = "none"
  /\ opState = "idle"
  /\ opEpoch = 0
  /\ lastExternalEpoch = 0
  /\ phase = "gas"
  /\ authoritySource = "none"

Activate ==
  /\ epoch < MaxEpoch
  /\ epoch' = epoch + 1
  /\ boundEpoch' = 0
  /\ frozen' = "none"
  /\ evidence' = "none"
  /\ approved' = "none"
  /\ published' = "none"
  /\ opState' = "idle"
  /\ opEpoch' = 0
  /\ lastExternalEpoch' = 0
  /\ authoritySource' = "none"
  /\ UNCHANGED phase

BindAirflow ==
  /\ boundEpoch # epoch
  /\ boundEpoch' = epoch
  /\ UNCHANGED
       << epoch, frozen, evidence, approved, published,
          opState, opEpoch, lastExternalEpoch, phase, authoritySource >>

SearchStep ==
  /\ phase' \in Phases
  /\ UNCHANGED
       << epoch, boundEpoch, frozen, evidence, approved, published,
          opState, opEpoch, lastExternalEpoch, authoritySource >>

Freeze(c) ==
  /\ c \in Candidates
  /\ boundEpoch = epoch
  /\ frozen' = c
  /\ evidence' = "none"
  /\ approved' = "none"
  /\ published' = "none"
  /\ phase' = "crystal"
  /\ UNCHANGED
       << epoch, boundEpoch, opState, opEpoch, lastExternalEpoch, authoritySource >>

RecordEvidence(c) ==
  /\ c \in Candidates
  /\ frozen = c
  /\ evidence' = c
  /\ UNCHANGED
       << epoch, boundEpoch, frozen, approved, published,
          opState, opEpoch, lastExternalEpoch, phase, authoritySource >>

Approve(c) ==
  /\ c \in Candidates
  /\ frozen = c
  /\ evidence = c
  /\ boundEpoch = epoch
  /\ approved' = c
  /\ UNCHANGED
       << epoch, boundEpoch, frozen, evidence, published,
          opState, opEpoch, lastExternalEpoch, phase, authoritySource >>

RequestEffect(e) ==
  /\ e \in 1..MaxEpoch
  /\ IF e = epoch /\ boundEpoch = epoch /\ opState = "idle"
        THEN /\ opState' = "pending"
             /\ opEpoch' = e
        ELSE /\ opState' = opState
             /\ opEpoch' = opEpoch
  /\ UNCHANGED
       << epoch, boundEpoch, frozen, evidence, approved, published,
          lastExternalEpoch, phase, authoritySource >>

CommitEffect ==
  /\ opState = "pending"
  /\ opEpoch = epoch
  /\ opState' = "committed"
  /\ lastExternalEpoch' = epoch
  /\ UNCHANGED
       << epoch, boundEpoch, frozen, evidence, approved, published,
          opEpoch, phase, authoritySource >>

LoseReceipt ==
  /\ opState = "pending"
  /\ opState' = "in_doubt"
  /\ UNCHANGED
       << epoch, boundEpoch, frozen, evidence, approved, published,
          opEpoch, lastExternalEpoch, phase, authoritySource >>

ObserveCommitted ==
  /\ opState = "in_doubt"
  /\ opEpoch = epoch
  /\ opState' = "committed"
  /\ lastExternalEpoch' = epoch
  /\ UNCHANGED
       << epoch, boundEpoch, frozen, evidence, approved, published,
          opEpoch, phase, authoritySource >>

ObserveAbsent ==
  /\ opState = "in_doubt"
  /\ opState' = "idle"
  /\ opEpoch' = 0
  /\ UNCHANGED
       << epoch, boundEpoch, frozen, evidence, approved, published,
          lastExternalEpoch, phase, authoritySource >>

TrustedGrant ==
  /\ approved # "none"
  /\ approved = evidence
  /\ approved = frozen
  /\ authoritySource' = "trusted"
  /\ UNCHANGED
       << epoch, boundEpoch, frozen, evidence, approved, published,
          opState, opEpoch, lastExternalEpoch, phase >>

Publish(c) ==
  /\ c \in Candidates
  /\ authoritySource = "trusted"
  /\ boundEpoch = epoch
  /\ frozen = c
  /\ evidence = c
  /\ approved = c
  /\ opState = "committed"
  /\ opEpoch = epoch
  /\ published' = c
  /\ UNCHANGED
       << epoch, boundEpoch, frozen, evidence, approved,
          opState, opEpoch, lastExternalEpoch, phase, authoritySource >>

Next ==
  \/ Activate
  \/ BindAirflow
  \/ SearchStep
  \/ (\E c \in Candidates : Freeze(c))
  \/ (\E c \in Candidates : RecordEvidence(c))
  \/ (\E c \in Candidates : Approve(c))
  \/ (\E e \in 1..MaxEpoch : RequestEffect(e))
  \/ CommitEffect
  \/ LoseReceipt
  \/ ObserveCommitted
  \/ ObserveAbsent
  \/ TrustedGrant
  \/ (\E c \in Candidates : Publish(c))

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ epoch \in 1..MaxEpoch
  /\ boundEpoch \in 0..MaxEpoch
  /\ frozen \in CandidateOrNone
  /\ evidence \in CandidateOrNone
  /\ approved \in CandidateOrNone
  /\ published \in CandidateOrNone
  /\ opState \in OpStates
  /\ opEpoch \in 0..MaxEpoch
  /\ lastExternalEpoch \in 0..MaxEpoch
  /\ phase \in Phases
  /\ authoritySource \in {"none", "trusted", "search"}

EvidenceBindsFrozen ==
  evidence = "none" \/ evidence = frozen

ApprovalRequiresEvidence ==
  approved = "none" \/ (approved = evidence /\ approved = frozen)

PublicationRequiresBoundEvidence ==
  published = "none" \/
    (published = approved /\
     published = evidence /\
     published = frozen /\
     boundEpoch = epoch /\
     authoritySource = "trusted")

NoStaleExternalCommit ==
  opState # "committed" \/ lastExternalEpoch = epoch

NoSearchAuthorityEscalation ==
  authoritySource # "search"

InDoubtRequiresObservation ==
  opState # "in_doubt" \/ opEpoch = epoch

=============================================================================
