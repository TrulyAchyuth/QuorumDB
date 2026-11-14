# Test results

Generated 2026-11-13 21:17 UTC by `scripts/test_summary.py` from the JUnit XML reports of the last `./gradlew clean test integrationTest` run.

**Total: 171 tests, 171 passed, 0 failed, 0 skipped.**

| Module | Suite | Tests | Passed | Failed | Skipped | Time (s) |
|---|---|---:|---:|---:|---:|---:|
| backend (integrationTest) | RealClusterIT | 9 | 9 | 0 | 0 | 56.36 |
| backend | ClusterConfigTest | 3 | 3 | 0 | 0 | 0.03 |
| backend | EventLogTest | 4 | 4 | 0 | 0 | 0.00 |
| backend | ProtoCodecTest | 2 | 2 | 0 | 0 | 0.02 |
| backend | SimulationLimitsTest | 4 | 4 | 0 | 0 | 0.88 |
| core | KvStateMachineTest | 9 | 9 | 0 | 0 | 0.01 |
| core | RaftElectionTest | 18 | 18 | 0 | 0 | 0.01 |
| core | RaftReplicationTest | 28 | 28 | 0 | 0 | 0.08 |
| core | FileStorageRecoveryTest | 12 | 12 | 0 | 0 | 0.96 |
| core | StorageContractTest | 10 | 10 | 0 | 0 | 0.14 |
| simulation | EventBudgetTest | 4 | 4 | 0 | 0 | 0.96 |
| simulation | LinearizabilityCheckerTest | 11 | 11 | 0 | 0 | 0.01 |
| simulation | MinorityPartitionTest | 5 | 5 | 0 | 0 | 0.01 |
| simulation | RandomFaultProperties | 3 | 3 | 0 | 0 | 4.72 |
| simulation | SafetyCheckerTest | 19 | 19 | 0 | 0 | 0.02 |
| simulation | ScenarioTest | 27 | 27 | 0 | 0 | 2.22 |
| simulation | SeededBugTest | 3 | 3 | 0 | 0 | 0.05 |

## backend (integrationTest)

### io.quorumdb.server.RealClusterIT

- passed: `isolatedLeaderCannotCommitAndMajoritySideKeepsServing()` (2.338s)
- passed: `fullClusterSigkillAndRestartRecoversAllDataFromDisk()` (4.542s)
- passed: `leaderSigkillFailsOverWithoutLosingAcknowledgedWritesAndTheNodeCatchesUp()` (5.536s)
- passed: `liveAuditOfALongLogAcrossACrashReportsNoFalseViolation()` (12.903s)
- passed: `consecutiveLiveRunsOnTheSameClusterAreEachLinearizable()` (9.008s)
- passed: `oversizedSimulationIsRefusedAndTheNodeStaysHealthy()` (1.867s)
- passed: `freshClusterElectsOneLeaderAndServesRequestsThroughAnyNode()` (1.779s)
- passed: `liveFaultLabExperimentIsAuditedAndLeavesNoFaultsBehind()` (10.176s)
- passed: `fiveNodeClusterSplitTwoThreeOnlyMajorityCommits()` (8.206s)

Recorded output (jqwik tries/seeds, or EVIDENCE lines with real measurements):

```
EVIDENCE write to isolated leader n2 answered HTTP 503 OUTCOME_UNKNOWN
EVIDENCE isolated n2: commit index stayed 2, role FOLLOWER; majority elected n1 and committed a write
EVIDENCE isolatedLeaderCannotCommitAndMajoritySideKeepsServing() finished; all node processes terminated
EVIDENCE all 3 processes SIGKILLed and restarted: 100/100 acknowledged keys recovered, term 2 -> 3
EVIDENCE fullClusterSigkillAndRestartRecoversAllDataFromDisk() finished; all node processes terminated
EVIDENCE SIGKILL of leader n3: new leader n1 observed after 277 ms (HTTP polling every 25 ms, so an upper bound); writes during failover: 131 acknowledged, 91 rejected
EVIDENCE restarted n3 recovered from disk and caught up in 1410 ms (incl. JVM startup); verified 181 acknowledged keys on it
EVIDENCE leaderSigkillFailsOverWithoutLosingAcknowledgedWritesAndTheNodeCatchesUp() finished; all node processes terminated
EVIDENCE preloaded 6500 writes, leader commit index 6501
EVIDENCE long-log live run live-ca302394: safety=SAFE
EVIDENCE liveAuditOfALongLogAcrossACrashReportsNoFalseViolation() finished; all node processes terminated
EVIDENCE back-to-back live run 1 (live-ce4cb771): linearizability=LINEARIZABLE ops=260
EVIDENCE back-to-back live run 2 (live-3c8b2e46): linearizability=LINEARIZABLE ops=281
EVIDENCE consecutiveLiveRunsOnTheSameClusterAreEachLinearizable() finished; all node processes terminated
EVIDENCE oversized simulation answered HTTP 422 in 130 ms: {"error":"RUN_TOO_LARGE","message":"this scenario produced more than 134217 events by 344 ms of simulated time, the limit for one run on this node. Shorten durationMs, use fewer clients or nodes, raise opIntervalMs or heartbeatIntervalMs, or set recordMessages to false (message events are usually most of the trace).","node":"n1","leaderHint":null,"timestamp":1794604631518}
EVIDENCE oversizedSimulationIsRefusedAndTheNodeStaysHealthy() finished; all node processes terminated
EVIDENCE 3 processes started and elected n1 in 1666 ms (incl. JVM startup)
EVIDENCE freshClusterElectsOneLeaderAndServesRequestsThroughAnyNode() finished; all node processes terminated
EVIDENCE live FaultLab run live-ec233352: headline=EXPECTED_UNAVAILABILITY safety=SAFE linearizability=LINEARIZABLE ops=568 elections=1 events=3469
EVIDENCE liveFaultLabExperimentIsAuditedAndLeavesNoFaultsBehind() finished; all node processes terminated
EVIDENCE 5-node split [n5, n1] | [n2, n3, n4]: minority commit stayed 1, majority elected n3 and committed
EVIDENCE fiveNodeClusterSplitTwoThreeOnlyMajorityCommits() finished; all node processes terminated
```

## backend

### io.quorumdb.server.ClusterConfigTest

- passed: `singleNodeClusterHasNoPeers()` (0.022s)
- passed: `parsesPeers()` (0.005s)
- passed: `rejectsInvalidConfiguration()` (0.001s)

### io.quorumdb.server.EventLogTest

- passed: `reportsWhenRequestedEventsWereOverwritten()` (0.001s)
- passed: `assignsGapFreeSequenceNumbersAndPagesForward()` (0.001s)
- passed: `latestOfGivenTypesIsNotCrowdedOutByFrequentEvents()` (0.000s)
- passed: `rareEventsOutliveTheMainRingUnderWriteLoad()` (0.000s)

### io.quorumdb.server.ProtoCodecTest

- passed: `malformedEnvelopesAreRejected()` (0.009s)
- passed: `everyMessageTypeRoundTrips()` (0.009s)

### io.quorumdb.server.SimulationLimitsTest

- passed: `runningSimulationsFitTheirShareOfTheHeapEvenAtTheLimit()` (0.001s)
- passed: `storeEvictsOldestRunsByTotalEventsAndKeepsTheNewest()` (0.083s)
- passed: `everyBuiltInScenarioFitsOneRunFromA256MbHeapUp()` (0.790s)
- passed: `hugeHeapIsStillCappedPerRun()` (0.000s)

## core

### io.quorumdb.kv.KvStateMachineTest

- passed: `digestDependsOnContentNotHistory()` (0.001s)
- passed: `malformedCommittedEntryIsRejectedDeterministicallyWithoutChangingState()` (0.000s)
- passed: `codecsRoundTripAndRejectGarbage()` (0.003s)
- passed: `retriedCommandIsExecutedOnce()` (0.000s)
- passed: `batchIsAllOrNothing()` (0.000s)
- passed: `putGetDeleteAndExistence()` (0.000s)
- passed: `compareAndSetUsesVersions()` (0.001s)
- passed: `snapshotRoundTripPreservesDataVersionsAndSessions()` (0.001s)
- passed: `scanReturnsOrderedPrefixMatches()` (0.001s)

### io.quorumdb.raft.RaftElectionTest

- passed: `votesFromAnEarlierElectionRoundAreIgnored()` (0.000s)
- passed: `refusesCandidateWhoseLogIsBehind()` (0.001s)
- passed: `messagesFromNonMembersAreIgnored()` (0.000s)
- passed: `leaderKeepsLeadingWhileAMajorityAnswers()` (0.001s)
- passed: `staleTermMessagesAreRejectedWithCurrentTerm()` (0.001s)
- passed: `leaderStepsDownWhenItSeesAHigherTerm()` (0.000s)
- passed: `candidateNeedsAMajorityAndCountsEachVoterOnce()` (0.001s)
- passed: `grantingAPreVoteChangesNothing()` (0.000s)
- passed: `singleNodeClusterElectsItself()` (0.001s)
- passed: `preVoteDoesNotChangeTermUntilAMajorityWouldVote()` (0.000s)
- passed: `followerStartsElectionAfterTimeout()` (0.000s)
- passed: `voteSurvivesRestart()` (0.000s)
- passed: `leaderWithoutQuorumContactStepsDown()` (0.001s)
- passed: `candidateBecomesLeaderWithMajorityAndAppendsNoOp()` (0.000s)
- passed: `grantsAtMostOneVotePerTerm()` (0.001s)
- passed: `followerWithFreshLeaderIgnoresVoteRequests()` (0.000s)
- passed: `heartbeatsPreventElections()` (0.000s)
- passed: `candidateYieldsToLeaderOfSameTerm()` (0.000s)

### io.quorumdb.raft.RaftReplicationTest

- passed: `conflictingSuffixIsReplacedByLeadersEntries()` (0.039s)
- passed: `followerRejectsWhenPreviousEntryIsMissing()` (0.002s)
- passed: `followerRejectsWhenPreviousTermDiffersAndReportsWholeConflictingTerm()` (0.001s)
- passed: `heartbeatRetransmitsUnacknowledgedEntries()` (0.004s)
- passed: `readIsNotServedOnAcknowledgementsThatPredateIt()` (0.001s)
- passed: `pendingProposalBecomesUnknownWhenLeadershipIsLost()` (0.001s)
- passed: `followerDoesNotAcknowledgeEntriesItCouldNotPersist()` (0.000s)
- passed: `storageFailureMakesTheNodeFailStop()` (0.001s)
- passed: `staleFailureReplyNeverMovesNextIndexBelowMatchIndex()` (0.001s)
- passed: `backPressureRejectsProposalsBeyondTheLimit()` (0.007s)
- passed: `snapshotIsNotResentWhileInFlightButIsRetransmittedAfterTimeout()` (0.002s)
- passed: `restartRestoresStateMachineFromSnapshotAndReplaysNothingUncommitted()` (0.000s)
- passed: `proposalOnFollowerIsRejectedWithLeaderHint()` (0.001s)
- passed: `leaderCommitsOnlyAfterAMajorityStoresTheEntry()` (0.000s)
- passed: `redeliveredOrReorderedAppendDoesNotDeleteNewerEntries()` (0.001s)
- passed: `deposedLeaderFailsPendingReads()` (0.001s)
- passed: `commitIndexNeverGoesBackwards()` (0.000s)
- passed: `stoppedNodeReportsUnknownForPendingWork()` (0.001s)
- passed: `snapshotIsTakenAfterThresholdAndLogIsCompacted()` (0.001s)
- passed: `fiveNodeLeaderNeedsThreeCopies()` (0.000s)
- passed: `leaderDoesNotCommitEntriesFromEarlierTermsByCountingReplicas()` (0.001s)
- passed: `readIndexWaitsForNoOpCommitAndAMajorityHeartbeat()` (0.000s)
- passed: `followerInstallsSnapshotAndContinuesFromIt()` (0.000s)
- passed: `leaderBacksUpUsingConflictHint()` (0.001s)
- passed: `concurrentReadsShareConfirmationRounds()` (0.001s)
- passed: `leaderSendsSnapshotToFollowerThatNeedsCompactedEntries()` (0.001s)
- passed: `followerCommitsUpToTheLastEntryVerifiedByTheMessage()` (0.001s)
- passed: `followerAppendsEntriesAndAcknowledgesMatchIndex()` (0.000s)

### io.quorumdb.storage.FileStorageRecoveryTest

- passed: `missingLogAfterSnapshotIsDetected()` (0.284s)
- passed: `interruptedAtomicWritesLeaveOldStateVisible()` (0.023s)
- passed: `tornFinalRecordIsDiscarded()` (0.012s)
- passed: `corruptionInTheMiddleOfTheLogIsFatal()` (0.011s)
- passed: `installSnapshotThatDiscardsLogSurvivesReopen()` (0.022s)
- passed: `truncatedTailOfLastRecordIsTreatedAsTornWrite()` (0.011s)
- passed: `corruptHardStateIsFatal()` (0.009s)
- passed: `everythingSurvivesReopen()` (0.037s)
- passed: `dataDirectoryCannotBeOpenedTwice()` (0.004s)
- passed: `corruptionInAnOlderSegmentIsFatal()` (0.268s)
- passed: `logRollsIntoSegmentsAndCompactionDeletesOldOnes()` (0.269s)
- passed: `faultyStorageRejectsWritesButServesReads()` (0.010s)

### io.quorumdb.storage.StorageContractTest

- passed: `[1] kind = "memory"` (0.005s)
- passed: `[2] kind = "file"` (0.002s)
- passed: `[1] kind = "memory"` (0.001s)
- passed: `[2] kind = "file"` (0.018s)
- passed: `[1] kind = "memory"` (0.001s)
- passed: `[2] kind = "file"` (0.019s)
- passed: `[1] kind = "memory"` (0.001s)
- passed: `[2] kind = "file"` (0.042s)
- passed: `[1] kind = "memory"` (0.001s)
- passed: `[2] kind = "file"` (0.034s)

## simulation

### io.quorumdb.faultlab.EventBudgetTest

- passed: `budgetIsNotMistakenForANodeFailure()` (0.065s)
- passed: `budgetMustBePositive()` (0.001s)
- passed: `runWithinBudgetIsIdenticalToAnUnlimitedRun()` (0.698s)
- passed: `runOverBudgetIsAbandonedEarlyWithATypedError()` (0.186s)

### io.quorumdb.faultlab.LinearizabilityCheckerTest

- passed: `handlesLongHistoriesWithManyClients()` (0.005s)
- passed: `readConcurrentWithWriteMaySeeOldOrNewValue()` (0.000s)
- passed: `indeterminateWriteMayTakeEffectLaterOrNever()` (0.000s)
- passed: `definitelyFailedOperationsAreIgnored()` (0.000s)
- passed: `valueCannotFlipBackWithoutAWrite()` (0.002s)
- passed: `staleReadIsNotLinearizable()` (0.001s)
- passed: `concurrentOperationsMayTakeEffectInEitherOrder()` (0.000s)
- passed: `keysAreCheckedIndependently()` (0.002s)
- passed: `readOfNeverWrittenValueIsRejected()` (0.001s)
- passed: `indeterminateWriteCannotTakeEffectBeforeItWasInvoked()` (0.000s)
- passed: `sequentialHistoryIsLinearizable()` (0.001s)

### io.quorumdb.faultlab.MinorityPartitionTest

- passed: `isolatedLeaderCannotCommitAndItsWritesAreNeverAcknowledged()` (0.006s)
- passed: `restartedFollowerCatchesUpFromLeaderLog()` (0.001s)
- passed: `exactlyOneLeaderIsElectedAndAllNodesAgreeOnIt()` (0.004s)
- passed: `committedDataSurvivesFullClusterRestart()` (0.002s)
- passed: `twoOfFiveCannotCommitThreeOfFiveCan()` (0.001s)

### io.quorumdb.faultlab.RandomFaultProperties

- passed: `noFaultScheduleBreaksSafetyOrLinearizability` (2.693s)
- passed: `clusterRecoversOnceAllFaultsHaveHealed` (1.235s)
- passed: `everyRunIsReproducibleFromItsScenarioAndSeed` (0.786s)

Recorded output (jqwik tries/seeds, or EVIDENCE lines with real measurements):

```
tries = 300                   | # of calls to property
checks = 300                  | # of not rejected calls
seed = -2176207779183852177   | random seed to reproduce generated values
tries = 150                   | # of calls to property
checks = 150                  | # of not rejected calls
seed = 9114285572842398251    | random seed to reproduce generated values
tries = 40                    | # of calls to property
checks = 40                   | # of not rejected calls
seed = 4789575591686876080    | random seed to reproduce generated values
```

### io.quorumdb.faultlab.SafetyCheckerTest

- passed: `matchingEntryWithDifferentPrefixViolatesLogMatching()` (0.001s)
- passed: `commitIndexJumpOrRegressionViolatesProgression()` (0.001s)
- passed: `termGoingBackwardsIsDetected()` (0.000s)
- passed: `restartLegitimatelyResetsCommitAndApplyPosition()` (0.000s)
- passed: `messageTermsDoNotCountAsNodeTerms()` (0.000s)
- passed: `compactedEntriesAreNotReportedMissing()` (0.001s)
- passed: `committedEntryReplacedInAFollowerLogViolatesDurability()` (0.000s)
- passed: `cleanHistoryPasses()` (0.000s)
- passed: `assumptionBreachIsReportedAlongsideViolations()` (0.000s)
- passed: `twoLeadersInOneTermViolatesElectionSafety()` (0.002s)
- passed: `differentEntriesCommittedAtSameIndexViolatesCommitAgreement()` (0.001s)
- passed: `emptyHistoryIsNotExercisedRatherThanPassed()` (0.001s)
- passed: `missingEventsInALiveAuditMakeTheVerdictInconclusive()` (0.002s)
- passed: `divergentStateAfterSamePrefixIsDetected()` (0.001s)
- passed: `sameIndexAndTermWithDifferentContentViolatesLogMatching()` (0.000s)
- passed: `divergentUncommittedSuffixesAreNotAViolation()` (0.001s)
- passed: `laterLeaderMissingCommittedEntryViolatesLeaderCompleteness()` (0.001s)
- passed: `applyingOutOfOrderOrBeforeCommitViolatesStateMachineSafety()` (0.002s)
- passed: `applyingDifferentEntriesAtAnIndexViolatesStateMachineSafety()` (0.000s)

### io.quorumdb.faultlab.ScenarioTest

- passed: `readIndexReadsAreLinearizableUnderPartitionForEverySeed()` (0.484s)
- passed: `unconfirmedLeaderReadsAreFlaggedExactlyWhenAStaleReadOccurred()` (0.492s)
- passed: `isolatedLeaderCommitsNothingAndLaterDiscardsItsEntries()` (0.088s)
- passed: `conditionTriggersFireWhenTheConditionHolds()` (0.009s)
- passed: `explanationCitesOnlyEventsThatExistAndSupportTheClaim()` (0.014s)
- passed: `[1] key = "leader-crash"` (0.011s)
- passed: `[2] key = "follower-crash"` (0.010s)
- passed: `[3] key = "leader-isolated"` (0.012s)
- passed: `[4] key = "split-5"` (0.025s)
- passed: `[5] key = "no-quorum"` (0.009s)
- passed: `[6] key = "flaky-network"` (0.019s)
- passed: `[7] key = "snapshot-catch-up"` (0.017s)
- passed: `[8] key = "storage-failure"` (0.017s)
- passed: `[9] key = "rolling-restart"` (0.012s)
- passed: `[10] key = "chaos"` (0.096s)
- passed: `[11] key = "rejoin-no-prevote"` (0.013s)
- passed: `[12] key = "rejoin-prevote"` (0.013s)
- passed: `invalidScenariosAreRejectedWithAUsefulMessage()` (0.000s)
- passed: `snapshotScenarioRestoresLaggingFollowerViaInstallSnapshot()` (0.027s)
- passed: `preVotePreventsTermInflationFromAnIsolatedNode()` (0.036s)
- passed: `leaderCrashElectsNewLeaderAndOldLeaderRejoins()` (0.015s)
- passed: `storageFailureMakesLeaderFailStopWithoutLosingAcknowledgedWrites()` (0.081s)
- passed: `sameScenarioAndSeedReproduceTheExactEventTrace()` (0.336s)
- passed: `diskLossIsDetectedAsSafetyViolationWithEvidenceAndFlaggedAsBrokenAssumption()` (0.053s)
- passed: `followerLocalReadsAreCaughtAsStale()` (0.314s)
- passed: `singleNodeClusterWorks()` (0.001s)
- passed: `losingTheMajorityIsReportedAsExpectedUnavailabilityNotAsAViolation()` (0.004s)

### io.quorumdb.faultlab.SeededBugTest

- passed: `brokenTruncationIsCaughtAsLogOrCommitViolation()` (0.021s)
- passed: `unmodifiedRaftPassesTheSameHarness()` (0.019s)
- passed: `forgettingVotesIsCaughtAsTwoLeadersInOneTerm()` (0.011s)
