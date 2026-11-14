package io.quorumdb.faultlab.check;

import java.util.List;

/**
 * Result of auditing one execution against Raft's safety properties.
 *
 * @param verdict           overall outcome
 * @param checks            one result per property, each with the amount of evidence examined
 * @param assumptionBreaches faults injected during the run that violate an assumption Raft's proof relies on
 *                          (e.g. a node losing its disk). Violations after such a breach are real, but they
 *                          indict the broken assumption rather than the algorithm.
 * @param observationGaps   reasons the history is known to be incomplete (live clusters only)
 */
public record SafetyReport(Verdict verdict, List<Check> checks, List<String> assumptionBreaches,
                           List<String> observationGaps) {

    public enum Verdict {
        /** Every property was checked against evidence and none was violated. */
        SAFE,
        /** At least one safety property was violated; see the check's violations for evidence. */
        VIOLATION,
        /** No violation was found, but observations were incomplete, so absence of evidence is not proof. */
        INCONCLUSIVE
    }

    public enum Status {
        PASS,
        VIOLATED,
        /** The property could not be judged because observations were missing. */
        INCONCLUSIVE,
        /** Nothing happened in this run that could have violated the property (e.g. no leader was elected). */
        NOT_EXERCISED
    }

    /**
     * @param id          stable identifier, e.g. ELECTION_SAFETY
     * @param property    the property in plain words
     * @param examined    number of observations that were checked for this property
     * @param examinedWhat what those observations were
     */
    public record Check(String id, String name, String property, Status status, long examined,
                        String examinedWhat, List<Violation> violations) {}

    /**
     * @param evidence sequence numbers of the recorded events that demonstrate the violation
     */
    public record Violation(String message, long timeMs, List<String> nodes, long index, long term,
                            List<Long> evidence) {}

    public boolean hasViolations() {
        return verdict == Verdict.VIOLATION;
    }
}
