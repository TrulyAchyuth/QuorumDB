package io.quorumdb.server.faults;

import java.util.List;

/**
 * One node's part of a live fault, sent by the coordinating node to each affected node.
 *
 * @param expiresAtMs wall-clock time at which the node reverts the directive on its own (0 = never).
 *                    Each node enforces its own expiry, so a fault heals even if the coordinator dies.
 */
public record FaultDirective(String faultId, Action action, List<String> peers, long delayMs, double dropProbability,
                             long expiresAtMs, String description) {

    public enum Action { BLOCK, DELAY, DROP, CRASH, RESTART, STORAGE_FAILURE, HEAL }
}
