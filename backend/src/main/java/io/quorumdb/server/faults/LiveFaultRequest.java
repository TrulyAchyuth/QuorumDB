package io.quorumdb.server.faults;

import java.util.List;

/**
 * A fault to inject into the running cluster.
 *
 * @param type       CRASH, RESTART, ISOLATE, PARTITION, DELAY, DROP, STORAGE_FAILURE or HEAL
 * @param targets    node ids or the selectors {@code leader}, {@code follower}, {@code random}
 * @param groups     for PARTITION: the groups; omitted = targets vs the rest
 * @param durationMs how long until it is reverted automatically; null = until healed
 */
public record LiveFaultRequest(String type, List<String> targets, List<List<String>> groups, Long durationMs,
                               Long delayMs, Double dropProbability) {}
