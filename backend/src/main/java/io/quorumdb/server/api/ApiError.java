package io.quorumdb.server.api;

/**
 * Uniform error body.
 *
 * @param error      stable code, e.g. NOT_FOUND, NO_LEADER, OUTCOME_UNKNOWN, VERSION_MISMATCH
 * @param leaderHint best known leader id when the error concerns leadership, otherwise null
 */
public record ApiError(String error, String message, String node, String leaderHint, long timestamp) {}
