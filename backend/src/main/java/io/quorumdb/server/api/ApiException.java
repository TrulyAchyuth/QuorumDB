package io.quorumdb.server.api;

import org.springframework.http.HttpStatus;

/** An error with a stable machine-readable code, rendered as {@link ApiError}. */
public class ApiException extends RuntimeException {
    private final HttpStatus status;
    private final String code;
    private final String leaderHint;

    public ApiException(HttpStatus status, String code, String message) {
        this(status, code, message, null);
    }

    public ApiException(HttpStatus status, String code, String message, String leaderHint) {
        super(message);
        this.status = status;
        this.code = code;
        this.leaderHint = leaderHint;
    }

    public HttpStatus status() { return status; }
    public String code() { return code; }
    public String leaderHint() { return leaderHint; }

    public static ApiException badRequest(String message) {
        return new ApiException(HttpStatus.BAD_REQUEST, "INVALID_REQUEST", message);
    }

    public static ApiException notFound(String code, String message) {
        return new ApiException(HttpStatus.NOT_FOUND, code, message);
    }
}
