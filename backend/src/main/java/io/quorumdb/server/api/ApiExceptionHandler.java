package io.quorumdb.server.api;

import io.quorumdb.server.config.ClusterConfig;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.resource.NoResourceFoundException;

/** Maps every failure to an {@link ApiError} with an appropriate status. Never leaks stack traces. */
@RestControllerAdvice
public class ApiExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);
    private final String node;

    public ApiExceptionHandler(ClusterConfig cluster) {
        this.node = cluster.self().id();
    }

    private ResponseEntity<ApiError> body(HttpStatus status, String code, String message, String leaderHint) {
        return ResponseEntity.status(status).body(new ApiError(code, message, node, leaderHint, System.currentTimeMillis()));
    }

    @ExceptionHandler(ApiException.class)
    ResponseEntity<ApiError> api(ApiException e) {
        ResponseEntity<ApiError> r = body(e.status(), e.code(), e.getMessage(), e.leaderHint());
        if (e.status() == HttpStatus.SERVICE_UNAVAILABLE || e.status() == HttpStatus.TOO_MANY_REQUESTS) {
            return ResponseEntity.status(e.status()).header("Retry-After", "1").body(r.getBody());
        }
        return r;
    }

    @ExceptionHandler({HttpMessageNotReadableException.class, MethodArgumentTypeMismatchException.class,
            MissingServletRequestParameterException.class, MethodArgumentNotValidException.class,
            IllegalArgumentException.class})
    ResponseEntity<ApiError> badRequest(Exception e) {
        String message = switch (e) {
            case HttpMessageNotReadableException x -> "request body is missing or is not valid JSON";
            case MethodArgumentNotValidException x -> x.getBindingResult().getAllErrors().stream()
                    .map(err -> err.getDefaultMessage()).findFirst().orElse("invalid request");
            default -> e.getMessage();
        };
        return body(HttpStatus.BAD_REQUEST, "INVALID_REQUEST", message, null);
    }

    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    ResponseEntity<ApiError> method(HttpRequestMethodNotSupportedException e) {
        return body(HttpStatus.METHOD_NOT_ALLOWED, "METHOD_NOT_ALLOWED", e.getMessage(), null);
    }

    @ExceptionHandler(HttpMediaTypeNotSupportedException.class)
    ResponseEntity<ApiError> mediaType(HttpMediaTypeNotSupportedException e) {
        return body(HttpStatus.UNSUPPORTED_MEDIA_TYPE, "UNSUPPORTED_MEDIA_TYPE", "use Content-Type: application/json", null);
    }

    @ExceptionHandler(NoResourceFoundException.class)
    ResponseEntity<ApiError> noRoute(NoResourceFoundException e) {
        return body(HttpStatus.NOT_FOUND, "NO_SUCH_ENDPOINT", "no endpoint " + e.getResourcePath(), null);
    }

    /**
     * The client went away while we were writing the response (navigated away, aborted fetch). Nothing can
     * be sent back; this is not a server error.
     */
    @ExceptionHandler({org.springframework.web.context.request.async.AsyncRequestNotUsableException.class,
            org.springframework.http.converter.HttpMessageNotWritableException.class})
    ResponseEntity<ApiError> clientGone(Exception e, HttpServletRequest request) {
        Throwable root = e;
        while (root.getCause() != null) root = root.getCause();
        if (root instanceof java.io.IOException) {
            log.debug("client disconnected during {} {}", request.getMethod(), request.getRequestURI());
            return null;
        }
        return unexpected(e, request);
    }

    @ExceptionHandler(Exception.class)
    ResponseEntity<ApiError> unexpected(Exception e, HttpServletRequest request) {
        log.error("unhandled error for {} {}", request.getMethod(), request.getRequestURI(), e);
        return body(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", "internal error; see node log", null);
    }
}
