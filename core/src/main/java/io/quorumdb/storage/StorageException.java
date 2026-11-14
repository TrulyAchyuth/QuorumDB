package io.quorumdb.storage;

/** Stable storage failed or is corrupt. The owning node must stop participating (fail-stop). */
public class StorageException extends RuntimeException {
    public StorageException(String message) {
        super(message);
    }

    public StorageException(String message, Throwable cause) {
        super(message, cause);
    }

    /** Persisted data failed validation in a way that cannot be explained by a torn final write. */
    public static final class Corrupted extends StorageException {
        public Corrupted(String message) {
            super(message);
        }
    }
}
