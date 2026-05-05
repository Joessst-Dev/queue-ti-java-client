package de.joesst.dev.queueti;

/**
 * Thrown when the queue-ti admin API returns HTTP 409 Conflict.
 */
public final class ConflictException extends RuntimeException {

    /**
     * Constructs a {@code ConflictException} with the given detail message.
     *
     * @param message a human-readable description of the conflict
     */
    public ConflictException(final String message) {
        super(message);
    }
}
