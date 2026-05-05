package de.joesst.dev.queueti;

/**
 * Thrown when the queue-ti admin API returns HTTP 404 Not Found.
 */
public final class NotFoundException extends RuntimeException {

    /**
     * Constructs a {@code NotFoundException} with the given detail message.
     *
     * @param message a human-readable description of what was not found
     */
    public NotFoundException(final String message) {
        super(message);
    }
}
