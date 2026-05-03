package de.joesst.dev.queueti;

/**
 * Configuration options for a batch consume operation.
 *
 * <p>Instances are immutable and thread-safe.
 */
public final class BatchOptions {

    private final String consumerGroup;
    private final Integer visibilityTimeoutSeconds;

    private BatchOptions(Builder builder) {
        this.consumerGroup = builder.consumerGroup;
        this.visibilityTimeoutSeconds = builder.visibilityTimeoutSeconds;
    }

    /**
     * Returns a new {@link Builder} for constructing a {@code BatchOptions}.
     *
     * @return a fresh builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the consumer group name. Defaults to an empty string.
     *
     * @return the consumer group; never {@code null}
     */
    public String getConsumerGroup() {
        return consumerGroup;
    }

    /**
     * Returns the visibility timeout in seconds, or {@code null} if the server default applies.
     *
     * @return the visibility timeout in seconds, or {@code null}
     */
    public Integer getVisibilityTimeoutSeconds() {
        return visibilityTimeoutSeconds;
    }

    /** Builder for {@link BatchOptions}. */
    public static final class Builder {

        private String consumerGroup = "";
        private Integer visibilityTimeoutSeconds = null;

        private Builder() {}

        /**
         * Sets the consumer group name.
         *
         * @param consumerGroup the group name; must not be {@code null}
         * @return this builder
         */
        public Builder consumerGroup(String consumerGroup) {
            this.consumerGroup = consumerGroup;
            return this;
        }

        /**
         * Sets the message visibility timeout in seconds.
         *
         * @param visibilityTimeoutSeconds timeout in seconds; may be {@code null} to use the
         *        server default
         * @return this builder
         */
        public Builder visibilityTimeoutSeconds(Integer visibilityTimeoutSeconds) {
            this.visibilityTimeoutSeconds = visibilityTimeoutSeconds;
            return this;
        }

        /**
         * Builds the {@link BatchOptions}.
         *
         * @return a new immutable {@code BatchOptions}
         */
        public BatchOptions build() {
            return new BatchOptions(this);
        }
    }
}
