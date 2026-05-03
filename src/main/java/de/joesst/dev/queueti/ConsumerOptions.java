package de.joesst.dev.queueti;

/**
 * Configuration options for a queue consumer.
 *
 * <p>Instances are immutable and thread-safe.
 */
public final class ConsumerOptions {

    private final int concurrency;
    private final String consumerGroup;
    private final Integer visibilityTimeoutSeconds;

    private ConsumerOptions(Builder builder) {
        if (builder.concurrency < 1) {
            throw new IllegalArgumentException("concurrency must be >= 1");
        }
        this.concurrency = builder.concurrency;
        this.consumerGroup = builder.consumerGroup;
        this.visibilityTimeoutSeconds = builder.visibilityTimeoutSeconds;
    }

    /**
     * Returns a new {@link Builder} for constructing a {@code ConsumerOptions}.
     *
     * @return a fresh builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the number of messages to process concurrently. Defaults to {@code 1}.
     *
     * @return the concurrency level, always {@code >= 1}
     */
    public int getConcurrency() {
        return concurrency;
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

    /** Builder for {@link ConsumerOptions}. */
    public static final class Builder {

        private int concurrency = 1;
        private String consumerGroup = "";
        private Integer visibilityTimeoutSeconds = null;

        private Builder() {}

        /**
         * Sets the number of messages to process concurrently.
         *
         * @param concurrency the concurrency level; must be {@code >= 1}
         * @return this builder
         */
        public Builder concurrency(int concurrency) {
            this.concurrency = concurrency;
            return this;
        }

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
         * Builds the {@link ConsumerOptions}.
         *
         * @return a new immutable {@code ConsumerOptions}
         * @throws IllegalArgumentException if concurrency is less than 1
         */
        public ConsumerOptions build() {
            return new ConsumerOptions(this);
        }
    }
}
