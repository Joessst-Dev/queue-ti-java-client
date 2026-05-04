package de.joesst.dev.queueti;

import java.util.Map;

/**
 * Configuration options for publishing a message to a queue.
 *
 * <p>Instances are immutable and thread-safe.
 */
public final class PublishOptions {

    private final Map<String, String> metadata;
    private final String key;

    private PublishOptions(Builder builder) {
        this.metadata = builder.metadata != null ? Map.copyOf(builder.metadata) : Map.of();
        this.key = builder.key;
    }

    /**
     * Returns a new {@link Builder} for constructing a {@code PublishOptions}.
     *
     * @return a fresh builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the message metadata. Never {@code null}; defaults to an empty map.
     *
     * @return an immutable metadata map
     */
    public Map<String, String> getMetadata() {
        return metadata;
    }

    /**
     * Returns the optional routing key, or {@code null} if none was set.
     *
     * @return the routing key, or {@code null}
     */
    public String getKey() {
        return key;
    }

    /** Builder for {@link PublishOptions}. */
    public static final class Builder {

        private Map<String, String> metadata = Map.of();
        private String key = null;

        private Builder() {}

        /**
         * Sets the message metadata. A {@code null} value is coerced to an empty map.
         *
         * @param metadata the metadata map; may be {@code null}
         * @return this builder
         */
        public Builder metadata(Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        /**
         * Sets the optional routing key.
         *
         * @param key the routing key; may be {@code null}
         * @return this builder
         */
        public Builder key(String key) {
            this.key = key;
            return this;
        }

        /**
         * Builds the {@link PublishOptions}.
         *
         * @return a new immutable {@code PublishOptions}
         */
        public PublishOptions build() {
            return new PublishOptions(this);
        }
    }
}
