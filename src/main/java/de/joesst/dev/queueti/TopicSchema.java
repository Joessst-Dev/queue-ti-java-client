package de.joesst.dev.queueti;

/**
 * A JSON schema associated with a queue-ti topic.
 *
 * @param topic      the topic name; never {@code null}
 * @param schemaJson the JSON Schema document as a string; never {@code null}
 * @param version    schema version number
 * @param updatedAt  ISO-8601 timestamp of the last update; never {@code null}
 */
public record TopicSchema(
        String topic,
        String schemaJson,
        int version,
        String updatedAt) {
}
