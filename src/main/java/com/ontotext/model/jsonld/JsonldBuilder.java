package com.ontotext.model.jsonld;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Class used to build a generic json-ld with some random data.
 */
public class JsonldBuilder {

    private static final ObjectMapper om = new ObjectMapper();

    /**
     * Builds a json-ld with some random data.
     *
     * @return the json-ld
     */
    public static byte[] build() {
        try {
            return om.writeValueAsBytes(build(String.format("https://id.example.com/%s", UUID.randomUUID())));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param graphIRI the IRI of the named graph in which the data is placed
     * @return the json-ld as a string
     */
    public static Map<String, Object> build(String graphIRI) {
        Map<String, Object> jsonLd = new HashMap<>();
        jsonLd.put("@context", "http://schema.org/docs/jsonldcontext.json");
        jsonLd.put("@id", graphIRI);

        var graph = new HashMap<String, Object>();
        graph.put("@id", String.format("https://id.example.com/graph/%s", UUID.randomUUID()));
        graph.put("@type", "https://type.example.com/" + UUID.randomUUID());
        graph.put("createdFor", createdFor());
        graph.put("assignedNumber", UUID.randomUUID());
        graph.put("comment", UUID.randomUUID());
        graph.put("reviewedBy", reviewedBy());
        graph.put("denotes", generateDenotes());
        graph.put("isPrimaryTopicOf", createIsPrimaryTopicOf());

        jsonLd.put("@graph", graph);
        return jsonLd;
    }

    private static Map<String, Object> generateDenotes() {
        Map<String, Object> denotes = new HashMap<>();
        denotes.put("@id", String.format("https://id.generate.com/a6/studyDesignExecution/dev/%s", UUID.randomUUID()));
        denotes.put("@type", "https://type.generate.com/" + UUID.randomUUID());
        return denotes;
    }

    private static Map<String, Object> createdBy() {
        Map<String, Object> createdBy = new HashMap<>();
        createdBy.put("@id", "https://id.created-by.com/" + UUID.randomUUID());
        createdBy.put("@type", "https://type.created-by.com/" + UUID.randomUUID());
        createdBy.put("userId", UUID.randomUUID());
        createdBy.put("userName", UUID.randomUUID());
        return createdBy;
    }

    private static Map<String, Object> createLastModifiedBy() {
        Map<String, Object> lastlyModifiedBy = new HashMap<>();
        lastlyModifiedBy.put("@id", "https://id.last-modified.com/" + UUID.randomUUID());
        lastlyModifiedBy.put("@type", "https://type.last-modified.com/" + UUID.randomUUID());
        lastlyModifiedBy.put("userId", UUID.randomUUID());
        lastlyModifiedBy.put("userName", UUID.randomUUID());
        return lastlyModifiedBy;
    }

    private static Map<String, Object> createStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("@id", String.format("https://id.status.com/%s", UUID.randomUUID()));
        status.put("prefLabel", UUID.randomUUID());
        status.put("@type", "https://type.status.com/" + UUID.randomUUID());
        return status;
    }

    private static Map<String, Object> createIsPrimaryTopicOf() {
        Map<String, Object> isPrimaryTopicOf = new HashMap<>();
        isPrimaryTopicOf.put("@id", String.format("https://id.status.com/" + "ROX%s", UUID.randomUUID()));
        isPrimaryTopicOf.put("@type", "https://type.primary-topic.com/" + UUID.randomUUID());
        isPrimaryTopicOf.put("createdBy", createdBy());
        isPrimaryTopicOf.put("lastlyModifiedBy", createLastModifiedBy());
        isPrimaryTopicOf.put("createdOn", "2023-01-19T11:54:05.080+00:00");
        isPrimaryTopicOf.put("lastlyModifiedOn", "2023-01-23T08:56:57.406+00:00");
        isPrimaryTopicOf.put("lockedBySourceSystem", false);
        isPrimaryTopicOf.put("status", createStatus());
        return isPrimaryTopicOf;
    }

    private static Map<String, Object> createdFor() {
        Map<String, Object> designedFor = new HashMap<>();
        designedFor.put("@id", String.format("https://id.generated-design.com/a2/dev/%s", UUID.randomUUID()));
        designedFor.put("@type", "https://type.generated-design.com/" + UUID.randomUUID());
        return designedFor;
    }

    private static Map<String, Object> reviewedBy() {
        Map<String, Object> reviewedBy = new HashMap<>();
        reviewedBy.put("@id", "https://id.reviewed-by.com/" + UUID.randomUUID());
        reviewedBy.put("@type", "https://id.reviewed.com/" + UUID.randomUUID());
        reviewedBy.put("userId", UUID.randomUUID());
        reviewedBy.put("userName", UUID.randomUUID());
        return reviewedBy;
    }

}