package io.github.mannkostir.projections.examples.resume;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

import org.apache.avro.Schema;

final class AvroSchemas {
    static final Schema CANDIDATE = load("candidate");
    static final Schema SKILL = load("skill");
    static final Schema COMPANY = load("company");
    static final Schema EXPERIENCE = load("experience");
    static final Schema PROJECT = load("project");
    static final Schema CANDIDATE_DOC = load("candidate-doc");
    static final Schema EXPERIENCE_DOC = CANDIDATE_DOC.getField("experiences").schema().getElementType();

    private AvroSchemas() {
    }

    private static Schema load(String name) {
        try (InputStream in = AvroSchemas.class.getResourceAsStream("/avro/" + name + ".avsc")) {
            return new Schema.Parser().parse(in);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read Avro schema avro/" + name + ".avsc", e);
        }
    }
}
