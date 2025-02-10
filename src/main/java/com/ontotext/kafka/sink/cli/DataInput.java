package com.ontotext.kafka.sink.cli;

import org.apache.commons.lang3.StringUtils;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

public record DataInput(Map<String, List<String>> data) {

    public DataInput(Map<String, List<String>> data) {
        this.data = Collections.unmodifiableMap(data);
    }

    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(",");
        data.forEach((k, v) -> joiner.add((StringUtils.isEmpty(k) ? "RANDOM_KEY" : k) + "=" + v));
        return joiner.toString();
    }

    public String[] validateDataFilesExist() {
        return data.values().stream().flatMap(List::stream).filter(s -> !Files.exists(Paths.get(s))).toArray(String[]::new);
    }
}
