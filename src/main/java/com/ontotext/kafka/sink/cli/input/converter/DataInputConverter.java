package com.ontotext.kafka.sink.cli.input.converter;

import com.ontotext.kafka.sink.cli.DataInput;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An implementation of {@link picocli.CommandLine.ITypeConverter} to convert a {@link java.lang.String} cmdline argument to
 * {@link Map<String, List<String>>} map. The keys of the map are used as {@link org.apache.kafka.clients.producer.ProducerRecord} keys,
 * the values are files on disk containing the actual records.
 */

public final class DataInputConverter implements CommandLine.ITypeConverter<DataInput> {

    /**
     * @param value the command line argument
     * @return a map of record keys -> list of files
     * @throws Exception
     */
    @Override
    public DataInput convert(String value) throws Exception {
        Map<String, List<String>> result = new HashMap<>();
        String[] split = value.split(",");
        for (String string : split) {
            String[] keyValue = string.split("=");
            if (keyValue.length == 2) {
                result.compute(keyValue[0], (k, v) -> v == null ? new ArrayList<>() : v).add(keyValue[1]);
            } else {
                // Treat it as value with key to be randomly generated
                result.compute(null, (k, v) -> v == null ? new ArrayList<>() : v).add(keyValue[0]);
            }
        }
        return new DataInput(result);
    }
}
