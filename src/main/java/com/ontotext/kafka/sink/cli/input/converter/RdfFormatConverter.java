package com.ontotext.kafka.sink.cli.input.converter;


import org.eclipse.rdf4j.rio.RDFFormat;
import picocli.CommandLine;

import static com.ontotext.rdf.RdfFormatConverter.getRDFFormat;

/**
 * A helper class to convert a string representation of RDF format to {@link RDFFormat} object
 */
public final class RdfFormatConverter implements CommandLine.ITypeConverter<RDFFormat> {
    @Override
    public RDFFormat convert(String s) throws Exception {
        return getRDFFormat(s);
    }
}
