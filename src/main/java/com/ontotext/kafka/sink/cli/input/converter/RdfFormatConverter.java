package com.ontotext.kafka.sink.cli.input.converter;


import org.eclipse.rdf4j.rio.RDFFormat;
import picocli.CommandLine;

import static com.ontotext.rdf.RdfFormatConverter.getRDFFormat;

public final class RdfFormatConverter implements CommandLine.ITypeConverter<RDFFormat> {
    @Override
    public RDFFormat convert(String s) throws Exception {
        return getRDFFormat(s);
    }
}
