package com.ontotext.rdf;

import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class RdfFormatConverter {

    protected final Logger log;
    private final RDFFormat inputFormat;
    private final RDFFormat outputFormat;

    public RdfFormatConverter(RDFFormat inputFormat, RDFFormat outputFormat) {
        this.inputFormat = inputFormat;
        this.outputFormat = outputFormat;
        this.log = LoggerFactory.getLogger(String.format("Converter [%s]->[%s]", inputFormat, outputFormat));
    }


    public byte[] convertData(byte[] inputData) throws IOException {
        if (inputFormat.equals(outputFormat)) {
            log.debug("Input and output formats [{}] are the same, skipping conversion", inputData);
            return inputData;
        }
        log.debug("Converting data from {} to {}", inputFormat, outputFormat);
        RDFParser rdfParser = Rio.createParser(inputFormat);
        try (InputStream is = new ByteArrayInputStream(inputData);
             ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
             BufferedOutputStream bos = new BufferedOutputStream(baos)) {
            RDFWriter rdfWriter = Rio.createWriter(outputFormat, bos);
            rdfParser.setRDFHandler(rdfWriter);
            rdfParser.parse(is);
            return baos.toByteArray();
        }
    }


    public RDFFormat getInputFormat() {
        return inputFormat;
    }

    public RDFFormat getOutputFormat() {
        return outputFormat;
    }

    public static RDFFormat getRDFFormat(String format) {
        if (RDFFormat.RDFXML.getDefaultFileExtension().contains(format)) {
            return RDFFormat.RDFXML;
        } else if (RDFFormat.NTRIPLES.getDefaultFileExtension().contains(format)) {
            return RDFFormat.NTRIPLES;
        } else if (RDFFormat.TURTLE.getDefaultFileExtension().contains(format)) {
            return RDFFormat.TURTLE;
        } else if (RDFFormat.TURTLESTAR.getDefaultFileExtension().contains(format)) {
            return RDFFormat.TURTLESTAR;
        } else if (RDFFormat.N3.getDefaultFileExtension().contains(format)) {
            return RDFFormat.N3;
        } else if (RDFFormat.TRIX.getDefaultFileExtension().contains(format)) {
            return RDFFormat.TRIX;
        } else if (RDFFormat.TRIG.getDefaultFileExtension().contains(format)) {
            return RDFFormat.TRIG;
        } else if (RDFFormat.TRIGSTAR.getDefaultFileExtension().contains(format)) {
            return RDFFormat.TRIGSTAR;
        } else if (RDFFormat.BINARY.getDefaultFileExtension().contains(format)) {
            return RDFFormat.BINARY;
        } else if (RDFFormat.NQUADS.getDefaultFileExtension().contains(format)) {
            return RDFFormat.NQUADS;
        } else if (RDFFormat.JSONLD.getDefaultFileExtension().contains(format)) {
            return RDFFormat.JSONLD;
        } else if (RDFFormat.NDJSONLD.getDefaultFileExtension().contains(format)) {
            return RDFFormat.NDJSONLD;
        } else if (RDFFormat.RDFJSON.getDefaultFileExtension().contains(format)) {
            return RDFFormat.RDFJSON;
        } else if (RDFFormat.RDFA.getDefaultFileExtension().contains(format)) {
            return RDFFormat.RDFA;
        } else if (RDFFormat.HDT.getDefaultFileExtension().contains(format)) {
            return RDFFormat.HDT;
        } else {
            throw new IllegalArgumentException("Invalid RDF Format " + format);
        }
    }

}
