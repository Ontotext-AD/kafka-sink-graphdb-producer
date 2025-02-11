package com.ontotext.producer;

import com.ontotext.confurations.RuntimeConfiguration;
import com.ontotext.kafka.sink.cli.DataInput;
import com.ontotext.model.jsonld.JsonldBuilder;
import com.ontotext.rdf.RdfFormatConverter;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Produces and sends Kafka records based on chosen strategy.
 * Currently supported strategies:
 * <ul>
 *     <li>Random data</li>
 *     <li>Data files</li>
 *     <li>Interactive</li>
 * </ul>
 *
 * @see com.ontotext.kafka.sink.cli.KafkaSinkProducerCli
 */
public class KafkaSinkProducer {
    private static final Logger log = LoggerFactory.getLogger(KafkaSinkProducer.class);
    private final String kafkaTopic;
    private final RuntimeConfiguration configuration;
    private final RDFFormat outputRdfFormat;

    public KafkaSinkProducer(String kafkaTopic, RDFFormat outputRdfFormat, RuntimeConfiguration configuration) {
        log.info("Initializing Kafka producer for Kafka topic {}", kafkaTopic);
        this.kafkaTopic = kafkaTopic;
        this.outputRdfFormat = outputRdfFormat;
        this.configuration = configuration;
    }

    /**
     * Parses the provided data files for (valid) records and sends them downstream.
     * Gracefully handles and logs any invalid records (or any related failures)
     *
     * @param dataInput - the data files, mapped by record keys
     */
    public void sendMessagesToKafka(DataInput dataInput) {
        log.info("Sending {} to kafka topic {}", dataInput.data(), kafkaTopic);
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(configuration.getProperties())) {
            int totalRecordsSent = 0;
            int numFailedRecords = 0;
            long startTime = System.currentTimeMillis();
            for (Map.Entry<String, List<String>> entry : dataInput.data().entrySet()) {
                String key = entry.getKey();
                List<String> files = entry.getValue();
                if (StringUtils.isEmpty(key)) {
                    log.debug("Generating random key");
                    key = UUID.randomUUID().toString();
                }
                for (String file : files) {
                    log.debug("Parsing rdf format from file name {}", file);
                    String ext = FilenameUtils.getExtension(file);
                    RDFFormat inputFormat = RdfFormatConverter.getRDFFormat(ext);
                    log.debug("Data is in format {}", inputFormat);
                    RdfFormatConverter dataConverter = new RdfFormatConverter(inputFormat, outputRdfFormat);
                    try {
                        byte[] contents = Files.readAllBytes(Paths.get(file));
                        log.info("Converting contents from file {} from {} to {}", file, inputFormat, outputRdfFormat);
                        byte[] converted = dataConverter.convertData(contents);
                        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(kafkaTopic, key.getBytes(Charset.defaultCharset()), converted);
                        producer.send(producerRecord);
                        totalRecordsSent++;
                    } catch (Exception e) {
                        log.error("Error reading file {}. Skipping this file", file, e);
                        numFailedRecords++;
                    }
                }
            }
            long endTime = System.currentTimeMillis();
            long totalRunTime = TimeUnit.MILLISECONDS.convert(endTime - startTime, TimeUnit.SECONDS);
            log.info("Successfully sent {} records to kafka topic {}. Failed number of records : {}. Time elapsed: {} seconds", totalRecordsSent, kafkaTopic,
                    numFailedRecords, totalRunTime);
            producer.flush();
        }
    }


    /**
     * Generates random records and sends them downstream
     * @param randomDataSize the number of randomly generated records
     */
    public void sendRandomDataToKafka(int randomDataSize) {
        log.info("Sending {} randomized data records to kafka topic {}", randomDataSize, kafkaTopic);
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(configuration.getProperties())) {
            long startTime = System.currentTimeMillis();
            for (int i = 0; i < randomDataSize; i++) {
                try {
                    log.debug("Creating a new object...");
                    byte[] obj = JsonldBuilder.build();
                    log.info("Converting contents from from jsonld to {}", outputRdfFormat);
                    RdfFormatConverter dataConverter = new RdfFormatConverter(RDFFormat.JSONLD, outputRdfFormat);
                    byte[] converted = dataConverter.convertData(obj);
                    ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(kafkaTopic, converted);
                    producer.send(producerRecord);
                    log.debug("Object successfully sent to kafka topic {}", kafkaTopic);
                } catch (Exception e) {
                    log.error("Could not send message to kafka topic {}", kafkaTopic, e);
                }

            }
            long endTime = System.currentTimeMillis();
            log.info("Successfully sent {} randomized data records to kafka topic {}. Time elapsed: {} seconds", randomDataSize, kafkaTopic,
                    TimeUnit.MILLISECONDS.convert(endTime - startTime, TimeUnit.SECONDS));
            // flush data - synchronous
            producer.flush();
        }
    }

    /**
     * Start an interactive session.
     * The interactive flow is as follows:
     * <pre>1. Ask the user for the input RDF format of all subsequent data. The format is constant for the entirety of the session</pre>
     * <pre>2. Aask for a new record and key. If no key is provided, one will be auto-generated.
     * </p> The record can be provided in a single line, or if multiline, Bash-style multiline formatting is used (i.e. put \ at the end of the line to denote more lines incoming)
     * </pre>
     * <pre>3. Parse and convert the record into the output RDF format</pre>
     * <pre>4. Send the record downstream</pre>
     * <pre>5. Repeat until process is interrupted (i.e. Ctrl+C)</pre>
     */
    public void runInteractiveMode() {
        log.info("Starting interactive mode");
        Signal.handle(new Signal("INT"),
                signal -> {
                    System.out.println("Ctrl+C");
                    System.exit(2); //kill -2 (SIGINT)
                });
        Scanner scanner = new Scanner(System.in);
        System.out.println("Choose RDF format for the data : (Default: jsonld)");
        String formatStr = scanner.nextLine();
        if (StringUtils.isEmpty(formatStr)) {
            formatStr = "jsonld";
        }
        RDFFormat inputRdfFormat = RdfFormatConverter.getRDFFormat(formatStr);
        RdfFormatConverter dataConverter = new RdfFormatConverter(inputRdfFormat, outputRdfFormat);
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(configuration.getProperties())) {
            while (true) {
                try {
                    System.out.println("Record key (leave blank to generate a random key): ");
                    String key = scanner.nextLine();
                    if (StringUtils.isEmpty(key)) {
                        log.info("Generating random key");
                        key = UUID.randomUUID().toString();
                    }
                    log.info("Record key is {}", key);
                    String nextRecord = getNextRecord(scanner);
                    if (nextRecord == null) {
                        break;
                    }
                    log.debug("Record : {}", nextRecord);
                    byte[] contents = nextRecord.getBytes(Charset.defaultCharset());
                    log.info("Converting record from input format {} to {}", inputRdfFormat, outputRdfFormat);
                    byte[] converted = dataConverter.convertData(contents);
                    ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(kafkaTopic, key.getBytes(Charset.defaultCharset()), converted);
                    producer.send(producerRecord);
                } catch (Exception e) {
                    log.error("Could not send record", e);
                }
            }
        }
    }

    private String getNextRecord(Scanner scanner) {
        StringJoiner sj = new StringJoiner("\n");
        String line = "";
        System.out.println("Paste or write next record to send." +
                " If multiline, use Bash-style multiline format (append \\ at the end of a line): ");
        while (true) {
            line = scanner.nextLine();
            sj.add(line.replace("\\", ""));
            if (!line.endsWith("\\")) { // more to follow
                break;
            }
        }
        return sj.toString();
    }
}