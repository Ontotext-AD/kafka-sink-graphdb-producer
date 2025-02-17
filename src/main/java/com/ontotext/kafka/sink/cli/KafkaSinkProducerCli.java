package com.ontotext.kafka.sink.cli;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.ontotext.confurations.RuntimeConfiguration;
import com.ontotext.kafka.sink.cli.input.converter.DataInputConverter;
import com.ontotext.kafka.sink.cli.input.converter.RdfFormatConverter;
import com.ontotext.producer.KafkaSinkProducer;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import static com.ontotext.kafka.sink.cli.KafkaSinkProducerCli.FOOTER;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.slf4j.Logger.ROOT_LOGGER_NAME;

/**
 * <p>
 * The main command line program. Parses the command line arguments and runs the {@link KafkaSinkProducer} to create and send records downstream
 * </p>
 * <h2> CLI usage </h2>
 * To view the help text, simply run the program without arguments, or with
 * <pre>
 *     --help
 * </pre>
 * The following arguments must be provided:
 *
 *     <ul>
 *         <li>--kafka-topic</li>
 *         <li>--rdf-format</li>
 *         <li><b>One of</b> --data, --random-data-size, --interactive</li>
 *     </ul>
 *
 *
 * <h2>Examples</h2>
 * <p id="checksum_example">
 * </pre><p>
 * <h3>Generate X random records</h3>
 * The below command will generate X records with random data, and will send them to the provided (required) kafka topic in the specified (required) RDF format
 * <pre>
 *     --random-data-size X
 * </pre>
 * </p>
 * <h3>Parse and send data on disk</h3>
 * The below command will read the files from disk, parse the records inside, and send them downstream. The key is provided in the argument. The RDF format of the data contents is specified in the file extension
 * <pre>
 *  --data key1=/tmp/data-file-1.ttl --data key1=/tmp/data-file-2.ttl --data key1=/tmp/data-file-3.jsonld
 * </pre>
 * The key will be randomly generated if not provided in the input
 * <pre>
 *  --data /tmp/data-file-1.ttl --data /tmp/data-file-2.ttl --data /tmp/data-file-3.jsonld
 * </pre>
 * This is identical to having the three files mapped to the same (randomly generated) key
 * </p>
 * <h3>Interactively insert records</h3>
 * The following command will start an interactive session with the user.
 * <pre>
 *     --interactive
 * </pre>
 * </p>
 */

@CommandLine.Command(name = "kafka-sink-producer",
        mixinStandardHelpOptions = true,
        versionProvider = KafkaSinkProducerCli.ManifestVersionProvider.class,
        description = "Produces Kafka records for a Kafka Sink Connector",
        footerHeading = "Examples%n",
        footer = FOOTER
)
public class KafkaSinkProducerCli implements Callable<Integer> {

    public static final String FOOTER = """
            
            Generate 10 random JSONLD-formatted records and send them to "add" topic downstream
            
            $ kafka-sink-producer --random-data-size 10 --topic add --rdf-format jsonld
            
            Parse files from disk and send them to "add" topic downstream, in TTL format. Auto-generate keys
            
            $ kafka-sink-producer --data /tmp/data-file1.jsonld --data /tmp/data-file2.ttl --rdf-format ttl --topic add
            
            Parse files from disk, send each data file with separate key
            
            $ kafka-sink-producer --data key1=/tmp/data-file1.jsonld --data key2=/tmp/data-file2.ttl --rdf-format ttl --topic add
            
            Interactively send records
            
            $ kafka-sink-producer --interactive
            
            When interacting with the producer (i.e. --interactive), records can be provided in single line, i.e.
            
            $ <urn:a> <urn:b> <urn:c> .
            
            or multiline, i.e. For multiline, use '\\' to indicate more lines incoming
            
            $ { "id" : "http://example.com#One" \\
                "urn:a" : "one", \\
                "urn:b" : "two", \\
            }
            
            """;

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(KafkaSinkProducerCli.class);

    /**
     * Referenced here for error handling
     */
    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec; // injected by picocli

    /**
     * The kafka topic to send data downstream. This is a required argument
     */
    @CommandLine.Option(names = "--topic", description = "Kafka topic", required = true)
    private String kafkaTopic;

    /**
     * Optional. Use if the Kafka Connect server runs on a separate machine/port. Defaults to localhost:19092
     */
    @CommandLine.Option(names = "--bootstrap-server", description = "Bootstrap server", defaultValue = "127.0.0.1:19092")
    private String bootstrapServer;

    /**
     * -v for DEBUG, -vv for TRACE
     */
    @CommandLine.Option(names = "-v", description = {"Verbosity level.", "For example, `-v -v -v` or `-vvv`"})
    private final boolean[] verbosity = new boolean[0];

    /**
     * Required. Must conform to the format specified in {@link RDFFormat#getDefaultFileExtension}
     */
    @CommandLine.Option(names = "--rdf-format", description = "The RDF format of the data", required = true, converter = RdfFormatConverter.class)
    private RDFFormat rdfFormat;

    /**
     * Optional. Additional properties to pass to the Kafka producer. Properties defined here will override properties with the same key defined in the configurations file
     */
    @CommandLine.Option(names = "--prop", description = "Additional properties to set")
    private final Map<String, String> props = new HashMap<>();

    private void setVerbosity() {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger logger = loggerContext.getLogger(ROOT_LOGGER_NAME);
        logger.setLevel(verbosity.length == 0 ? Level.INFO : verbosity.length == 1 ? Level.DEBUG : Level.TRACE);
    }

    @CommandLine.ArgGroup(multiplicity = "1")
    Mutex mutex;

    /**
     * The CLI accepts only one of the three options below
     * <pre>
     *     --random-data-size INT - auto-generate this number of records
     *     --data - parse provided files
     *     --interactive - start interactive session
     * </pre>
     */
    static class Mutex {
        @CommandLine.Option(names = "--random-data-size", description = "How many random data records to produce. This option is mostly feasible with ADD operations")
        private int randomDataSize;

        @CommandLine.Option(names = "--data", description = {"The data, formatted as key=file.ext to send.", "Different key-filesets can be provided, delimited by ','", "Key can be null"}, converter = DataInputConverter.class)
        DataInput dataInput;

        @CommandLine.Option(names = "--interactive", description = "Paste/write records into the console interactively")
        boolean interactive;


        @Override
        public String toString() {
            if (dataInput == null) {
                return "Random data of size " + randomDataSize;
            }
            return dataInput.toString();
        }
    }

    @Override
    public Integer call() throws Exception {
        setVerbosity();
        validateMutex(mutex);
        log.info("Starting Kafka Sink Producer");
        log.debug("Kafka topic: {}", kafkaTopic);
        log.debug("Bootstrap server: {}", bootstrapServer);
        log.debug("Additional properties: {}", props);
        log.debug(mutex.toString());

        if (StringUtils.isNotEmpty(bootstrapServer)) {
            props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        }
        RuntimeConfiguration configuration = new RuntimeConfiguration(props);
        KafkaSinkProducer producer = new KafkaSinkProducer(kafkaTopic, rdfFormat, configuration);
        if (mutex.interactive) {
            producer.runInteractiveMode();
        } else if (mutex.dataInput == null) {
            producer.sendRandomDataToKafka(mutex.randomDataSize);
        } else
            producer.sendMessagesToKafka(mutex.dataInput);
        return 0;
    }

    private void validateMutex(Mutex mutex) {
        if (mutex.randomDataSize < 0) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    String.format("Invalid data size %d for option '--random-data-size'. Value must be positive", mutex.randomDataSize));
        }
        if (mutex.dataInput != null) {
            String[] dataFilesNotFound = mutex.dataInput.validateDataFilesExist();
            if (ArrayUtils.isNotEmpty(dataFilesNotFound)) {
                throw new CommandLine.ParameterException(spec.commandLine(),
                        String.format("Invalid values for '--data' option. The files [%s] were not found", String.join(",", dataFilesNotFound)));
            }
        }
    }


    public static void main(String[] args) {
        int exitCode = new CommandLine(new KafkaSinkProducerCli()).execute(args);
        System.exit(exitCode);
    }

    static class ManifestVersionProvider implements CommandLine.IVersionProvider {
        public String[] getVersion() throws Exception {
            Enumeration<URL> resources = CommandLine.class.getClassLoader().getResources("META-INF/MANIFEST.MF");
            while (resources.hasMoreElements()) {
                URL url = resources.nextElement();
                try {
                    Manifest manifest = new Manifest(url.openStream());
                    if (isApplicableManifest(manifest)) {
                        Attributes attr = manifest.getMainAttributes();
                        return new String[]{get(attr, "Version").toString()};
                    }
                } catch (IOException ex) {
                    return new String[]{"Unable to read from " + url + ": " + ex};
                }
            }
            return new String[0];
        }

        private boolean isApplicableManifest(Manifest manifest) {
            Attributes attributes = manifest.getMainAttributes();
            return get(attributes, "Version") != null;
        }

        private static Object get(Attributes attributes, String key) {
            return attributes.get(new Attributes.Name(key));
        }
    }
}
