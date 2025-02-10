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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import static com.ontotext.confurations.RuntimeConfiguration.VERSION;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.slf4j.Logger.ROOT_LOGGER_NAME;

@CommandLine.Command(name = "kafka-sink-producer", mixinStandardHelpOptions = true, version = VERSION, description = "Produces Kafka records for a Kafka Sink Connector")
public class KafkaSinkProducerCli implements Callable<Integer> {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(KafkaSinkProducerCli.class);

    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec; // injected by picocli

    @CommandLine.Option(names = "--topic", description = "Kafka topic", required = true)
    private String kafkaTopic;

    @CommandLine.Option(names = "--bootstrap-server", description = "Bootstrap server", defaultValue = "127.0.0.1:19092")
    private String bootstrapServer;

    @CommandLine.Option(names = "-v", description = {"Verbosity level.", "For example, `-v -v -v` or `-vvv`"})
    private final boolean[] verbosity = new boolean[0];

    @CommandLine.Option(names = "--rdf-format", description = "The RDF format of the data", required = true, converter = RdfFormatConverter.class)
    private RDFFormat rdfFormat;

    @CommandLine.Option(names = "--prop", description = "Additional properties to set")
    private final Map<String, String> props = new HashMap<>();

    private void setVerbosity() {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger logger = loggerContext.getLogger(ROOT_LOGGER_NAME);
        logger.setLevel(verbosity.length == 0 ? Level.INFO : verbosity.length == 1 ? Level.DEBUG : Level.TRACE);
    }

    @CommandLine.ArgGroup(multiplicity = "1")
    Mutex mutex;

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
}
