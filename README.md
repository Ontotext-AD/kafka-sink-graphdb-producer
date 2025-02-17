# Kafka Sink Producer CLI

The Kafka Sink Producer CLI is a Java-based command-line application designed to produce and send RDF-formatted data to a Kafka topic. It supports multiple
modes of operation, including generating random data, parsing files, and interactive input. The application is built using [Picocli](https://picocli.info/) for
command-line parsing and requires **Java 21 or higher** to run.

---

## Features

- **Multiple Data Input Modes**:
  - Generate random RDF data in JSON-LD or TTL format.
  - Parse RDF data from files on disk.
  - Interactive mode for manual input of RDF records.
- **Customizable Kafka Producer Properties**: Override or extend Kafka producer configurations via command-line options.
- **Docker Compose Support**: Easily set up a local GDB + Kafka environment using the provided `docker-compose.yml` file.

---

## Prerequisites

- **Java 21 or higher**: Ensure you have Java 21+ installed on your system.
- **Docker Compose**: Required if running Kafka and GraphDB environment locally.
- **[GDB Kafka Sink Connector](https://github.com/Ontotext-AD/kafka-sink-graphdb) version 3.0.0 or higher**

---

## Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/Ontotext-AD/kafka-sink-graphdb-producer.git
cd kafka-sink-graphdb-producer
```

### 2. Set Up GDB and Kafka with Docker Compose

```bash
cd docker-compose
docker-compose up -d
```

This will start a Kafka broker and Zookeeper instance on localhost:19092. Additionally, it will spin up 3 kafka sink connector instances and a GraphDB instance

### 3. Build the Application

Compile and package the application using Maven:

```bash
mvn clean verify
```

This will generate a distribution file (*tar* and *zip*) in **build/distributions**

### 4. Extract distribution

Choose the distribution file and extract. This can be run on **Windows** or **UNIX** operating systems that have **Java 21+** installed.

```bash
cd build/distributions
tar xvf kafka-producer-XXX.tar
cd kafka-producer-XXX/bin
/bin/bash kafka-producer
```

## Usage

The Kafka Sink Producer CLI provides several options for producing and sending RDF data to a Kafka topic. Below are some common usage examples.

### Help Menu

Run `./kafka-producer -h` to see the entire usage menu:

```cli
./kafka-producer -h
Usage: kafka-sink-producer [-hVv] [--bootstrap-server=<bootstrapServer>]
                           --rdf-format=<rdfFormat> --topic=<kafkaTopic>
                           [--prop=<String=String>]...
                           (--random-data-size=<randomDataSize> |
                           --data=<dataInput> | --interactive)
Produces Kafka records for a Kafka Sink Connector
      --bootstrap-server=<bootstrapServer>
                             Bootstrap server
      --data=<dataInput>     The data, formatted as key=file.ext to send.
                             Different key-filesets can be provided, delimited
                               by ','
                             Key can be null
  -h, --help                 Show this help message and exit.
      --interactive          Paste/write records into the console interactively
      --prop=<String=String> Additional properties to set
      --random-data-size=<randomDataSize>
                             How many random data records to produce. This
                               option is mostly feasible with ADD operations
      --rdf-format=<rdfFormat>
                             The RDF format of the data
      --topic=<kafkaTopic>   Kafka topic
  -v                         Verbosity level.
                             For example, `-v -v -v` or `-vvv`
  -V, --version              Print version information and exit.

```

### Generate Random Data

Generate 10 random JSON-LD records and send them to the `add` Kafka topic:

```bash
./kafka-producer --random-data-size 10 --topic add --rdf-format jsonld
```

### Parse Files from Disk

Parse RDF files from disk and send them to the `add` topic in **TTL** format. Keys will be auto-generated:

```bash
./kafka-producer --data /tmp/data-file1.jsonld --data /tmp/data-file2.ttl --rdf-format ttl --topic add
```

### Parse files with custom Kafka record keys:

```bash
./kafka-producer --data key1=/tmp/data-file1.jsonld --data key2=/tmp/data-file2.ttl --rdf-format ttl --topic add
```

### Interactive Mode

Start an interactive session to manually input RDF records:

```bash
./kafka-producer --interactive --topic add --rdf-format ttl
```

In interactive mode, you can provide records in single-line or multi-line format. For multi-line input, use \ to indicate continuation:

```bash
$ <urn:a> <urn:b> <urn:c> .
```

```bash
$ { "id" : "http://example.com#One" \\
"urn:a" : "one", \\
"urn:b" : "two", \\
}
```

### Custom Kafka Producer Properties

Override or extend Kafka producer properties:

```bash
./kafka-producer --topic add --rdf-format jsonld --prop key1=value1 --prop key2=value2
```

### Verbosity Control

Increase logging verbosity for debugging:

```bash
./kafka-producer --topic add --rdf-format jsonld -v -v -v
```

## Configuration

### Kafka Producer Properties

You can configure Kafka producer properties in the graphdb-kafka-sink.properties file or override them using the --prop option.

Example graphdb-kafka-sink.properties:

```properties
bootstrap.servers=127.0.0.1:19092
key.serializer=org.apache.kafka.common.serialization.StringSerializer
value.serializer=org.apache.kafka.common.serialization.StringSerializer
```

## Troubleshooting

- Kafka Connection Issues: Ensure the Kafka broker is running and accessible at the specified --bootstrap-server.

- File Parsing Errors: Verify that the input files are valid RDF documents in the specified format.

- Verbosity: Use -v or -vv for detailed logging to diagnose issues.