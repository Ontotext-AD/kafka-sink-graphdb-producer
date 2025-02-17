# kafka-sink-graphdb-producer

## About this Example
This repository contains examples on how data can be ingested to GraphDB.

JSON-LD is a widely used data format that provides a way to express structured data using JSON. It is a simple and flexible way to represent complex data models, making it a popular choice for many applications. In this example we are using JSON-LD in order to ingest data in GraphDB. GraphDB is a powerful graph database that allows you to store and manage large-scale graph data efficiently.

To ingest JSON-LD data into GraphDB there are different approaches. This developer example will demonstrate two of those.

### Kafka SINK Connector
The first way to ingest JSON-LD data is to use the GraphDB open source [Kafka SINK Connector](https://github.com/Ontotext-AD/kafka-sink-graphdb). The Kafka SINK connector uses a Kafka topic in order to ingest data in GraphDB.

### Explanation of the Kafka SINK Example
In the git repository above there is a detailed description of how to start all the necessary tools with docker-compose.

This java project contains a runnable class KafkaSinkProduces which can be used to insert the configured JSON-LD projects in a Kafka topic. Kafka Sink streams the data to GraphDB and the JSON-LD is transformed into triples in the GraphDB.

### Results
The data ingestion of around 50k JSON-LD objects using the Kafka SINK connector inserts around 1.2M statements in GraphDB and takes approximately 3-4 minutes.

### RDF4J to Ingest JSON-LD
Alternative, but less resilient way to achieve JSON-LD data ingestion is to use RDF4J. RDF4J (formerly known as Sesame) is an open-source Java-based framework for managing RDF (Resource Description Framework) data. It provides a comprehensive set of APIs for working with RDF data, including support for querying, storing, and manipulating RDF graphs.

RDF4J is a powerful and flexible framework that supports a range of RDF formats, including RDF/XML, Turtle, and JSON-LD. It also provides a variety of query languages, including SPARQL and SeRQL, which allow you to retrieve and manipulate RDF data in a flexible and expressive way.

One of the key strengths of RDF4J is its scalability. It can handle large-scale RDF datasets and provides efficient storage and querying mechanisms for managing them. It also provides support for distributed querying, batch processing and storage, allowing you to work with large datasets across multiple nodes.

In this example we demonstrate how we can use RDF4J to ingest JSON-LD in GraphDB using simple **batch processing**. The Advantage of this approach in comparison to the Kafka SINK example is that it is more lightweight and requires less configuration.

### Explanation of the RDF4J Example
In this example there is a runnable java class called Rdf4jProducer that once started uploads a certain number of randomly generated JSON-LD objects in GraphDB. The only think you need before running it is to set up GraphDB instance and create a repo in it. After this open application.properties file and set up how much data you want to insert with what batch size. Then only start the program and the data will be inserted.

### Results
The data ingestion of around 50k JSON-LD objects using the Kafka SINK connector inserts around 1.2M statements in GraphDB and takes approximately 2-3 minutes on a desktop machine.