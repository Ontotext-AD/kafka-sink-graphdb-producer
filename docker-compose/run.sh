#!/usr/bin/env bash

set -eu

function wait_service {
	printf "waiting for $1"
	until curl -s --fail -m 1 "$1" &> /dev/null; do
		sleep 1
		printf '.'
	done
	echo
}

function create_graphdb_repo {
	if ! curl --fail -X GET --header 'Accept: application/json' http://localhost:7200/rest/repositories/test &> /dev/null; then
		curl 'http://localhost:7200/rest/repositories' \
			-H 'Accept: application/json, text/plain, */*' \
			-H 'Content-Type: application/json;charset=UTF-8' \
			-d '{"id": "test", "params": {"imports": {"name": "imports", "label": "Imported RDF files('\'';'\'' delimited)", "value": ""}, "defaultNS": {"name": "defaultNS", "label": "Default namespaces for imports('\'';'\'' delimited)", "value": ""}}, "title": "", "type": "graphdb", "location": ""}'
	fi
}

function create_add_kafka_sink_connector {
	if ! curl -s --fail localhost:8083/connectors/kafka-sink-graphdb &> /dev/null; then
		curl -H 'Content-Type: application/json' --data '
		{
			"name": "kafka-sink-add",
			"config": {
				"connector.class":"com.ontotext.kafka.GraphDBSinkConnector",
				"key.converter": "com.ontotext.kafka.convert.DirectRDFConverter",
				"value.converter": "com.ontotext.kafka.convert.DirectRDFConverter",
				"value.converter.schemas.enable": "false",
				"topics":"add-data",
				"tasks.max":"1",
				"offset.storage.file.filename": "/tmp/storage",
				"graphdb.server.url": "http://graphdb:7200",
				"graphdb.server.repository": "test",
				"graphdb.batch.size": 1000,
				"graphdb.batch.commit.limit.ms": 1000,
				"graphdb.auth.type": "NONE",
				"graphdb.update.type": "ADD",
				"graphdb.update.rdf.format": "jsonld"
			}
		}' http://localhost:8083/connectors -w "\n"
	fi
}

function create_replace_kafka_sink_connector {
	if ! curl -s --fail localhost:8083/connectors/kafka-sink-graphdb &> /dev/null; then
		curl -H 'Content-Type: application/json' --data '
		{
			"name": "kafka-sink-replace",
			"config": {
				"connector.class":"com.ontotext.kafka.GraphDBSinkConnector",
				"key.converter": "com.ontotext.kafka.convert.DirectRDFConverter",
				"value.converter": "com.ontotext.kafka.convert.DirectRDFConverter",
				"value.converter.schemas.enable": "false",
				"topics":"replace-data",
				"tasks.max":"1",
				"offset.storage.file.filename": "/tmp/storage",
				"graphdb.server.url": "http://graphdb:7200",
				"graphdb.server.repository": "test",
				"graphdb.batch.size": 1000,
				"graphdb.batch.commit.limit.ms": 1000,
				"graphdb.auth.type": "NONE",
				"graphdb.update.type": "REPLACE_GRAPH",
				"graphdb.update.rdf.format": "jsonld"
			}
		}' http://localhost:8084/connectors -w "\n"
	fi
}

docker compose up -d

# wait for graphdb and connect to start
wait_service 'http://localhost:7200/protocol'
echo 'GraphDB started at 127.0.0.1:7200'
wait_service 'http://localhost:8083'
echo 'GraphDB Kafka Connect-1 started at 8083'
wait_service 'http://localhost:8084'
echo 'GraphDB Kafka Connect-1 started at 8084'

# create a test repository in graphdb
create_graphdb_repo

# create the graphdb kafka sink connector for the add operation
create_add_kafka_sink_connector
# create the graphdb kafka sink connector for the update operation
create_replace_kafka_sink_connector

echo 'Initialization Completed!'
