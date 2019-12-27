# Kstreams Key-value API

A simple example Kafka Streams application which serves a ktable over http.  Produce messages to Kafka, and retrieve the latest value for a given key over http.

I couldn't find any good examples for this, and came across a few gotchas when doing it myself.  So I'm sharing this.  It's not production-ready, it's a minimal example of how to achieve this.

## Assumptions

We assume that Kafka is running locally, listening in plaintext on `localhost:9092` with no authentication required.  You can achieve this with a variation of [my docker-compose](https://gist.github.com/WheresAlice/e02935a0bc291de3cd2c8ccaf5b7579e) (you only need Kafka and Zookeeper services).

## Building

`mvn clean package`

## Usage

Create the topic `keyvalue-store`.  This needs to exist before running Kafka Streams.  It should be a compacted topic to save space and processing time.

```shell script
kafka-topics --bootstrap-server localhost:9092 --create --topic keyvalue-store --config "cleanup.policy=compact" --config "delete.retention.ms=100" --partitions 1 --replication-factor 1
```

Run the main application with `mvn exec:java -Dexec.mainClass=kafka.App`

Produce data with `mvn exec:java -Dexec.mainClass=kafka.Producer` (or use any Kafka producer)

Get the values from the http server with:
 
```shell script
curl localhost:4567/alice
curl localhost:4567/bob
curl localhost:4567/carol
```

## Debug

Print messages on the Kafka topic with `mvn exec:java -Dexec.mainClass=kafka.Debug`

## Dependencies

* Kafka Streams (part of Apache Kafka)
* [Spark](http://sparkjava.com/) (the microframework for web applications, not the big data framework)