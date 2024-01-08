# UForwarder

## Overview

Uforwarder is a proxy that transfers data from kafka to message consumer through RPC protocol.
Uforwarder aims to achieve reliable message delivery in realtime at scale through simple RPC protocol between Kafka and consumers.

Properties and Traits:

- Out of order Message delivery
- At least once message delivery
- Parallel message processing of each kafka topic partition
- Independent scaling of kafka topic partition and consumer instances
- Retry queue and dead letter queue for error handling
- Flow control / throttling
- Head of line blocking detection and auto recovery (TODO: add link to more details)

RPC protocol supported

- GRPC

## Documentation

Enabling Seamless Kafka Async Queuing with Consumer Proxy ([Blog](https://www.uber.com/blog/kafka-async-queuing-with-consumer-proxy/))

## Table of Contents

- Build
- Contributing
- License
- Acknowledgments
- Contact

## Build

### Prerequisite

- Java 11 and above
- Docker 24.0 and above

### Steps

- Build binary

```
./gradlew jar
```

- Run unit test

```
./gradlew test
```

- Run integration test

```
./gradlew integrationTest
```

- Run code coverage verification

```
./gradlew check
```

## Usage

### Prerequisite

- Kafka 2.8 and above
- Zookeeper 3.4 and above

### Steps

- Create a new docker network

```
docker network create docker-network
```

- Start Zookeeper and Kafka broker

```
docker run --env ALLOW_ANONYMOUS_LOGIN=yes --network docker-network -p 2181:2181 --name zookeeper zookeeper:3.8.0
docker run --env KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 --env KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 --env KAFKA_LISTENERS=DOCKER://0.0.0.0:9092,HOSTER://0.0.0.0:9093 --env KAFKA_ADVERTISED_LISTENERS=DOCKER://kafka:9092,HOSTER://localhost:9093 -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=DOCKER:PLAINTEXT,HOSTER:PLAINTEXT -e KAFKA_INTER_BROKER_LISTENER_NAME=DOCKER --network docker-network -p 9093:9093 --name kafka confluentinc/cp-kafka:5.2.1
```

- Run Uforwarder controller and worker

```
docker run --env UFORWARDER_PROFILE=uforwarder-controller --env UFORWARDER_KAFKA_CONNECT=kafka:9092 --env UFORWARDER_ZOOKEEPER_CONNECT=zookeeper:2181/uforwarder --network docker-network --name controller -p 8087:8087 uforwarder:latest
docker run --env UFORWARDER_PROFILE=uforwarder-worker --env UFORWARDER_KAFKA_CONNECT=kafka:9092 --env UFORWARDER_CONTROLLER_CONNECT=controller:8087 --network docker-network --name worker uforwarder:latest
```

- Run Sample Consumer

```
./gradlew uforwarder-sample-consumer:bootRun
```

## Contributing

- Fork the project
- Create a new branch
- Make your changes and commit them
- Push to your fork and submit a pull request

## License

Apache License Version 2.0

## Acknowledgments

Give credits to any third party, organization or individuals that contributed to the project

## Contact

uforwarder-committers@googlegroups.com
