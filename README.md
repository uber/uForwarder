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

## Overfiew

For an overview of the project, please read the Uber Engineering blog [Enabling Seamless Kafka Async Queuing with Consumer Proxy](https://www.uber.com/blog/kafka-async-queuing-with-consumer-proxy/)

## Table of Contents

1. [Build](#build)
2. [Contributing](#contributing)
3. [License](#license)
4. [Acknowledgments](#acknowledgments)
5. [Contact](#contact)

## Build

### Prerequisite

- Java 11 and above
- Docker 24.0 and above

### Steps

1. Build binary

  ```
  ./gradlew jar
  ```

2. Run unit test

  ```
  ./gradlew test
  ```

3. Run integration test

  ```
  ./gradlew integrationTest
  ```

4. Run code coverage verification

  ```
  ./gradlew check
  ```

## Usage

### Prerequisite

- Kafka 2.8 and above
- Zookeeper 3.4 and above

### Steps

1. Create a new docker network

```
docker network create docker-network
```

2. Start Zookeeper and Kafka broker

```
docker run --env ALLOW_ANONYMOUS_LOGIN=yes --network docker-network -p 2181:2181 --name zookeeper zookeeper:3.8.0
docker run --env KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 --env KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 --env KAFKA_LISTENERS=DOCKER://0.0.0.0:9092,HOSTER://0.0.0.0:9093 --env KAFKA_ADVERTISED_LISTENERS=DOCKER://kafka:9092,HOSTER://localhost:9093 -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=DOCKER:PLAINTEXT,HOSTER:PLAINTEXT -e KAFKA_INTER_BROKER_LISTENER_NAME=DOCKER --network docker-network -p 9093:9093 --name kafka confluentinc/cp-kafka:5.2.1
```

3. Build uforwarer Docker image

```
cd uforwarder-image/build/docker
docker build -t uforwarder:latest .
```

4. Run Uforwarder controller and worker

```
docker run --env UFORWARDER_PROFILE=uforwarder-controller --env UFORWARDER_KAFKA_CONNECT=kafka:9092 --env UFORWARDER_ZOOKEEPER_CONNECT=zookeeper:2181/uforwarder --network docker-network --name controller -p 8087:8087 uforwarder:latest
docker run --env UFORWARDER_PROFILE=uforwarder-worker --env UFORWARDER_KAFKA_CONNECT=kafka:9092 --env UFORWARDER_CONTROLLER_CONNECT=controller:8087 --network docker-network --name worker uforwarder:latest
```

4. Run Sample Consumer

```
./gradlew uforwarder-sample-consumer:bootRun
```

## Contributing

We accept contributions through Pull Requests (PR's).  Please sign our [Contributing License Agreement (CLA)](cla-assistant.io) after you submit the PR, so that it can be accepted.

## License

Apache License Version 2.0

## Contact

uforwarder-committers@googlegroups.com
