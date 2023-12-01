## Running in docker

Build UForwarder docker image

```
chmod +x ./build.sh && ./build.sh
```

- Create docker network and prepare dependencies

```
docker network create docker-network
docker run --env ALLOW_ANONYMOUS_LOGIN=yes --network docker-network -p 2181:2181 --name zookeeper zookeeper:3.8.0
docker run --env KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 --env KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 --env KAFKA_LISTENERS=DOCKER://0.0.0.0:9092,HOSTER://0.0.0.0:9093 --env KAFKA_ADVERTISED_LISTENERS=DOCKER://kafka:9092,HOSTER://localhost:9093 -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=DOCKER:PLAINTEXT,HOSTER:PLAINTEXT -e KAFKA_INTER_BROKER_LISTENER_NAME=DOCKER --network docker-network -p 9093:9093 --name kafka confluentinc/cp-kafka:5.2.1
```

- Run uForwarder Controller

```
docker run --env UFORWARDER_PROFILE=uforwarder-controller --env UFORWARDER_KAFKA_CONNECT=kafka:9092 --env UFORWARDER_ZOOKEEPER_CONNECT=zookeeper:2181/uforwarder --network docker-network --name controller uforwarder:latest
```

- Run uForwarder Worker

```
docker run --env UFORWARDER_PROFILE=uforwarder-worker --env UFORWARDER_KAFKA_CONNECT=kafka:9092 --env UFORWARDER_CONTROLLER_CONNECT=controller:8087 --network docker-network --name worker uforwarder:latest
```
