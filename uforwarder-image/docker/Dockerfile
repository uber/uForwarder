FROM eclipse-temurin:11.0.17_8-jdk

COPY bin_main.jar /opt/uforwarder/
COPY entrypoint.sh /opt/uforwarder/
RUN chmod +x /opt/uforwarder/entrypoint.sh

ENV UFORWARDER_PROFILE uforwarder-controller
ENV UFORWARDER_CONTROLLER_CONNECT controller:8087
ENV UFORWARDER_KAFKA_CONNECT kafka:9092
ENV UFORWARDER_ZOOKEEPER_CONNECT zookeeper:2181/uforwarder
ENV UFORWARDER_MEMORY_LIMIT_MB 512
WORKDIR /opt/uforwarder

CMD [ "/opt/uforwarder/entrypoint.sh" ]
