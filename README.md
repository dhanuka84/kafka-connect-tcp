# Kafka Connect Socket Source
The connector is used to load data to Kafka from a Socket.

# Building
You can build the connector with Maven using the standard lifecycle phases:
```
mvn clean
mvn package
```

# Sample Configuration
``` ini
name=socket-connector
connector.class=org.apache.kafka.connect.socket.SocketSourceConnector
tasks.max=1
topic=topic
schema.name=socketschema
port=12345
batch.size=100
```
