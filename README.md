# Golang Kafka Example

### Setup

1. Run `docker-compose up -d` in the root directory. This will start Apache Kafka and Zookeeper

2. Run `go run producer/producer.go` to start the producer which is a REST API listening on port 3000

3. Run `go run worker/worker.go` to start the consumer

### Test it out!

Send a POST request to `localhost:3000`

This will produce a message in Apache Kafka using Sarama by IBM and the consumer will process it.

```bash
curl --location --request POST '0.0.0.0:3000/api/v1/comments' \
--header 'Content-Type: application/json' \
--data-raw '{ "text":"message 1" }'

curl --location --request POST '0.0.0.0:3000/api/v1/comments' \
--header 'Content-Type: application/json' \
--data-raw '{ "text":"message 2" }'
```