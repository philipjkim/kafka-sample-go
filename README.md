kafka-sample-go
===============

Simple Kafka pub/sub example written in Go

## Starting Kafka

    docker-compose up -d


## Scaling Kafka Brokers

If you want to scale Kafka brokers to 4:

    docker-compose scale kafka=4


## Publishing Messages

    cd pub
    go run pub.go -topic=t1 -zk=192.168.99.100:2181


## Consuming Messages

    cd sub
    go run sub.go -topic=t1 -zk=192.168.99.100:2181


## References

- https://github.com/wurstmeister/kafka-docker
- https://github.com/Shopify/sarama
- https://github.com/wvanbergen/kazoo-go
