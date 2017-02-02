package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/philipjkim/kafka-brokers-go"
)

const (
	defaultKafkaTopic = "test_topic"
)

var (
	zkServers = flag.String("zk", os.Getenv("ZK_SERVERS"), "The comma-separated list of ZooKeeper servers. You can skip this flag by setting ZK_SERVERS environment variable")
	topic     = flag.String("topic", defaultKafkaTopic, "The topic to produce to")
	key       = flag.String("key", "", "The key of the message to produce. Can be empty.")
	silent    = flag.Bool("silent", false, "Turn off printing the message's topic, partition, and offset to stdout")

	logger = log.New(os.Stderr, "", log.LstdFlags)
)

func main() {
	flag.Parse()

	if *zkServers == "" {
		log.Fatalln("no -zk specified. Alternatively, set the ZK_SERVERS environment variable")
	}

	conn, err := kb.NewConn(strings.Split(*zkServers, ","))
	if err != nil {
		log.Fatalln(err)
	}
	defer conn.Close()
	brokerList, _, err := conn.GetW()
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Printf("brokerList: %q\n", brokerList)

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewManualPartitioner

	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			logger.Println("Failed to close Kafka producer cleanly:", err)
		}
	}()

	fmt.Println("Type a message and press Enter key to produce it. CTRL+C to exit.")

	for {
		message := &sarama.ProducerMessage{Topic: *topic, Partition: int32(0)}

		if *key != "" {
			message.Key = sarama.StringEncoder(*key)
		}

		in := bufio.NewReader(os.Stdin)
		value, err := in.ReadString('\n')
		if value == "" {
			value = "(empty message)"
		}
		message.Value = sarama.StringEncoder(value)

		partition, offset, err := producer.SendMessage(message)
		if err != nil {
			log.Fatalln(err)
		} else if !*silent {
			fmt.Printf("topic=%s\tpartition=%d\toffset=%d\n", *topic, partition, offset)
		}
	}
}

func stdinAvailable() bool {
	stat, _ := os.Stdin.Stat()
	return (stat.Mode() & os.ModeCharDevice) == 0
}
