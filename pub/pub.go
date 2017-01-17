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
	"github.com/rcrowley/go-metrics"
)

var (
	zkServers   = flag.String("zk", os.Getenv("ZK_SERVERS"), "The comma-separated list of ZooKeeper servers. You can skip this flag by setting ZK_SERVERS environment variable")
	topic       = flag.String("topic", "", "REQUIRED: the topic to produce to")
	key         = flag.String("key", "", "The key of the message to produce. Can be empty.")
	partitioner = flag.String("partitioner", "", "The partitioning scheme to use. Can be `hash`, `manual`, or `random`")
	partition   = flag.Int("partition", -1, "The partition to produce to.")
	verbose     = flag.Bool("verbose", false, "Turn on sarama logging to stderr")
	showMetrics = flag.Bool("metrics", false, "Output metrics on successful publish to stderr")
	silent      = flag.Bool("silent", false, "Turn off printing the message's topic, partition, and offset to stdout")

	logger = log.New(os.Stderr, "", log.LstdFlags)
)

func main() {
	flag.Parse()

	if *zkServers == "" {
		printUsageErrorAndExit("no -zk specified. Alternatively, set the ZK_SERVERS environment variable")
	}

	conn, err := kb.NewConn(strings.Split(*zkServers, ","))
	if err != nil {
		printErrorAndExit(69, "Failed to create connection to zk: %s", err)
	}
	defer conn.Close()
	brokerList, _, err := conn.GetW()
	if err != nil {
		printErrorAndExit(69, "Failed to get broker list from zk: %s", err)
	}
	fmt.Printf("brokerList: %q\n", brokerList)

	if *topic == "" {
		printUsageErrorAndExit("no -topic specified")
	}

	if *verbose {
		sarama.Logger = logger
	}

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	switch *partitioner {
	case "":
		if *partition >= 0 {
			config.Producer.Partitioner = sarama.NewManualPartitioner
		} else {
			config.Producer.Partitioner = sarama.NewHashPartitioner
		}
	case "hash":
		config.Producer.Partitioner = sarama.NewHashPartitioner
	case "random":
		config.Producer.Partitioner = sarama.NewRandomPartitioner
	case "manual":
		config.Producer.Partitioner = sarama.NewManualPartitioner
		if *partition == -1 {
			printUsageErrorAndExit("-partition is required when partitioning manually")
		}
	default:
		printUsageErrorAndExit(fmt.Sprintf("Partitioner %s not supported.", *partitioner))
	}

	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		printErrorAndExit(69, "Failed to open Kafka producer: %s", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			logger.Println("Failed to close Kafka producer cleanly:", err)
		}
	}()

	fmt.Println("Type a message and press Enter key to produce it. CTRL+C to exit.")

	for {
		message := &sarama.ProducerMessage{Topic: *topic, Partition: int32(*partition)}

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
			printErrorAndExit(69, "Failed to produce message: %s", err)
		} else if !*silent {
			fmt.Printf("topic=%s\tpartition=%d\toffset=%d\n", *topic, partition, offset)
		}
		if *showMetrics {
			metrics.WriteOnce(config.MetricRegistry, os.Stderr)
		}
	}
}

func printErrorAndExit(code int, format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	os.Exit(code)
}

func printUsageErrorAndExit(message string) {
	fmt.Fprintln(os.Stderr, "ERROR:", message)
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Available command line options:")
	flag.PrintDefaults()
	os.Exit(64)
}

func stdinAvailable() bool {
	stat, _ := os.Stdin.Stat()
	return (stat.Mode() & os.ModeCharDevice) == 0
}
