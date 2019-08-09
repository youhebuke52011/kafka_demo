package main

import (
	"github.com/Shopify/sarama"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"
)

func main() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// Trap SIGINT to trigger a graceful shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	//ProduceBySelect(producer, signals)
	ProduceByGoroutines(producer, signals)
}

func ProduceBySelect(producer sarama.AsyncProducer, signals chan os.Signal) {
	var enqueued, errors int
	timer := time.NewTimer(2 * time.Second)
ProducerLoop:
	for {
		select {
		case producer.Input() <- &sarama.ProducerMessage{Topic: "my_topic", Key: nil, Value: sarama.StringEncoder(time.Now().Format("2006-01-02 15:04:05") + ": testing 123")}:
			enqueued++
		case err := <-producer.Errors():
			log.Println("Failed to produce message", err)
			errors++
		case <-signals:
			break ProducerLoop
		case <-timer.C:
			break ProducerLoop
		}
		time.Sleep(time.Second)
	}

	log.Printf("Enqueued: %d; errors: %d\n", enqueued, errors)
}

func ProduceByGoroutines(producer sarama.AsyncProducer, signals chan os.Signal) {

	var (
		wg                          sync.WaitGroup
		enqueued, successes, errors int
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for range producer.Successes() {
			successes++
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range producer.Errors() {
			log.Println(err)
			errors++
		}
	}()

	timer := time.NewTimer(2 * time.Second)

ProducerLoop:
	for {
		message := &sarama.ProducerMessage{Topic: "my_topic", Value: sarama.StringEncoder(time.Now().Format("2006-01-02 15:04:05") + ": testing 123")}
		select {
		case producer.Input() <- message:
			enqueued++

		case <-signals:
			producer.AsyncClose() // Trigger a shutdown of the producer.
			break ProducerLoop
		case <-timer.C:
			producer.AsyncClose() // Trigger a shutdown of the producer.
			break ProducerLoop
		}
		//time.Sleep(time.Second)
	}

	wg.Wait()

	log.Printf("Successfully produced: %d; errors: %d\n", successes, errors)
}
