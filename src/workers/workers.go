package workers

import (
	"fmt"
	"github.com/streadway/amqp"
	. "koding_challenge/src/helpers"
	"log"
	"time"
)

type AsyncWorker interface {
	Process(*MetricData, chan bool, chan int64, *bool)
}

var (
	distinctNameWorker = NewDistinctNameWorker()
	hourlyLogWorker    = NewHourlyLogWorker()
	accountNameWorker  = NewAccountNameWorker()
)

const (
	MonthlyJobWatcherDelay = 24
	WorkerProcessRetryTime = 5
)

func ListenTasks(debugMode *bool) {
	log.Println("listening jobs.")
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		LogError(err, "Failed to connect to RabbitMQ server")
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		LogError(err, "Failed to open a channel")
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"koding_challenge", //task_name
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		LogError(err, "Failed to declare a queue")
	}

	err = ch.Qos(
		1,
		0,
		false,
	)
	if err != nil {
		LogError(err, "Failed to set Qos")
	}
	tasks, err := ch.Consume(
		q.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		LogError(err, "Failed to register a consumer")
	}

	forever := make(chan bool)
	go func() {
		for task := range tasks {
			log.Println("Received a task!")
			task.Ack(false)
			metric := DecodeRequest(task.Body)

			go func() {
				distinctNameDone := make(chan bool)
				hourlyLogDone := make(chan bool)
				accountNameDone := make(chan bool)
				elapsedTimes := make(chan int64, 3)

				go distinctNameWorker.Process(metric, distinctNameDone, elapsedTimes, debugMode)
				go hourlyLogWorker.Process(metric, hourlyLogDone, elapsedTimes, debugMode)
				go accountNameWorker.Process(metric, accountNameDone, elapsedTimes, debugMode)

				for {
					select {
					case distinctNameDoneValue, ok := <-distinctNameDone:
						if !distinctNameDoneValue && ok {
							RetryProcess(&distinctNameWorker, metric, elapsedTimes, debugMode)
						}

					case hourlyLogDoneValue, ok := <-hourlyLogDone:
						if !hourlyLogDoneValue && ok {
							RetryProcess(&hourlyLogWorker, metric, elapsedTimes, debugMode)
						}

					case accountNameDoneValue, ok := <-accountNameDone:
						if !accountNameDoneValue && ok {
							RetryProcess(&accountNameWorker, metric, elapsedTimes, debugMode)
						}

					default:
						if len(elapsedTimes) == 3 {
							fmt.Println(
								"Time Spent:",
								FindElapsedTime(FindSlowestProcess(elapsedTimes), metric.CreatedAt),
								"ms",
							)
							return
						}
					}

				}
			}()
		}
	}()

	//this goroutine moves metrics that are older than 30 days
	//and moves them to monthly buckets
	go func() {
		for {
			distinctNameWorker.MonthlyCronJob()
			//works once in a day
			time.Sleep(MonthlyJobWatcherDelay * time.Hour)
		}
	}()
	<-forever
}

func RetryProcess(worker AsyncWorker, metric *MetricData, elapsedTime chan int64, debugMode *bool) {
	for {
		didSuccess := make(chan bool)
		go func() {
			worker.Process(metric, didSuccess, elapsedTime, debugMode)
		}()

		if <-didSuccess {
			return
		}

		time.Sleep(WorkerProcessRetryTime * time.Second)
	}
}

//finds the latest record from given sets of time records
func FindSlowestProcess(timeRecords chan int64) int64 {
	max := <-timeRecords
	length := len(timeRecords)
	for i := 0; i < length; i++ {
		currentRecord := <-timeRecords
		if max < currentRecord {
			max = currentRecord
		}
	}
	return max
}

func FindElapsedTime(endTime int64, startTime int64) int64 {
	//converting nanoseconds to milliseconds
	return (endTime - startTime) / 1000000
}
