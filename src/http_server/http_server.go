package http_server

import (
	"fmt"
	"github.com/julienschmidt/httprouter"
	"github.com/streadway/amqp"
	. "koding_challenge/src/helpers"
	"log"
	"net/http"
	"strings"
	"time"
)

//if request can't be transfered to the job queue
//retries after following number of seconds
const RetrySpan = 4

var debug *bool

func StartServer(debugOption *bool) {
	log.Println("Server started at :8080")
	debug = debugOption
	router := httprouter.New()
	router.GET("/metrics/:username/:metric", MetricsHandler)
	http.ListenAndServe(":8080", router)
}

func MetricsHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	//receives request from user and pass it to RabbitMQ
	username := EscapeQuotes(ps.ByName("username"))
	metricName := EscapeQuotes(ps.ByName("metric"))
	receivedTime := time.Now().UTC().UnixNano()

	if *debug {
		fmt.Print("\n")
		log.Println("New Request - user:", username, "metric:", metricName)
	}

	finished := false
	for {
		finished = SendRequestToQueue(username, metricName, receivedTime)
		if finished {
			break
		}
		time.Sleep(time.Second * RetrySpan)
	}
	if *debug {
		log.Println("Sent to the RabbitMQ server")
	}
}

//SendRequestToQueue sends given username and metric to RabbitMQ server
func SendRequestToQueue(username string, metricName string, receivedTime int64) bool {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		LogError(err, "Failed to connect to RabbitMQ server")
		return false
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		LogError(err, "Failed to open a channel")
		return false
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
		return false
	}

	encodedValue := EncodeRequest(MetricData{
		username,
		metricName,
		0,
		receivedTime, //unixtime in nanoseconds
	})

	err = ch.Publish("", q.Name, false, false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        encodedValue,
		})
	if err != nil {
		LogError(err, "error while publishing")
	}
	return true
}

func EscapeQuotes(text string) string {
	return strings.Replace(text, "'", "''", -1)
}
