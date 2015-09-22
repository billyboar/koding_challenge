package workers

import (
	"fmt"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	. "koding_challenge/src/helpers"
	"log"
	"time"
)

type HourlyLogWorker struct{}

func NewHourlyLogWorker() HourlyLogWorker {
	hourlyLogWorker := HourlyLogWorker{}
	conn := hourlyLogWorker.Connect()
	defer conn.Close()

	collection := conn.DB("koding_challenge").C("metrics")
	index := mgo.Index{
		Key:         []string{"name"},
		Unique:      false,
		DropDups:    false,
		Background:  true,
		Sparse:      true,
		ExpireAfter: time.Hour,
		Name:        "expire",
	}
	err := collection.EnsureIndex(index)
	if err != nil {
		fmt.Println("error file ensuring index", err)
	}
	return hourlyLogWorker
}

func (worker *HourlyLogWorker) Process(metric *MetricData, success chan bool, timeRecorder chan int64, debugMode *bool) {
	conn := worker.Connect()
	defer conn.Close()
	collection := conn.DB("koding_challenge").C("metrics")

	metric.CreatedAt = time.Now().UTC().UnixNano()
	change := mgo.Change{
		Update:    bson.M{"$inc": bson.M{"count": 1}},
		Upsert:    true,
		ReturnNew: true,
	}

	var result interface{}

	//updating counter
	_, err := collection.Find(bson.M{"username": metric.Username, "name": metric.Name}).Apply(change, &result)
	if err != nil {
		LogError(err, "error occured while updating mongodb")
		success <- false
		return
	}

	if *debugMode {
		log.Println("HourlyLogWorker finished the job")
	}
	timeRecorder <- time.Now().UTC().UnixNano()
	success <- true
}

func (worker *HourlyLogWorker) Connect() *mgo.Session {
	conn, err := mgo.Dial("localhost")
	if err != nil {
		LogError(err, "Error while connecting to mongo db")
	}
	return conn
}
