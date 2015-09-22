package workers

import (
	"github.com/garyburd/redigo/redis"
	"koding_challenge/src/configs"
	. "koding_challenge/src/helpers"
	"log"
	"strconv"
	"time"
)

type DistinctNameWorker struct {
	ConnPool *redis.Pool
}

const (
	SecondsForDay = 86400
)

func NewDistinctNameWorker() DistinctNameWorker {
	redisCfg := configs.LoadRedisConfig()
	return DistinctNameWorker{
		&redis.Pool{
			MaxIdle:   100,
			MaxActive: 100000,
			Dial: func() (redis.Conn, error) {
				c, err := redis.Dial("tcp", ":"+redisCfg["port"])
				if err != nil {
					log.Fatal(err.Error())
				}
				return c, err
			},
		},
	}
}

func (worker *DistinctNameWorker) Process(metric *MetricData, success chan bool, timeRecorder chan int64, debugMode *bool) {
	client := worker.ConnPool.Get()
	defer client.Close()

	currentTime := time.Now().UTC().Unix()

	// finds how many days past since epoch time
	// SecondsForDay = 24 * 60 * 60
	currentDay := currentTime / SecondsForDay
	_, err := client.Do("HINCRBY", currentDay, metric.Name, 1)
	if err != nil {
		LogError(err, "can't increase count in redis")
		success <- false
		return
	}

	//adding today's id to current_month set
	_, err = client.Do("SADD", "current_month", currentDay)
	if err != nil {
		LogError(err, "can't add to redis (current_month)")
		success <- false
		return
	}

	if *debugMode {
		log.Println("DinstinctNameWorker finished the job")
	}
	timeRecorder <- time.Now().UTC().UnixNano()
	success <- true
}

//this function moves individual days' results that are older than 30 days to monthly hash
func (worker *DistinctNameWorker) MonthlyCronJob() {
	currentDay := time.Now().UTC().Unix() / SecondsForDay
	client := worker.ConnPool.Get()
	defer client.Close()

	reply, err := client.Do("SMEMBERS", "current_month")
	results, err := redis.Values(reply, err)
	if err != nil {
		log.Fatal(err)
	}
	for _, result := range results {
		resultDay, err := strconv.ParseInt(string(result.([]uint8)), 10, 32)
		if err != nil {
			LogError(err, "can't parse results from redis")
		}
		if currentDay-resultDay > 30 {
			//moving this day's results to monthly bucket
			monthForResultDay := time.Unix(resultDay*SecondsForDay, 0).Month().String()
			yearForResultDay := time.Unix(resultDay*SecondsForDay, 0).Year()

			//adding this day's results to MONTH+YEAR named hash
			client.Send("SADD", monthForResultDay+"-"+string(yearForResultDay), resultDay)
			//removing from current_month set
			client.Send("SREM", "current_month", resultDay)
			client.Flush()
			client.Receive()
			_, err := client.Receive()
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	log.Println("Monthly job watcher finished job")
}
