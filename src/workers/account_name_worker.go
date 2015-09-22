package workers

import (
	"database/sql"
	"fmt"
	"koding_challenge/src/configs"
	. "koding_challenge/src/helpers"
	"log"
	"time"

	_ "github.com/lib/pq"
)

type AccountNameWorker struct {
	Conn    *sql.DB
	Configs string
}

func NewAccountNameWorker() AccountNameWorker {
	return AccountNameWorker{
		nil,
		configs.LoadPostgresConfig(),
	}
}

func (worker *AccountNameWorker) Process(metric *MetricData, success chan bool, timeRecorder chan int64, debugMode *bool) {
	db, err := sql.Open("postgres", worker.Configs)
	if err != nil {
		log.Fatal(err, "can't connect to database")
	}
	defer db.Close()

	sqlQuery := fmt.Sprintf(
		`DO $$
			BEGIN
				IF NOT EXISTS (SELECT * FROM "accounts" WHERE "username"='%s' AND "metric"='%s') THEN
				INSERT INTO "accounts"(username, metric) VALUES ('%s', '%s');
				END IF;
			END
		$$;`,
		metric.Username,
		metric.Name,
		metric.Username,
		metric.Name,
	)

	_, err = db.Query(sqlQuery)
	if err != nil {
		LogError(err, "error while inserting account")
		success <- false
		return
	}

	if *debugMode {
		log.Println("AccountNameWorker finished the job")
	}
	timeRecorder <- time.Now().UTC().UnixNano()
	success <- true
}
