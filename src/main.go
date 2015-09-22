package main

import (
	"flag"
	"koding_challenge/src/http_server"
	"koding_challenge/src/workers"
)

var debug = flag.Bool("debug", false, "turn on to see more verbose results")

func main() {
	flag.Parse()
	forever := make(chan bool)
	go http_server.StartServer(debug)
	go workers.ListenTasks(debug)
	<-forever
}
