package helpers

import (
	"bytes"
	"encoding/gob"
	"log"
	// "time"
)

type MetricData struct {
	Username  string
	Name      string
	Count     int64
	CreatedAt int64
}

//encodes Metrics struct into a byte array
func EncodeRequest(metric MetricData) []byte {
	var encoded bytes.Buffer
	enc := gob.NewEncoder(&encoded)
	err := enc.Encode(metric)
	if err != nil {
		LogError(err, "error occured while encoding")
		return nil
	}
	return encoded.Bytes()
}

func DecodeRequest(metricBytes []byte) *MetricData {
	decoded := bytes.NewBuffer(metricBytes)
	dec := gob.NewDecoder(decoded)
	var metric MetricData
	err := dec.Decode(&metric)
	if err != nil {
		LogError(err, "error occured while decoding")
		return nil
	}
	return &metric
}

func LogError(err error, detail string) {
	log.Printf("Error: %s - %s\n", err, detail)
}
