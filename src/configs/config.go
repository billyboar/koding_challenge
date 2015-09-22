package configs

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

var configPath = "/Users/bilegtbat/Tests/go/src/koding_challenge/src/configs/config.json"

func LoadConfigFile() map[string]interface{} {
	currentDir, err := os.Getwd()
	if err != nil {
		log.Fatal(err, " - error while getting current directory")
	}
	jsonFile, err := ioutil.ReadFile(currentDir + "/configs/config.json")
	if err != nil {
		log.Fatal(err, " - error while loading config file")
	}
	var data map[string]interface{}
	err = json.Unmarshal(jsonFile, &data)
	if err != nil {
		log.Fatal(err, " - error while unmarshaling json file")
	}
	return data
}

func LoadPostgresConfig() string {
	data := LoadConfigFile()
	databaseCfg := data["postgres"].(map[string]interface{})

	config := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable",
		databaseCfg["username"].(string),
		databaseCfg["password"].(string),
		databaseCfg["name"].(string),
	)

	return config
}

func LoadRedisConfig() map[string]string {
	databaseConfig := make(map[string]string)
	data := LoadConfigFile()
	databaseCfg := data["redis"].(map[string]interface{})
	databaseConfig["port"] = databaseCfg["port"].(string)
	return databaseConfig
}
