package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	logger "github.com/ElrondNetwork/elrond-go-logger"
	"github.com/ElrondNetwork/elrond-tools-go/elasticquery/config"
	"github.com/ElrondNetwork/elrond-tools-go/elasticquery/elastic"
	"github.com/pelletier/go-toml"
	"github.com/urfave/cli"
)

const tomlFile = "./config.toml"

var (
	log = logger.GetOrCreate("main")
)

const helpTemplate = `NAME:
   {{.Name}} - {{.Usage}}
USAGE:
   {{.HelpName}} {{if .VisibleFlags}}[global options]{{end}}
   {{if len .Authors}}
AUTHOR:
   {{range .Authors}}{{ . }}{{end}}
   {{end}}{{if .Commands}}
GLOBAL OPTIONS:
   {{range .VisibleFlags}}{{.}}
   {{end}}
VERSION:
   {{.Version}}
   {{end}}
`

func main() {
	app := cli.NewApp()
	cli.AppHelpTemplate = helpTemplate
	app.Name = "Elasticsearch reindexing CLI App"
	app.Version = "v1.0.0"
	app.Usage = "This is the entry point for Elasticsearch scroll api query"
	app.Authors = []cli.Author{
		{
			Name:  "The Elrond Team",
			Email: "contact@elrond.com",
		},
	}

	app.Action = query

	err := app.Run(os.Args)
	if err != nil {
		os.Exit(1)
	}
}

func query(_ctx *cli.Context) error {
	cfg, err := loadConfig()
	if err != nil {
		log.Error("cannot load configuration", "error", err)
		return err
	}

	es, err := elastic.NewElasticClient(cfg.ESConfig)
	if err != nil {
		log.Error("cannot create es client", "error", err)
		return err
	}
	count := 0
	handlerFunc := func(responseBytes []byte) error {
		count++
		var esResponse elastic.GeneralElasticResponse
		err := json.Unmarshal(responseBytes, &esResponse)
		if err != nil {
			return err
		}

		resultsMap := elastic.ExtractSourceFromEsResponse(esResponse)
		log.Info("\tindexing", "bulk size", len(resultsMap), "count", count)
		for id, source := range resultsMap {
			log.Info(fmt.Sprintf(`{ "index" : { "_id" : "%s" }: %s }%s`, id, source, "\n"))
		}

		return nil
	}

	err = es.DoScrollRequestAllDocuments("accountsesdt", elastic.GetAll().Bytes(), handlerFunc)
	if err != nil {
		log.Error("%w while r.sourceElastic.DoScrollRequestAllDocuments", err)
		return err
	}
	return nil
}

func loadConfig() (*config.GeneralConfig, error) {
	tomlBytes, err := loadBytesFromFile(tomlFile)
	if err != nil {
		return nil, err
	}

	var tc config.GeneralConfig
	err = toml.Unmarshal(tomlBytes, &tc)
	if err != nil {
		return nil, err
	}

	return &tc, nil
}

func loadBytesFromFile(file string) ([]byte, error) {
	return ioutil.ReadFile(file)
}
