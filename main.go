package main

import (
	"flag"
	log "github.com/sirupsen/logrus"
)

type Options struct {
	mongodbURI string
	help       bool
	namespace  string
	threads    int
}

func main() {

	log.SetFormatter(&log.JSONFormatter{})

	standardFields := log.Fields{
		"appname": "mongo-perf-inspect",
		"thread":  "thread-1",
	}

	mongodbURI := flag.String("mongodbURI", "mongodb://localhost:27017", "MongoDB connection details (default 'mongodb://localhost:27017' )")
	help := flag.Bool("help", false, "Show Help")
	namespace := flag.String("namespace", "sample_mflix.movies", "Namespace to use , for example myDatabase.myCollection")
	threads := flag.Int("threads", 1, "Number of threads (default 1)")

	flag.Parse()

	cmdOptions := Options{
		*mongodbURI,
		*help,
		*namespace,
		*threads,
	}

	log.WithFields(standardFields).Info(cmdOptions)

}
