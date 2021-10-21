package main

import (
	"flag"
	log "github.com/sirupsen/logrus"
	"os"
)

type Options struct {
	mongodbURI string
	help       bool
	namespace  string
	threads    int
}

var standardFields = log.Fields{
	"appname": "mongo-perf-inspect",
	"thread":  "thread-1",
}

func init() {
	// Log as JSON instead of the default ASCII formatter.
	log.SetFormatter(&log.JSONFormatter{})

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	log.SetOutput(os.Stdout)

	// Only log the warning severity or above.
	log.SetLevel(log.InfoLevel)

}

func getOptions() Options {
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

	return cmdOptions
}

func main() {

	cmdOptions := getOptions()
	log.WithFields(standardFields).Info(cmdOptions)

	if cmdOptions.help == true {

	}

}
