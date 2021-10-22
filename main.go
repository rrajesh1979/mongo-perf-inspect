package main

import (
	"context"
	cryptoRand "crypto/rand"
	"flag"
	"github.com/bxcodec/faker/v3"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var standardFields = log.Fields{
	"appname": "mongo-perf-inspect",
	"thread":  "thread-1",
}

func init() {
	// Log as JSON instead of the default ASCII formatter.
	log.SetFormatter(&log.JSONFormatter{
		PrettyPrint: false,
	})

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	log.SetOutput(os.Stdout)

	// Only log the warning severity or above.
	log.SetLevel(log.ErrorLevel)
}

func getMongoConnection(uri string) (*mongo.Client, func(), error) {
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(uri))
	return client, func() {
		if err := client.Disconnect(context.TODO()); err != nil {
			panic(err)
		}
	}, err
}

func main() {

	log.WithFields(standardFields).Info("MongoDB performance inspector tool v0.01 developed in golang !")

	cmdOptions := getOptions()
	log.WithFields(standardFields).Info(cmdOptions)

	if cmdOptions.help {
		flag.Usage()
	}

	client, closeConnection, err := getMongoConnection(cmdOptions.mongodbURI)
	if err != nil {
		panic(err)
	} else {
		defer closeConnection()
	}

	var duration = time.Duration(cmdOptions.duration) * time.Second
	var endTime = time.Now().Add(duration)

	startLoadProcess(cmdOptions, client, endTime)

}

func startLoadProcess(cmdOptions Options, client *mongo.Client, endTime time.Time) {
	parallelization := cmdOptions.workers
	c := make(chan string)
	var wg sync.WaitGroup
	wg.Add(parallelization)

	for i := 0; i < parallelization; i++ {
		go func(c chan string) {
			for {
				workerId, more := <-c
				if !more {
					wg.Done()
					return
				}
				insertDoc(cmdOptions, client, endTime, workerId)
			}
		}(c)
	}

	for i := 0; i < parallelization; i++ {
		c <- strings.Join([]string{"worker-", strconv.Itoa(i)}, "")
	}

	close(c)
	wg.Wait()
}

func insertDoc(cmdOptions Options, client *mongo.Client, endTime time.Time, workerId string) {
	for {
		if time.Now().After(endTime) {
			return
		}

		newDoc := createTestDoc(cmdOptions.numFields, cmdOptions.depth, cmdOptions.binary, workerId)
		collection := client.Database(cmdOptions.dbName).Collection(cmdOptions.collName)
		_, err := collection.InsertOne(context.TODO(), newDoc)
		if err != nil {
			panic(err)
		}

	}
}

type Options struct {
	mongodbURI      string
	help            bool
	namespace       string
	threads         int
	workers         int
	insertOps       int
	queryOps        int
	updateOps       int
	deleteOps       int
	emptyCollection bool
	duration        int
	numFields       int
	depth           int
	logfile         string
	printDoc        bool
	threadIdStart   int
	updateFields    int
	projectFields   int
	batchSize       int
	binary          int
	dbName          string
	collName        string
}

func getOptions() Options {
	mongodbURI := flag.String("mongodbURI", "mongodb://localhost:27017", "MongoDB connection details")
	help := flag.Bool("help", false, "Show Help")
	namespace := flag.String("namespace", "sample_mflix.movies", "Namespace to use for example myDatabase.myCollection")
	workers := flag.Int("workers", 1, "Number of workers")
	threads := flag.Int("threads", 1, "Number of threads")

	insertOps := flag.Int("i", 100, "Ratio of insert operations")
	updateOps := flag.Int("u", 0, "Ratio of update operations")
	queryOps := flag.Int("q", 0, "Ratio of query operations")
	deleteOps := flag.Int("d", 0, "Ratio of delete operations")

	emptyCollection := flag.Bool("empty", false, "Remove data from collection on startup")

	duration := flag.Int("duration", 180, "Test duration in seconds")
	numfields := flag.Int("numFields", 10, "Number of top level fields in test documents")
	depth := flag.Int("depth", 0, "The depth of the document created")

	logfile := flag.String("logfile", "mongo-perf-inspect.log", "Output stats to <file>")
	printDoc := flag.Bool("printDoc", false, "Print out a sample document according to the other parameters then quit")

	threadIdStart := flag.Int("threadIdStart", 0, "Start 'workerId' for each thread. 'w' value in _id.")
	updatefields := flag.Int("updateFields", 1, "Number of fields to update.")
	projectfields := flag.Int("projectFields", 1, "Number of fields to project in finds (default 0, which is no projection)")

	batchSize := flag.Int("batchSize", 512, "Bulk op batch size")
	binary := flag.Int("binary", 0, "Add a binary blob of size KB")

	flag.Parse()

	namespaceParts := strings.Split(*namespace, ".")
	if len(namespaceParts) != 2 {
		log.Info("Provide valid namespace to use for example myDatabase.myCollection")
		flag.Usage()
		os.Exit(1)
	}
	dbName := namespaceParts[0]
	collName := namespaceParts[1]

	//Validate command line options
	cmdOptions := Options{
		*mongodbURI,
		*help,
		*namespace,
		*threads,
		*workers,
		*insertOps,
		*queryOps,
		*updateOps,
		*deleteOps,
		*emptyCollection,
		*duration,
		*numfields,
		*depth,
		*logfile,
		*printDoc,
		*threadIdStart,
		*updatefields,
		*projectfields,
		*batchSize,
		*binary,
		dbName,
		collName,
	}

	return cmdOptions
}

func GenRandomBytes(size int) (blk []byte, err error) {
	blk = make([]byte, size)
	_, err = cryptoRand.Read(blk)
	return
}

func createTestDoc(numFields int, depth int, binarySize int, threadId string) bson.M {
	idVal := primitive.NewObjectID()
	newDoc := bson.M{"_id": idVal}

	for i := 0; i < numFields; i++ {

		switch i {
		case 0:
			newDoc[strings.Join([]string{"i", strconv.Itoa(i)}, "")] = time.Now().UnixNano()
		case 1:
			timestamp, _ := time.Parse(time.RFC3339, faker.Timestamp())
			newDoc[strings.Join([]string{"i", strconv.Itoa(i)}, "")] = timestamp
		case 2:
			newDoc[strings.Join([]string{"i", strconv.Itoa(i)}, "")] = rand.Intn(1000)
		case 3:
			newDoc[strings.Join([]string{"i", strconv.Itoa(i)}, "")] = rand.Float64()
		case 4:
			newDoc[strings.Join([]string{"i", strconv.Itoa(i)}, "")] = faker.Name()
		case 5:
			newDoc[strings.Join([]string{"i", strconv.Itoa(i)}, "")] = faker.Email()
		default:
			newDoc[strings.Join([]string{"i", strconv.Itoa(i)}, "")] = faker.Word()
		}

		if binarySize != 0 {
			binData, _ := GenRandomBytes(32)
			newDoc[strings.Join([]string{"i", strconv.Itoa(numFields + 1)}, "")] = binData
		}
		newDoc[strings.Join([]string{"i", strconv.Itoa(numFields + 2)}, "")] = threadId
	}

	log.Info(newDoc)

	return newDoc
}
