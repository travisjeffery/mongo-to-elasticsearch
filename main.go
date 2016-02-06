package main

import (
	"regexp"
	"time"

	elastigo "github.com/mattbaird/elastigo/lib"
	"github.com/segmentio/go-log"
	"gopkg.in/alecthomas/kingpin.v2"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var (
	mongoAddr   = kingpin.Flag("mongo", "Mongo address").Required().String()
	esAddr      = kingpin.Flag("elasticsearch", "Elasticsearch address").Required().String()
	collection  = kingpin.Flag("collection", "Collection to move from mongo, the same name will be used for the elasticsearch type").Required().String()
	concurrency = kingpin.Flag("concurrency", "Number of calls to make to es").Default("1000").Int()
	project     = kingpin.Flag("project", "The project to move").Default("54735a819b032df50644cc3e").String()
)

func main() {
	kingpin.Parse()

	session, err := mgo.Dial(*mongoAddr)
	if err != nil {
		panic(err)
	}
	defer session.Close()
	session.SetMode(mgo.SecondaryPreferred, false)
	session.SetBatch(200)
	session.SetPrefetch(0.75)
	session.SetSocketTimeout(1 * time.Hour)

	var val map[string]interface{}

	re := regexp.MustCompile("^_+")

	index := "project_" + *project
	ch := make(chan map[string]interface{}, *concurrency)

	go func() {
		es := elastigo.NewConn()
		es.Domain = *esAddr

		indexer := es.NewBulkIndexer(1000)
		indexer.Start()

		go func() {
			for {
				for err := range indexer.ErrorChannel {
					log.Error("index error: %s", err)
				}
			}
		}()

		for v := range ch {
			id := v["id"].(bson.ObjectId).Hex()
			err := indexer.Index(index, *collection, id, "", "", nil, v, false)
			if err != nil {
				log.Error("error indexing in elasticsearch: %s (id: %s)", err, id)
				ch <- v
			} else {
				log.Debug("indexed: %s", id)
			}
		}
	}()

	for {
		s := session.Copy()
		c := s.DB("").C(*collection)
		iter := c.Find(bson.M{"_project": *project}).Iter()

		for iter.Next(&val) {
			v := make(map[string]interface{})

			// elasticsearch prefixes metadata with underscores so we remove them
			for k, value := range val {
				k = re.ReplaceAllString(k, "")
				v[k] = value
			}

			ch <- v
		}
		if iter.Timeout() {
			s.Close()
			continue
		}
		if err := iter.Close(); err != nil {
			log.Error("error in iteration: %s", err)
		}
		s.Close()
	}
}
