package main

import (
	"fmt"
	"regexp"

	elastigo "github.com/mattbaird/elastigo/lib"
	"gopkg.in/alecthomas/kingpin.v2"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var (
	mongoAddr   = kingpin.Flag("mongo", "Mongo address").Required().String()
	esAddr      = kingpin.Flag("elasticsearch", "Elasticsearch address").Required().String()
	collection  = kingpin.Flag("collection", "Collection to move from mongo, the same name will be used for the elasticsearch type").Required().String()
	concurrency = kingpin.Flag("concurrency", "Number of calls to make to es").Default("1000").Int()
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

	es := elastigo.NewConn()
	es.Domain = *esAddr

	var val map[string]interface{}

	re := regexp.MustCompile("^_+")

	project := "54735a819b032df50644cc3e"
	index := "project_" + project
	ch := make(chan map[string]interface{}, *concurrency)

	go func() {
		for val := range ch {
			go func(index, collection string, v map[string]interface{}) {
				id := v["id"].(bson.ObjectId)

				_, err := es.Index(index, collection, id.Hex(), nil, v)
				if err != nil {
					fmt.Printf("error indexing in elasticsearch: %s (id: %s)\n", err, id.Hex())
					ch <- v
				} else {
					fmt.Printf("success indexing in elasticsearch: %s\n", id.Hex())
				}
			}(index, *collection, val)
		}
	}()

	for {
		s := session.Copy()
		c := s.DB("").C(*collection)
		iter := c.Find(bson.M{"_project": project}).Iter()

		fmt.Printf("in outer loop\n")
		for iter.Next(&val) {
			v := make(map[string]interface{})

			// elasticsearch prefixes metadata with underscores so we remove them
			for k, value := range val {
				if re.Match([]byte(k)) == false {
					continue
				}
				delete(v, k)
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
			fmt.Printf("error in iteration: %s\n", err)
		}
		s.Close()
	}
}
