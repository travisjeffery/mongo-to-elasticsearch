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
	startIDStr = kingpin.Flag("start", "ID to start at.").Required().String()
	endIDStr   = kingpin.Flag("end", "ID to end at.").Required().String()
	mongoAddr  = kingpin.Flag("mongo", "Mongo address").Required().String()
	esAddr     = kingpin.Flag("elasticsearch", "Elasticsearch address").Required().String()
	collection = kingpin.Flag("collection", "Collection to move from mongo, the same name will be used for the elasticsearch type").Required().String()
)

func main() {
	kingpin.Parse()

	session, err := mgo.Dial(*mongoAddr)
	if err != nil {
		panic(err)
	}
	defer session.Close()

	es := elastigo.NewConn()
	es.Domain = *esAddr

	c := session.DB("").C(*collection)
	var v map[string]interface{}

	re := regexp.MustCompile("^_+")

	startID := bson.ObjectIdHex(*startIDStr)
	endID := bson.ObjectIdHex(*endIDStr)

	iter := c.Find(bson.M{"_id": bson.M{"$gte": startID, "$lte": endID}}).Sort("_id").Iter()
	for {
		for iter.Next(&v) {
			startID = v["_id"].(bson.ObjectId)
			project := v["_project"].(string)
			index := fmt.Sprintf("project_%s", project)

			// elasticsearch prefixes metadata with underscores so we remove them
			for k, val := range v {
				if re.Match([]byte(k)) == false {
					continue
				}
				delete(v, k)
				k = re.ReplaceAllString(k, "")
				v[k] = val
			}

			_, err := es.Index(index, *collection, startID.Hex(), nil, v)
			if err != nil {
				fmt.Printf("error indexing in elasticsearch: %s (id: %s)\n", err, startID.Hex())
				fmt.Println(err)
			}
		}
		if iter.Err() != nil {
			fmt.Printf("iteration error: %s (id: %s)\n", iter.Err(), startID.Hex())

			if err := iter.Close(); err != nil {
				fmt.Printf("error closing cursor: %s (id: %s)\n", err, startID.Hex())
			}
		}
		if iter.Timeout() {
			continue
		}
		iter = c.Find(bson.M{"_id": bson.M{"$gte": startID, "$lte": endID}}).Sort("_id").Iter()
	}
}
