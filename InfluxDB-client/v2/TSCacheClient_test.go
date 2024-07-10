package client

import (
	"fmt"
	"strings"
	"testing"
)

func TestTSCacheClient(t *testing.T) {
	querySet := `SELECT mean(latitude),mean(longitude),mean(elevation),mean(grade),mean(heading),mean(velocity) FROM "readings" WHERE ("name"='truck_0') AND TIME >= '2022-01-01T01:00:00Z' AND TIME < '2022-01-01T01:10:00Z' group by time(1m)`
	queryGet := `SELECT mean(latitude),mean(longitude),mean(elevation),mean(grade),mean(heading),mean(velocity) FROM "readings" WHERE ("name"='truck_0') AND TIME >= '2022-01-01T01:00:00Z' AND TIME < '2022-01-01T01:15:00Z' group by time(1m)`

	cacheUrlString := "192.168.1.102:11211"
	urlArr := strings.Split(cacheUrlString, ",")
	conns := InitStsConnsArr(urlArr)
	DB = "iot_medium"
	fmt.Printf("number of conns:%d\n", len(conns))
	TagKV = GetTagKV(c, "iot_medium")
	Fields = GetFieldKeys(c, "iot_medium")
	STsConnArr = InitStsConnsArr(urlArr)
	var dbConn, _ = NewHTTPClient(HTTPConfig{
		Addr: "http://192.168.1.103:8086",
	})

	respSet, _, _ := TSCacheClient(dbConn, querySet)

	query1 := NewQuery(querySet, "iot_medium", "s")
	resp1, err := dbConn.Query(query1)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("\tdatabase resp1:\n", resp1.ToString())
	}
	fmt.Println("\tresp set:")
	fmt.Println(respSet.ToString())

	respGet, _, _ := TSCacheClient(dbConn, queryGet)

	query2 := NewQuery(queryGet, "iot_medium", "s")
	resp2, err := dbConn.Query(query2)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("\tdatabase resp2:\n", resp2.ToString())
	}
	fmt.Println("\tresp get:")
	fmt.Println(respGet.ToString())

}
