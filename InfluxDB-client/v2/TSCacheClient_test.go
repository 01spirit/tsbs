package client

import (
	"fmt"
	"strings"
	"testing"
)

func TestTSCacheClient(t *testing.T) {
	querySet := `SELECT mean(velocity),mean(fuel_consumption) FROM "readings" WHERE ("name"='truck_1' or "name"='truck_2') AND TIME >= '2022-01-01T01:00:00Z' AND TIME < '2022-01-01T02:00:00Z' GROUP BY "name",time(10m)`

	queryGet := `SELECT mean(velocity),mean(fuel_consumption) FROM "readings" WHERE ("name"='truck_1' or "name"='truck_2') AND TIME >= '2022-01-01T01:00:00Z' AND TIME < '2022-01-01T02:00:00Z' GROUP BY "name",time(10m)`

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

	//query1 := NewQuery(querySet, "iot_medium", "s")
	//resp1, err := dbConn.Query(query1)
	//if err != nil {
	//	fmt.Println(err)
	//} else {
	//	//fmt.Println("\tresp1:\n", resp1.ToString())
	//}
	////values1 := ResponseToByteArray(resp1, querySet)
	//numOfTab1 := GetNumOfTable(resp1)
	//
	//partialSegment := ""
	//fields := ""
	//metric := ""
	//_, startTime1, endTime1, tags1 := GetQueryTemplate(querySet)
	//partialSegment, fields, metric = GetPartialSegmentAndFields(querySet)
	//fields = "time[int64]," + fields
	//datatypes1 := GetDataTypeArrayFromSF(fields)
	//
	//starSegment := GetStarSegment(metric, partialSegment)
	//
	//values1 := ResponseToByteArrayWithParams(resp1, datatypes1, tags1, metric, partialSegment)
	//err = STsConnArr[0].Set(&stscache.Item{Key: starSegment, Value: values1, Time_start: startTime1, Time_end: endTime1, NumOfTables: numOfTab1})
	//
	//_, startTimeGet, endTimeGet, tagsGet := GetQueryTemplate(queryGet)
	//partialSegment, fields, metric = GetPartialSegmentAndFields(queryGet)
	//fields = "time[int64]," + fields
	//datatypesGet := GetDataTypeArrayFromSF(fields)
	//semanticSegment := GetTotalSegment(metric, tagsGet, partialSegment)
	//
	//valuesGet, _, err := STsConnArr[0].Get(semanticSegment, startTimeGet, endTimeGet)
	//respGet, _, _, _, _ := ByteArrayToResponseWithDatatype(valuesGet, datatypesGet)
	//fmt.Println("\tresp from cache:\n", respGet.ToString())
	//
	//queryG := NewQuery(queryGet, "iot_medium", "s")
	//respG, err := dbConn.Query(queryG)
	//if err != nil {
	//	fmt.Println(err)
	//} else {
	//	fmt.Println("\tresp from database:\n", respG.ToString())
	//}
}
