package client

import (
	"fmt"
	stscache "github.com/timescale/tsbs/InfluxDB-client/memcache"
	"strings"
	"testing"
)

func TestOperator(t *testing.T) {
	querySetBig := `SELECT mean(velocity),mean(fuel_consumption),mean(grade) FROM "readings" WHERE ("name"='truck_1' or "name"='truck_2') AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T02:00:00Z' GROUP BY "name",time(10m)`
	querySetSmall := `SELECT mean(velocity),mean(fuel_consumption) FROM "readings" WHERE ("name"='truck_1' or "name"='truck_2') AND TIME >= '2022-01-01T01:00:00Z' AND TIME < '2022-01-01T01:10:00Z' GROUP BY "name",time(10m)`

	queryGet := `SELECT mean(velocity),mean(fuel_consumption) FROM "readings" WHERE ("name"='truck_1' or "name"='truck_2') AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T01:30:00Z' GROUP BY "name",time(10m)`

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

	query1 := NewQuery(querySetBig, "iot_medium", "s")
	resp1, err := dbConn.Query(query1)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("\tresp1:\n", resp1.ToString())
	}
	//values1 := ResponseToByteArray(resp1, querySetBig)
	numOfTab1 := GetNumOfTable(resp1)

	partialSegment := ""
	fields := ""
	metric := ""
	_, startTime1, endTime1, tags1 := GetQueryTemplate(querySetBig)
	partialSegment, fields, metric = GetPartialSegmentAndFields(querySetBig)
	fields = "time[int64]," + fields
	datatypes1 := GetDataTypeArrayFromSF(fields)

	starSegment := GetStarSegment(metric, partialSegment)

	values1 := ResponseToByteArrayWithParams(resp1, datatypes1, tags1, metric, partialSegment)
	err = STsConnArr[0].Set(&stscache.Item{Key: starSegment, Value: values1, Time_start: startTime1, Time_end: endTime1, NumOfTables: numOfTab1})

	query3 := NewQuery(querySetSmall, "iot_medium", "s")
	resp3, err := dbConn.Query(query3)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("\tresp3:\n", resp3.ToString())
	}
	//values3 := ResponseToByteArray(resp3, querySetSmall)
	numOfTab3 := GetNumOfTable(resp3)

	_, startTime3, endTime3, tags3 := GetQueryTemplate(querySetSmall)
	partialSegment, fields, metric = GetPartialSegmentAndFields(querySetSmall)
	fields = "time[int64]," + fields
	datatypes3 := GetDataTypeArrayFromSF(fields)

	starSegment = GetStarSegment(metric, partialSegment)
	values3 := ResponseToByteArrayWithParams(resp3, datatypes3, tags3, metric, partialSegment)
	err = STsConnArr[0].Set(&stscache.Item{Key: starSegment, Value: values3, Time_start: startTime3, Time_end: endTime3, NumOfTables: numOfTab3})

	_, startTimeGet, endTimeGet, tagsGet := GetQueryTemplate(queryGet)
	partialSegment, fields, metric = GetPartialSegmentAndFields(queryGet)
	fields = "time[int64]," + fields
	datatypesGet := GetDataTypeArrayFromSF(fields)
	semanticSegment := GetTotalSegment(metric, tagsGet, partialSegment)
	//respGet, _, _ := STsCacheClient(dbConn, queryGet)
	valuesGet, _, err := STsConnArr[0].Get(semanticSegment, startTimeGet, endTimeGet)
	respGet, _, _, _, _ := ByteArrayToResponseWithDatatype(valuesGet, datatypesGet)
	fmt.Println("\tresp from cache:\n", respGet.ToString())

	queryG := NewQuery(queryGet, "iot_medium", "s")
	respG, err := dbConn.Query(queryG)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("\tresp from database:\n", respG.ToString())
	}

}

func TestOperatorMidMiss(t *testing.T) {
	querySetBig := `SELECT mean(velocity),mean(fuel_consumption),mean(grade) FROM "readings" WHERE ("name"='truck_1' or "name"='truck_2') AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T02:00:00Z' GROUP BY "name",time(10m)`
	querySetSmall1 := `SELECT mean(velocity),mean(fuel_consumption) FROM "readings" WHERE ("name"='truck_1' or "name"='truck_2') AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T00:30:00Z' GROUP BY "name",time(10m)`
	querySetSmall2 := `SELECT mean(velocity),mean(fuel_consumption) FROM "readings" WHERE ("name"='truck_1' or "name"='truck_2') AND TIME >= '2022-01-01T01:00:00Z' AND TIME < '2022-01-01T01:30:00Z' GROUP BY "name",time(10m)`

	queryGet := `SELECT mean(velocity),mean(fuel_consumption) FROM "readings" WHERE ("name"='truck_1' or "name"='truck_2') AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T01:30:00Z' GROUP BY "name",time(10m)`

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

	query1 := NewQuery(querySetBig, "iot_medium", "s")
	resp1, err := dbConn.Query(query1)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("\tresp1:\n", resp1.ToString())
	}
	//values1 := ResponseToByteArray(resp1, querySetBig)
	numOfTab1 := GetNumOfTable(resp1)

	partialSegment := ""
	fields := ""
	metric := ""
	_, startTime1, endTime1, tags1 := GetQueryTemplate(querySetBig)
	partialSegment, fields, metric = GetPartialSegmentAndFields(querySetBig)
	fields = "time[int64]," + fields
	datatypes1 := GetDataTypeArrayFromSF(fields)

	starSegment := GetStarSegment(metric, partialSegment)

	values1 := ResponseToByteArrayWithParams(resp1, datatypes1, tags1, metric, partialSegment)
	err = STsConnArr[0].Set(&stscache.Item{Key: starSegment, Value: values1, Time_start: startTime1, Time_end: endTime1, NumOfTables: numOfTab1})

	query2 := NewQuery(querySetSmall1, "iot_medium", "s")
	resp2, err := dbConn.Query(query2)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("\tresp2:\n", resp2.ToString())
	}
	numOfTab2 := GetNumOfTable(resp2)

	_, startTime2, endTime2, tags2 := GetQueryTemplate(querySetSmall1)
	partialSegment, fields, metric = GetPartialSegmentAndFields(querySetSmall1)
	fields = "time[int64]," + fields
	datatypes2 := GetDataTypeArrayFromSF(fields)

	starSegment = GetStarSegment(metric, partialSegment)
	values2 := ResponseToByteArrayWithParams(resp2, datatypes2, tags2, metric, partialSegment)
	err = STsConnArr[0].Set(&stscache.Item{Key: starSegment, Value: values2, Time_start: startTime2, Time_end: endTime2, NumOfTables: numOfTab2})

	query3 := NewQuery(querySetSmall2, "iot_medium", "s")
	resp3, err := dbConn.Query(query3)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("\tresp3:\n", resp3.ToString())
	}
	//values3 := ResponseToByteArray(resp3, querySetSmall)
	numOfTab3 := GetNumOfTable(resp3)

	_, startTime3, endTime3, tags3 := GetQueryTemplate(querySetSmall2)
	partialSegment, fields, metric = GetPartialSegmentAndFields(querySetSmall2)
	fields = "time[int64]," + fields
	datatypes3 := GetDataTypeArrayFromSF(fields)

	starSegment = GetStarSegment(metric, partialSegment)
	values3 := ResponseToByteArrayWithParams(resp3, datatypes3, tags3, metric, partialSegment)
	err = STsConnArr[0].Set(&stscache.Item{Key: starSegment, Value: values3, Time_start: startTime3, Time_end: endTime3, NumOfTables: numOfTab3})

	_, startTimeGet, endTimeGet, tagsGet := GetQueryTemplate(queryGet)
	partialSegment, fields, metric = GetPartialSegmentAndFields(queryGet)
	fields = "time[int64]," + fields
	datatypesGet := GetDataTypeArrayFromSF(fields)
	semanticSegment := GetTotalSegment(metric, tagsGet, partialSegment)
	//respGet, _, _ := STsCacheClient(dbConn, queryGet)
	valuesGet, _, err := STsConnArr[0].Get(semanticSegment, startTimeGet, endTimeGet)
	respGet, _, _, _, _ := ByteArrayToResponseWithDatatype(valuesGet, datatypesGet)
	fmt.Println("\tresp from cache:\n", respGet.ToString())

	queryG := NewQuery(queryGet, "iot_medium", "s")
	respG, err := dbConn.Query(queryG)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("\tresp from database:\n", respG.ToString())
	}

}

func TestAggregationOperator(t *testing.T) {
	querySet := `SELECT mean(velocity),mean(fuel_consumption) FROM "readings" WHERE ("name"='truck_1' or "name"='truck_2') AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T02:00:00Z' GROUP BY "name",time(5m)`

	queryGet := `SELECT mean(velocity),mean(fuel_consumption) FROM "readings" WHERE ("name"='truck_1' or "name"='truck_2') AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T01:00:00Z' GROUP BY "name",time(15m)`

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

	query1 := NewQuery(querySet, "iot_medium", "s")
	resp1, err := dbConn.Query(query1)
	if err != nil {
		fmt.Println(err)
	} else {
		//fmt.Println("\tresp1:\n", resp1.ToString())
	}
	//values1 := ResponseToByteArray(resp1, querySet)
	numOfTab1 := GetNumOfTable(resp1)

	partialSegment := ""
	fields := ""
	metric := ""
	_, startTime1, endTime1, tags1 := GetQueryTemplate(querySet)
	partialSegment, fields, metric = GetPartialSegmentAndFields(querySet)
	fields = "time[int64]," + fields
	datatypes1 := GetDataTypeArrayFromSF(fields)

	starSegment := GetStarSegment(metric, partialSegment)

	values1 := ResponseToByteArrayWithParams(resp1, datatypes1, tags1, metric, partialSegment)
	err = STsConnArr[0].Set(&stscache.Item{Key: starSegment, Value: values1, Time_start: startTime1, Time_end: endTime1, NumOfTables: numOfTab1})

	_, startTimeGet, endTimeGet, tagsGet := GetQueryTemplate(queryGet)
	partialSegment, fields, metric = GetPartialSegmentAndFields(queryGet)
	fields = "time[int64]," + fields
	datatypesGet := GetDataTypeArrayFromSF(fields)
	semanticSegment := GetTotalSegment(metric, tagsGet, partialSegment)

	valuesGet, _, err := STsConnArr[0].Get(semanticSegment, startTimeGet, endTimeGet)
	respGet, _, _, _, _ := ByteArrayToResponseWithDatatype(valuesGet, datatypesGet)
	fmt.Println("\tresp from cache:\n", respGet.ToString())

	queryG := NewQuery(queryGet, "iot_medium", "s")
	respG, err := dbConn.Query(queryG)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("\tresp from database:\n", respG.ToString())
	}

}

func TestEdgeOperator(t *testing.T) {
	querySet := `SELECT mean(latitude),mean(longitude),mean(elevation) FROM "readings" WHERE ("name"='truck_1' or "name"='truck_2') AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T00:30:00Z' GROUP BY "name",time(1m)`

	queryGet := `SELECT mean(latitude),mean(longitude),mean(elevation) FROM "readings" WHERE ("name"='truck_1' or "name"='truck_2') AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T00:30:00Z' GROUP BY "name",time(10m)`

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

	query1 := NewQuery(querySet, "iot_medium", "s")
	resp1, err := dbConn.Query(query1)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("\tresp set:\n", resp1.ToString())
	}
	//values1 := ResponseToByteArray(resp1, querySet)
	numOfTab1 := GetNumOfTable(resp1)

	_, startTime1, endTime1, tags1 := GetQueryTemplate(querySet)
	partialSegment1, fields1, metric1 := GetPartialSegmentAndFields(querySet)
	fields1 = "time[int64]," + fields1
	datatypes1 := GetDataTypeArrayFromSF(fields1)

	starSegment1 := GetStarSegment(metric1, partialSegment1)

	values1 := ResponseToByteArrayWithParams(resp1, datatypes1, tags1, metric1, partialSegment1)
	err = STsConnArr[0].Set(&stscache.Item{Key: starSegment1, Value: values1, Time_start: startTime1, Time_end: endTime1, NumOfTables: numOfTab1})

	_, startTimeGet, endTimeGet, tagsGet := GetQueryTemplate(queryGet)
	partialSegmentGet, fieldsGet, metricGet := GetPartialSegmentAndFields(queryGet)
	fieldsGet = "time[int64]," + fieldsGet
	datatypesGet := GetDataTypeArrayFromSF(fieldsGet)
	semanticSegment := GetTotalSegment(metricGet, tagsGet, partialSegmentGet)

	valuesGet, _, err := STsConnArr[0].Get(semanticSegment, startTimeGet, endTimeGet)
	respGet, _, _, _, _ := ByteArrayToResponseWithDatatype(valuesGet, datatypesGet)
	fmt.Println("\tresp from cache:\n", respGet.ToString())

	queryDB := NewQuery(queryGet, "iot_medium", "s")
	respDB, err := dbConn.Query(queryDB)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("\tresp from database:\n", respDB.ToString())
	}

}
