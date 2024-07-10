package client

import (
	"fmt"
	stscache "github.com/timescale/tsbs/InfluxDB-client/memcache"
	"log"
	"strings"
	"testing"
	"time"
)

func TestPartialHitPerformance(t *testing.T) {
	var dbConn, _ = NewHTTPClient(HTTPConfig{
		Addr: "http://192.168.1.103:8086",
	})
	DB = "iot_medium"
	TagKV = GetTagKV(dbConn, DB)
	Fields = GetFieldKeys(dbConn, DB)
	cacheUrlString := "192.168.1.101:11211"
	cacheUrlArr := strings.Split(cacheUrlString, ",")
	STsConnArr = InitStsConnsArr(cacheUrlArr)
	fmt.Printf("number of STsConnArr:%d\n", len(STsConnArr))

	tests := []struct {
		name            string
		tagNum          int     // tag 的数量
		timeIntervalNum int64   // 查询时间范围有多少分钟	32个tag的时候，1分钟对应1KB
		hitRatio        float64 // 查数据库和cache的比例 	0 表示全查数据库，1 表示全查cache
	}{
		{
			name:            "",
			tagNum:          32,
			timeIntervalNum: 8,
			hitRatio:        1,
		},
		{
			name:            "",
			tagNum:          32,
			timeIntervalNum: 16,
			hitRatio:        1,
		},
		{
			name:            "",
			tagNum:          32,
			timeIntervalNum: 32,
			hitRatio:        1,
		},
		{
			name:            "",
			tagNum:          32,
			timeIntervalNum: 64,
			hitRatio:        1,
		},
		{
			name:            "",
			tagNum:          32,
			timeIntervalNum: 128,
			hitRatio:        1,
		},
		{
			name:            "",
			tagNum:          32,
			timeIntervalNum: 256,
			hitRatio:        1,
		},
		{
			name:            "",
			tagNum:          32,
			timeIntervalNum: 512,
			hitRatio:        1,
		},
		{
			name:            "",
			tagNum:          32,
			timeIntervalNum: 1024,
			hitRatio:        1,
		},
		{
			name:            "",
			tagNum:          32,
			timeIntervalNum: 2048,
			hitRatio:        0.9,
		},
		{
			name:            "",
			tagNum:          32,
			timeIntervalNum: 4096,
			hitRatio:        0.9,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			// 测试用例的参数
			tagNum := tt.tagNum
			timeInterval := tt.timeIntervalNum * time.Minute.Nanoseconds() / 1e9
			hitRatio := tt.hitRatio

			// cache set 的结束时间
			setTimeInterval := int64(float64(timeInterval) * hitRatio)

			// 数据集的起止时间
			totalBeginTime := TimeStringToInt64(`2022-01-01T00:00:00Z`)
			totalEndTime := TimeStringToInt64(`2022-12-31T00:00:00Z`)

			var queryCount int64 = 0

			var queryTime int64 = 0

			for i := totalBeginTime; i < (totalEndTime+totalBeginTime)/2; i += timeInterval {
				if queryCount == 200 {
					break
				}

				// 统计量
				statStartTime := time.Now()
				var lag int64 = 0

				curStartTime := TimeInt64ToString(i)
				curEndTime := TimeInt64ToString(i + timeInterval)

				// tag string
				tags := make([]string, 0)
				for j := 0; j < tagNum; j++ {
					tags = append(tags, fmt.Sprintf("\"name\"='truck_%d'", j))
				}
				tagString := strings.Join(tags, " or ")

				// cache query string
				setStartTime := curStartTime
				setEndTime := TimeInt64ToString(i + setTimeInterval)
				queryStringSet := fmt.Sprintf("SELECT mean(longitude),mean(latitude),mean(velocity) FROM \"readings\" WHERE (%s) AND TIME >= '%s' AND TIME < '%s' GROUP BY \"name\",time(5m)", tagString, setStartTime, setEndTime)
				//fmt.Println(queryStringSet)

				// database query string
				dbStartTime := setEndTime
				dbEndTime := curEndTime
				queryStringDB := fmt.Sprintf("SELECT mean(longitude),mean(latitude),mean(velocity) FROM \"readings\" WHERE (%s) AND TIME >= '%s' AND TIME < '%s' GROUP BY \"name\",time(5m)", tagString, dbStartTime, dbEndTime)
				//fmt.Println(queryStringDB)

				// 向 cache 存取
				if hitRatio != 0 {
					// 存入 cache
					querySet := NewQuery(queryStringSet, "iot_medium", "s")
					respSet, err := dbConn.Query(querySet)
					if err != nil {
						fmt.Println(err)
					}
					numOfTabSet := GetNumOfTable(respSet)
					_, startTimeSet, endTimeSet, tagsSet := GetQueryTemplate(queryStringSet)
					partialSegment, fields, metric := GetPartialSegmentAndFields(queryStringSet)
					fields = "time[int64]," + fields
					datatypesSet := GetDataTypeArrayFromSF(fields)
					starSegment := GetStarSegment(metric, partialSegment)
					values3 := ResponseToByteArrayWithParams(respSet, datatypesSet, tagsSet, metric, partialSegment)

					err = STsConnArr[0].Set(&stscache.Item{Key: starSegment, Value: values3, Time_start: startTimeSet, Time_end: endTimeSet, NumOfTables: numOfTabSet})

					// 从 cache 取
					statStartTime = time.Now()
					semanticSegment := GetTotalSegment(metric, tagsSet, partialSegment)
					values, _, err := STsConnArr[0].Get(semanticSegment, startTimeSet, endTimeSet)
					if err != nil {
						log.Println(err)
					}
					//rc, _, _, _, _ := ByteArrayToResponseWithDatatype(values, datatypesSet)
					//fmt.Println(rc.ToString())
					//lag = time.Since(statStartTime).Nanoseconds() / 1e6
					lag = time.Since(statStartTime).Nanoseconds()
					queryTime += lag
					fmt.Println("cache length: ", len(values))
					fmt.Println("lag: ", lag)
				}

				// 向数据库存取
				if hitRatio != 1 {
					// 查数据库
					statStartTime = time.Now()
					query := NewQuery(queryStringDB, "iot_medium", "s")
					_, err := dbConn.Query(query)
					if err != nil {
						log.Fatalln(err)
					}
					lag = time.Since(statStartTime).Nanoseconds()
					queryTime += lag

					//byteArr := ResponseToByteArray(resp, queryStringDB)
					//fmt.Println(len(byteArr))
					//fmt.Println(resp.ToString())
				}

				queryCount++
				fmt.Println("count: ", queryCount)
			}

			// 每条查询的平均延迟，单位 ns
			avgLag := queryTime / queryCount // ns

			fmt.Printf("\n\ttag number: %d\ttime interval: %d\t\n\ttotal time: %d ns\tquery count: %d\n\taverage lag: %d ns\n", tagNum, timeInterval, queryTime, queryCount, avgLag)

		})
	}

}

func TestSameTagRequestSize(t *testing.T) {

	/*
		tag 固定，时间范围改变
		32 tag  4 field  time(1m)		1min : 32*4*8 = 1024 bytes
	*/
	// 1 KB		1min
	//queryString := `SELECT mean(longitude),mean(latitude),mean(velocity) FROM "readings" WHERE ("name"='truck_0' or "name"='truck_1' or "name"='truck_2' or "name"='truck_3' or "name"='truck_4' or "name"='truck_5' or "name"='truck_6' or "name"='truck_7' or "name"='truck_8' or "name"='truck_9' or "name"='truck_10' or "name"='truck_11' or "name"='truck_12' or "name"='truck_13' or "name"='truck_14' or "name"='truck_15' or "name"='truck_16' or "name"='truck_17' or "name"='truck_18' or "name"='truck_19' or "name"='truck_20' or "name"='truck_21' or "name"='truck_22' or "name"='truck_23' or "name"='truck_24' or "name"='truck_25' or "name"='truck_26' or "name"='truck_27' or "name"='truck_28' or "name"='truck_29' or "name"='truck_30' or "name"='truck_31') AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T00:01:00Z' GROUP BY "name",time(1m)`

	// 8 KB 	8min
	//queryString := `SELECT mean(longitude),mean(latitude),mean(velocity) FROM "readings" WHERE ("name"='truck_0' or "name"='truck_1' or "name"='truck_2' or "name"='truck_3' or "name"='truck_4' or "name"='truck_5' or "name"='truck_6' or "name"='truck_7' or "name"='truck_8' or "name"='truck_9' or "name"='truck_10' or "name"='truck_11' or "name"='truck_12' or "name"='truck_13' or "name"='truck_14' or "name"='truck_15' or "name"='truck_16' or "name"='truck_17' or "name"='truck_18' or "name"='truck_19' or "name"='truck_20' or "name"='truck_21' or "name"='truck_22' or "name"='truck_23' or "name"='truck_24' or "name"='truck_25' or "name"='truck_26' or "name"='truck_27' or "name"='truck_28' or "name"='truck_29' or "name"='truck_30' or "name"='truck_31') AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T00:08:00Z' GROUP BY "name",time(1m)`

	// 64 KB 	64min		1h4min
	//queryString := `SELECT mean(longitude),mean(latitude),mean(velocity) FROM "readings" WHERE ("name"='truck_0' or "name"='truck_1' or "name"='truck_2' or "name"='truck_3' or "name"='truck_4' or "name"='truck_5' or "name"='truck_6' or "name"='truck_7' or "name"='truck_8' or "name"='truck_9' or "name"='truck_10' or "name"='truck_11' or "name"='truck_12' or "name"='truck_13' or "name"='truck_14' or "name"='truck_15' or "name"='truck_16' or "name"='truck_17' or "name"='truck_18' or "name"='truck_19' or "name"='truck_20' or "name"='truck_21' or "name"='truck_22' or "name"='truck_23' or "name"='truck_24' or "name"='truck_25' or "name"='truck_26' or "name"='truck_27' or "name"='truck_28' or "name"='truck_29' or "name"='truck_30' or "name"='truck_31') AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T01:04:00Z' GROUP BY "name",time(1m)`

	// 512 KB	512min		8h32min
	//queryString := `SELECT mean(longitude),mean(latitude),mean(velocity) FROM "readings" WHERE ("name"='truck_0' or "name"='truck_1' or "name"='truck_2' or "name"='truck_3' or "name"='truck_4' or "name"='truck_5' or "name"='truck_6' or "name"='truck_7' or "name"='truck_8' or "name"='truck_9' or "name"='truck_10' or "name"='truck_11' or "name"='truck_12' or "name"='truck_13' or "name"='truck_14' or "name"='truck_15' or "name"='truck_16' or "name"='truck_17' or "name"='truck_18' or "name"='truck_19' or "name"='truck_20' or "name"='truck_21' or "name"='truck_22' or "name"='truck_23' or "name"='truck_24' or "name"='truck_25' or "name"='truck_26' or "name"='truck_27' or "name"='truck_28' or "name"='truck_29' or "name"='truck_30' or "name"='truck_31') AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T08:32:00Z' GROUP BY "name",time(1m)`

	// 4 MB 	4096min		68h16min
	queryString := `SELECT mean(longitude),mean(latitude),mean(velocity) FROM "readings" WHERE ("name"='truck_0' or "name"='truck_1' or "name"='truck_2' or "name"='truck_3' or "name"='truck_4' or "name"='truck_5' or "name"='truck_6' or "name"='truck_7' or "name"='truck_8' or "name"='truck_9' or "name"='truck_10' or "name"='truck_11' or "name"='truck_12' or "name"='truck_13' or "name"='truck_14' or "name"='truck_15' or "name"='truck_16' or "name"='truck_17' or "name"='truck_18' or "name"='truck_19' or "name"='truck_20' or "name"='truck_21' or "name"='truck_22' or "name"='truck_23' or "name"='truck_24' or "name"='truck_25' or "name"='truck_26' or "name"='truck_27' or "name"='truck_28' or "name"='truck_29' or "name"='truck_30' or "name"='truck_31') AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-03T20:16:00Z' GROUP BY "name",time(1m)`

	cacheUrlString := "192.168.1.102:11211"
	urlArr := strings.Split(cacheUrlString, ",")
	conns := InitStsConnsArr(urlArr)
	DB = "iot_medium"
	fmt.Printf("number of conns:%d\n", len(conns))
	TagKV = GetTagKV(c, "iot_medium")
	Fields = GetFieldKeys(c, "iot_medium")
	STsConnArr = InitStsConnsArr(urlArr)

	partialSegment := ""
	fields := ""
	metric := ""
	queryTemplate, _, _, tags := GetQueryTemplate(queryString)
	partialSegment, fields, metric = GetPartialSegmentAndFields(queryString)
	QueryTemplateToPartialSegment[queryTemplate] = partialSegment
	SegmentToFields[partialSegment] = fields
	SegmentToMetric[partialSegment] = metric
	fields = "time[int64]," + fields
	datatypes := GetDataTypeArrayFromSF(fields)

	var dbConn, _ = NewHTTPClient(HTTPConfig{
		Addr: "http://192.168.1.103:8086",
	})

	query := NewQuery(queryString, "iot_medium", "s")
	resp, err := dbConn.Query(query)
	if err != nil {
		log.Fatalln(err)
	}
	byteArr := ResponseToByteArrayWithParams(resp, datatypes, tags, metric, partialSegment)

	fmt.Println("byte length: ", len(byteArr))
	//fmt.Println(resp.ToString())

}

func TestSameTimeRangeRequestSize(t *testing.T) {

	/*
		时间范围固定 ( 512min	 8h32min )，tag 数量改变( 1 - 256 )
		1 tag  		16 KB		1*4*8*512 = 16384 bytes
		4 tag		64 KB
		32 tag		512 KB
		256 tag  	4 MB			256*4*8*512 = 4194304 bytes
	*/
	// 1 KB		1min
	queryString := `SELECT mean(longitude),mean(latitude),mean(velocity) FROM "readings" WHERE ("name"='truck_0') AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T08:32:00Z' GROUP BY "name",time(1m)`

	cacheUrlString := "192.168.1.102:11211"
	urlArr := strings.Split(cacheUrlString, ",")
	conns := InitStsConnsArr(urlArr)
	DB = "iot_medium"
	fmt.Printf("number of conns:%d\n", len(conns))
	TagKV = GetTagKV(c, "iot_medium")
	Fields = GetFieldKeys(c, "iot_medium")
	STsConnArr = InitStsConnsArr(urlArr)

	partialSegment := ""
	fields := ""
	metric := ""
	queryTemplate, _, _, tags := GetQueryTemplate(queryString)
	partialSegment, fields, metric = GetPartialSegmentAndFields(queryString)
	QueryTemplateToPartialSegment[queryTemplate] = partialSegment
	SegmentToFields[partialSegment] = fields
	SegmentToMetric[partialSegment] = metric
	fields = "time[int64]," + fields
	datatypes := GetDataTypeArrayFromSF(fields)

	var dbConn, _ = NewHTTPClient(HTTPConfig{
		Addr: "http://192.168.1.103:8086",
	})

	query := NewQuery(queryString, "iot_medium", "s")
	resp, err := dbConn.Query(query)
	if err != nil {
		log.Fatalln(err)
	}
	byteArr := ResponseToByteArrayWithParams(resp, datatypes, tags, metric, partialSegment)

	fmt.Println("byte length: ", len(byteArr))
	//fmt.Println(resp.ToString())

}

func TestRequestSize(t *testing.T) {

	// 1.04 KB (1070 bytes)  5h  time(10m)  3field  1tag
	queryString := `SELECT mean(longitude),mean(latitude),mean(velocity) FROM "readings" WHERE ("name"='truck_1') AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T05:00:00Z' GROUP BY "name",time(10m)`

	// 8.36 KB (8560 bytes)  5h  time(10m)  3field  8tag
	//queryString := `SELECT mean(longitude),mean(latitude),mean(velocity) FROM "readings" WHERE ("name"='truck_1' or "name"='truck_2' or "name"='truck_3' or "name"='truck_4' or "name"='truck_5' or "name"='truck_6' or "name"='truck_7' or "name"='truck_8') AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T05:00:00Z' GROUP BY "name",time(10m)`

	// 16.72 KB (17128 bytes)  5h  time(10m)  3field  16tag
	//queryString := `SELECT mean(longitude),mean(latitude),mean(velocity) FROM "readings" WHERE ("name"='truck_1' or "name"='truck_2' or "name"='truck_3' or "name"='truck_4' or "name"='truck_5' or "name"='truck_6' or "name"='truck_7' or "name"='truck_8' or "name"='truck_11' or "name"='truck_12' or "name"='truck_13' or "name"='truck_14' or "name"='truck_15' or "name"='truck_16' or "name"='truck_17' or "name"='truck_18') AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T05:00:00Z' GROUP BY "name",time(10m)`

	// 33.46 KB (34264 bytes)  5h  time(10m)  3field  32tag
	//queryString := `SELECT mean(longitude),mean(latitude),mean(velocity) FROM "readings" WHERE ("name"='truck_1' or "name"='truck_2' or "name"='truck_3' or "name"='truck_4' or "name"='truck_5' or "name"='truck_6' or "name"='truck_7' or "name"='truck_8' or "name"='truck_11' or "name"='truck_12' or "name"='truck_13' or "name"='truck_14' or "name"='truck_15' or "name"='truck_16' or "name"='truck_17' or "name"='truck_18' or "name"='truck_21' or "name"='truck_22' or "name"='truck_23' or "name"='truck_24' or "name"='truck_25' or "name"='truck_26' or "name"='truck_27' or "name"='truck_28' or "name"='truck_31' or "name"='truck_32' or "name"='truck_33' or "name"='truck_34' or "name"='truck_35' or "name"='truck_36' or "name"='truck_37' or "name"='truck_38') AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T05:00:00Z' GROUP BY "name",time(10m)`

	// 65.87 KB (67448 bytes)  5h  time(10m)  7field  32tag
	//queryString := `SELECT mean(longitude),mean(latitude),mean(velocity),mean(elevation),mean(grade),mean(heading),mean(fuel_consumption) FROM "readings" WHERE ("name"='truck_1' or "name"='truck_2' or "name"='truck_3' or "name"='truck_4' or "name"='truck_5' or "name"='truck_6' or "name"='truck_7' or "name"='truck_8' or "name"='truck_11' or "name"='truck_12' or "name"='truck_13' or "name"='truck_14' or "name"='truck_15' or "name"='truck_16' or "name"='truck_17' or "name"='truck_18' or "name"='truck_21' or "name"='truck_22' or "name"='truck_23' or "name"='truck_24' or "name"='truck_25' or "name"='truck_26' or "name"='truck_27' or "name"='truck_28' or "name"='truck_31' or "name"='truck_32' or "name"='truck_33' or "name"='truck_34' or "name"='truck_35' or "name"='truck_36' or "name"='truck_37' or "name"='truck_38') AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T05:00:00Z' GROUP BY "name",time(10m)`

	// 129.77 KB (132882 bytes)  5h  time(5m)  7field  33tag
	//queryString := `SELECT mean(longitude),mean(latitude),mean(velocity),mean(elevation),mean(grade),mean(heading),mean(fuel_consumption) FROM "readings" WHERE ("name"='truck_0' or "name"='truck_1' or "name"='truck_2' or "name"='truck_3' or "name"='truck_4' or "name"='truck_5' or "name"='truck_6' or "name"='truck_7' or "name"='truck_8' or "name"='truck_11' or "name"='truck_12' or "name"='truck_13' or "name"='truck_14' or "name"='truck_15' or "name"='truck_16' or "name"='truck_17' or "name"='truck_18' or "name"='truck_21' or "name"='truck_22' or "name"='truck_23' or "name"='truck_24' or "name"='truck_25' or "name"='truck_26' or "name"='truck_27' or "name"='truck_28' or "name"='truck_31' or "name"='truck_32' or "name"='truck_33' or "name"='truck_34' or "name"='truck_35' or "name"='truck_36' or "name"='truck_37' or "name"='truck_38') AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T05:00:00Z' GROUP BY "name",time(5m)`

	// 258.05 KB (264246 bytes)  5h  time(2m)  7field  27tag
	//queryString := `SELECT mean(longitude),mean(latitude),mean(velocity),mean(elevation),mean(grade),mean(heading),mean(fuel_consumption) FROM "readings" WHERE ("name"='truck_6' or "name"='truck_7' or "name"='truck_8' or "name"='truck_11' or "name"='truck_12' or "name"='truck_13' or "name"='truck_14' or "name"='truck_15' or "name"='truck_16' or "name"='truck_17' or "name"='truck_18' or "name"='truck_21' or "name"='truck_22' or "name"='truck_23' or "name"='truck_24' or "name"='truck_25' or "name"='truck_26' or "name"='truck_27' or "name"='truck_28' or "name"='truck_31' or "name"='truck_32' or "name"='truck_33' or "name"='truck_34' or "name"='truck_35' or "name"='truck_36' or "name"='truck_37' or "name"='truck_38') AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T05:00:00Z' GROUP BY "name",time(2m)`

	// 511.18 KB (523446 bytes)  5h  time(1m)  7field  27tag
	//queryString := `SELECT mean(longitude),mean(latitude),mean(velocity),mean(elevation),mean(grade),mean(heading),mean(fuel_consumption) FROM "readings" WHERE ("name"='truck_6' or "name"='truck_7' or "name"='truck_8' or "name"='truck_11' or "name"='truck_12' or "name"='truck_13' or "name"='truck_14' or "name"='truck_15' or "name"='truck_16' or "name"='truck_17' or "name"='truck_18' or "name"='truck_21' or "name"='truck_22' or "name"='truck_23' or "name"='truck_24' or "name"='truck_25' or "name"='truck_26' or "name"='truck_27' or "name"='truck_28' or "name"='truck_31' or "name"='truck_32' or "name"='truck_33' or "name"='truck_34' or "name"='truck_35' or "name"='truck_36' or "name"='truck_37' or "name"='truck_38') AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T05:00:00Z' GROUP BY "name",time(1m)`

	// 1017.45 KB (1041873 bytes)  5h  time(30s)  7field  27tag
	//queryString := `SELECT mean(longitude),mean(latitude),mean(velocity),mean(elevation),mean(grade),mean(heading),mean(fuel_consumption) FROM "readings" WHERE ("name"='truck_6' or "name"='truck_7' or "name"='truck_8' or "name"='truck_11' or "name"='truck_12' or "name"='truck_13' or "name"='truck_14' or "name"='truck_15' or "name"='truck_16' or "name"='truck_17' or "name"='truck_18' or "name"='truck_21' or "name"='truck_22' or "name"='truck_23' or "name"='truck_24' or "name"='truck_25' or "name"='truck_26' or "name"='truck_27' or "name"='truck_28' or "name"='truck_31' or "name"='truck_32' or "name"='truck_33' or "name"='truck_34' or "name"='truck_35' or "name"='truck_36' or "name"='truck_37' or "name"='truck_38') AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T05:00:00Z' GROUP BY "name",time(30s)`

	// 2041.60 KB (2080360 bytes)  5h  time(20s)  7field  36tag
	//queryString := `SELECT mean(longitude),mean(latitude),mean(velocity),mean(elevation),mean(grade),mean(heading),mean(fuel_consumption) FROM "readings" WHERE ("name"='truck_1' or "name"='truck_2' or "name"='truck_3' or "name"='truck_4' or "name"='truck_5' or "name"='truck_6' or "name"='truck_7' or "name"='truck_8' or "name"='truck_11' or "name"='truck_12' or "name"='truck_13' or "name"='truck_14' or "name"='truck_15' or "name"='truck_16' or "name"='truck_17' or "name"='truck_18' or "name"='truck_21' or "name"='truck_22' or "name"='truck_23' or "name"='truck_24' or "name"='truck_25' or "name"='truck_26' or "name"='truck_27' or "name"='truck_28' or "name"='truck_31' or "name"='truck_32' or "name"='truck_33' or "name"='truck_34' or "name"='truck_35' or "name"='truck_36' or "name"='truck_37' or "name"='truck_38' or "name"='truck_41' or "name"='truck_42' or "name"='truck_43' or "name"='truck_44') AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T05:00:00Z' GROUP BY "name",time(20s)`

	// 4056.60 KB (4153960 bytes)  5h  time(10s)  7field  36tag
	//queryString := `SELECT mean(longitude),mean(latitude),mean(velocity),mean(elevation),mean(grade),mean(heading),mean(fuel_consumption) FROM "readings" WHERE ("name"='truck_1' or "name"='truck_2' or "name"='truck_3' or "name"='truck_4' or "name"='truck_5' or "name"='truck_6' or "name"='truck_7' or "name"='truck_8' or "name"='truck_11' or "name"='truck_12' or "name"='truck_13' or "name"='truck_14' or "name"='truck_15' or "name"='truck_16' or "name"='truck_17' or "name"='truck_18' or "name"='truck_21' or "name"='truck_22' or "name"='truck_23' or "name"='truck_24' or "name"='truck_25' or "name"='truck_26' or "name"='truck_27' or "name"='truck_28' or "name"='truck_31' or "name"='truck_32' or "name"='truck_33' or "name"='truck_34' or "name"='truck_35' or "name"='truck_36' or "name"='truck_37' or "name"='truck_38' or "name"='truck_41' or "name"='truck_42' or "name"='truck_43' or "name"='truck_44') AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T05:00:00Z' GROUP BY "name",time(10s)`

	cacheUrlString := "192.168.1.102:11211"
	urlArr := strings.Split(cacheUrlString, ",")
	conns := InitStsConnsArr(urlArr)
	DB = "iot_medium"
	fmt.Printf("number of conns:%d\n", len(conns))
	TagKV = GetTagKV(c, "iot_medium")
	Fields = GetFieldKeys(c, "iot_medium")
	STsConnArr = InitStsConnsArr(urlArr)

	partialSegment := ""
	fields := ""
	metric := ""
	queryTemplate, _, _, tags := GetQueryTemplate(queryString)
	partialSegment, fields, metric = GetPartialSegmentAndFields(queryString)
	QueryTemplateToPartialSegment[queryTemplate] = partialSegment
	SegmentToFields[partialSegment] = fields
	SegmentToMetric[partialSegment] = metric
	fields = "time[int64]," + fields
	datatypes := GetDataTypeArrayFromSF(fields)

	var dbConn, _ = NewHTTPClient(HTTPConfig{
		Addr: "http://192.168.1.103:8086",
	})

	query := NewQuery(queryString, "iot_medium", "s")
	resp, err := dbConn.Query(query)
	if err != nil {
		log.Fatalln(err)
	}
	byteArr := ResponseToByteArrayWithParams(resp, datatypes, tags, metric, partialSegment)

	fmt.Println("byte length: ", len(byteArr))
	//fmt.Println(resp.ToString())

}

// 结果表明：请求数据大小相同的情况下，聚合时间越小，数据库查询速度越快
func TestTimeInterval(t *testing.T) {
	queryString1 := `SELECT mean(longitude),mean(latitude),mean(velocity) FROM "readings" WHERE ("name"='truck_1') AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T05:00:00Z' GROUP BY "name",time(10m)`
	queryString2 := `SELECT mean(longitude),mean(latitude),mean(velocity) FROM "readings" WHERE ("name"='truck_1') AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T10:00:00Z' GROUP BY "name",time(10m)`

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

	query1 := NewQuery(queryString1, "iot_medium", "s")
	resp1, err := dbConn.Query(query1)
	if err != nil {
		log.Fatalln(err)
	}
	byteArr1 := ResponseToByteArray(resp1, queryString1)
	fmt.Println(len(byteArr1))

	query2 := NewQuery(queryString2, "iot_medium", "s")
	resp2, err := dbConn.Query(query2)
	if err != nil {
		log.Fatalln(err)
	}
	byteArr2 := ResponseToByteArray(resp2, queryString2)
	fmt.Println(len(byteArr2))

	startTime1 := time.Now()

	for i := 0; i < 1000; i++ {
		query1 := NewQuery(queryString1, "iot_medium", "s")
		_, err := dbConn.Query(query1)
		if err != nil {
			log.Fatalln(err)
		}
	}

	lag1 := time.Since(startTime1).Nanoseconds() / 1e6
	fmt.Println("lag 1: ", lag1)

	startTime2 := time.Now()

	for i := 0; i < 1000; i++ {
		query2 := NewQuery(queryString2, "iot_medium", "s")
		_, err := dbConn.Query(query2)
		if err != nil {
			log.Fatalln(err)
		}
	}

	lag2 := time.Since(startTime2).Nanoseconds() / 1e6
	fmt.Println("lag 2: ", lag2)

}
