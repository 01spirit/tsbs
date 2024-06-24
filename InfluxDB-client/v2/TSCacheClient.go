package client

import (
	"fmt"
	stscache "github.com/timescale/tsbs/InfluxDB-client/memcache"
	"log"
	"time"
)

func TSCacheClient(conn Client, queryString string) (*Response, uint64, uint8) {

	CacheNum := len(STsConnArr)

	if CacheNum == 0 {
		CacheNum = 1
	}

	byteLength := uint64(0)
	hitKind := uint8(0)

	queryTemplate, startTime, endTime, tags := GetQueryTemplate(queryString)

	mtx.Lock()

	partialSegment := ""
	fields := ""
	metric := ""
	if ps, ok := QueryTemplateToPartialSegment[queryTemplate]; !ok {
		partialSegment, fields, metric = GetPartialSegmentAndFields(queryString)
		QueryTemplateToPartialSegment[queryTemplate] = partialSegment
		SegmentToFields[partialSegment] = fields
		SegmentToMetric[partialSegment] = metric
	} else {
		partialSegment = ps
		fields = SegmentToFields[partialSegment]
		metric = SegmentToMetric[partialSegment]
	}

	// 用于 Get 的语义段
	semanticSegment := GetTotalSegment(metric, tags, partialSegment)

	CacheIndex := GetCacheHashValue(fields)
	fields = "time[int64]," + fields
	datatypes := GetDataTypeArrayFromSF(fields)

	mtx.Unlock()

	/* 向 cache 查询数据 */
	values, _, err := STsConnArr[CacheIndex].Get(semanticSegment, startTime, endTime)
	if err != nil { // 缓存未命中
		/* 向数据库查询全部数据，存入 cache */
		q := NewQuery(queryString, DB, "s")
		resp, err := conn.Query(q)
		if err != nil {
			log.Println(queryString)
		}

		if !ResponseIsEmpty(resp) {
			numOfTab := GetNumOfTable(resp)

			//remainValues := ResponseToByteArray(resp, queryString)
			remainValues := ResponseToByteArrayWithParams(resp, datatypes, tags, metric, partialSegment)

			err = STsConnArr[CacheIndex].Set(&stscache.Item{Key: semanticSegment, Value: remainValues, Time_start: startTime, Time_end: endTime, NumOfTables: numOfTab})

			if err != nil {
				//log.Printf("Error setting value: %v\nQUERY STRING:\t%s\n", err, queryString)
			} else {
				//log.Printf("STORED.")
			}

		} else { // 查数据库为空

			//num++
			//fmt.Println("miss number: ", num)
			//fmt.Printf("\tdatabase miss 1:%s\n", queryString)
		}

		return resp, byteLength, hitKind

	} else { // 缓存部分命中或完全命中
		/* 把查询结果从字节流转换成 Response 结构 */
		convertedResponse, flagNum, flagArr, timeRangeArr, tagArr := ByteArrayToResponseWithDatatype(values, datatypes)

		//fmt.Println("\tconverted response:")
		//fmt.Println(len(values))
		//fmt.Println(convertedResponse.ToString())

		if flagNum == 0 { // 全部命中
			//log.Printf("GET.")
			hitKind = 2
			//log.Printf("bytes get:%d\n", len(values))
			return convertedResponse, byteLength, hitKind

		} else { // 部分命中，剩余查询
			hitKind = 1

			remainQueryString, minTime, maxTime := RemainQueryString(queryString, flagArr, timeRangeArr, tagArr)

			// tagArr 是要查询的所有 tag ，remainTags 是部分命中的 tag
			remainTags := make([]string, 0)
			for i, tag := range tagArr {
				if flagArr[i] == 1 {
					remainTags = append(remainTags, fmt.Sprintf("%s=%s", tag[0], tag[1]))
				}

			}
			//fmt.Println("\t", remainQueryString)

			// 太小的剩余查询区间直接略过
			if maxTime-minTime <= int64(time.Minute.Seconds()) {
				hitKind = 2

				//fmt.Printf("\tremain resp too small 2:%s\n", queryString)

				return convertedResponse, byteLength, hitKind
			}

			remainQuery := NewQuery(remainQueryString, DB, "s")
			remainResp, err := conn.Query(remainQuery)
			if err != nil {
				log.Println(remainQueryString)
			}

			//fmt.Println("\tremain resp:\n", remainResp.ToString())

			// 查数据库为空
			if ResponseIsEmpty(remainResp) {
				hitKind = 2

				//fmt.Printf("\tdatabase miss 2:%s\n", remainQueryString)

				return convertedResponse, byteLength, hitKind
			}

			//remainByteArr := ResponseToByteArray(remainResp, queryString)
			remainByteArr := RemainResponseToByteArrayWithParams(remainResp, datatypes, remainTags, metric, partialSegment)

			numOfTableR := len(remainResp.Results)

			err = STsConnArr[CacheIndex].Set(&stscache.Item{
				Key:         semanticSegment,
				Value:       remainByteArr,
				Time_start:  minTime,
				Time_end:    maxTime,
				NumOfTables: int64(numOfTableR),
			})

			if err != nil {
				//log.Printf("partial get Set fail: %v\tvalue length:%d\tthread:%d\nQUERY STRING:\t%s\n", err, len(remainByteArr), workerNum, remainQueryString)
			} else {
				//log.Printf("bytes set:%d\n", len(remainByteArr))
			}

			// 剩余结果合并
			//totalResp := MergeResponse(remainResp, convertedResponse)
			totalResp := MergeRemainResponse(remainResp, convertedResponse)

			return totalResp, byteLength, hitKind
		}

	}

}
