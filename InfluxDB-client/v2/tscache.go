package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/timescale/tsbs/InfluxDB-client/models"
	"log"
	"strconv"
	"strings"
)

func TSCacheResponseToByteArrayWithParams(resp *Response, queryString string, datatypes []string, tags []string, metric string, partialSegment string) []byte {
	columnNum := 0 // 列的数量

	/* 结果为空 */
	if ResponseIsEmpty(resp) {
		return StringToByteArray("empty response")
	}

	//mtx.Lock()

	columnNum = len(datatypes)
	singleSegments := GetSingleSegment(metric, partialSegment, tags)

	//mtx.Unlock()

	result := make([]byte, 0)
	columnBytes := make([][]byte, columnNum) // 分别存储每列的所有字节，包括时间戳和一个field

	// i : 子表序号
	for i, series := range resp.Results[0].Series {
		// 子表数据行数
		rowNums := len(series.Values)
		// 时间戳字节数组
		timestampByteArray := make([][]byte, 0)
		for _, value := range series.Values {
			datatype := "int64"
			tmpBytes := InterfaceToByteArray(0, datatype, value[0])
			timestampByteArray = append(timestampByteArray, tmpBytes)
		}

		columnBytes = make([][]byte, columnNum)
		// j : 行序号
		for j, value := range series.Values {
			// k : 列序号
			for k, val := range value {
				if k == 0 {
					continue
				}
				columnBytes[k-1] = append(columnBytes[k-1], timestampByteArray[j]...)
				tmpBytes := InterfaceToByteArray(k, datatypes[k], val)
				columnBytes[k-1] = append(columnBytes[k-1], tmpBytes...)
			}
		}

		for j := range series.Columns {
			if j == 0 {
				continue
			}
			// field 数据类型和每行字节数（算上时间戳和一个field）
			datatype := datatypes[j]
			bytesPerLine := 8
			switch datatype {
			case "bool":
				bytesPerLine += 1
				break
			case "int64":
				bytesPerLine += 8
				break
			case "float64":
				bytesPerLine += 8
				break
			case "string":
				bytesPerLine += STRINGBYTELENGTH
				break
			default:
				bytesPerLine += 8
				break
			}
			// 该列总字节数，包括时间戳和一个field
			totalByteLength, err := Int64ToByteArray(int64(bytesPerLine * rowNums))
			if err != nil {
				log.Fatalf("TSCache ResponseToByteArray:convert bytesPerLine to bytes fail:%v", err)
			}

			// 单列共用子表语义段
			result = append(result, []byte(singleSegments[i])...)
			result = append(result, []byte(" ")...)
			result = append(result, totalByteLength...)

			// 一列的数据（时间戳和一个field）
			result = append(result, columnBytes[j-1]...)
		}
	}
	return result
}

func TSCacheResponseToByteArray(resp *Response, queryString string) []byte {
	columnNum := 0 // 列的数量

	/* 结果为空 */
	if ResponseIsEmpty(resp) {
		return StringToByteArray("empty response")
	}

	/* 获取每一列的数据类型 */
	datatypes := make([]string, 0)
	datatypes = append(datatypes, "int64") // 第一列时间戳

	mtx.Lock()

	// 语义段和数据类型，存为全局变量
	semanticSegment := ""
	fields := "" // eg: elevation[float64]
	queryTemplate, _, _, _ := GetQueryTemplate(queryString)
	if ss, ok := QueryTemplates[queryTemplate]; !ok { // 查询模版中不存在该查询
		semanticSegment, fields = GetSemanticSegmentAndFields(queryString)
		QueryTemplates[queryTemplate] = semanticSegment
		SegmentToFields[semanticSegment] = fields
	} else {
		semanticSegment = ss
		fields = SegmentToFields[semanticSegment]
	}
	fieldArr := strings.Split(fields, ",")
	columnNum = len(fieldArr)
	for i := range fieldArr {
		startIndex := strings.Index(fieldArr[i], "[")
		endIndex := strings.Index(fieldArr[i], "]")
		datatypes = append(datatypes, fieldArr[i][startIndex+1:endIndex])
	}

	// 每张子表单独的语义段
	seperateSemanticSegment := GetSeperateSemanticSegment(queryString)

	mtx.Unlock()

	result := make([]byte, 0)
	columnBytes := make([][]byte, columnNum) // 分别存储每列的所有字节，包括时间戳和一个field

	// i : 子表序号
	for i, series := range resp.Results[0].Series {
		// 子表数据行数
		rowNums := len(series.Values)
		// 时间戳字节数组
		timestampByteArray := make([][]byte, 0)
		for _, value := range series.Values {
			datatype := "int64"
			tmpBytes := InterfaceToByteArray(0, datatype, value[0])
			timestampByteArray = append(timestampByteArray, tmpBytes)
		}

		columnBytes = make([][]byte, columnNum)
		// j : 行序号
		for j, value := range series.Values {
			// k : 列序号
			for k, val := range value {
				if k == 0 {
					continue
				}
				columnBytes[k-1] = append(columnBytes[k-1], timestampByteArray[j]...)
				tmpBytes := InterfaceToByteArray(k, datatypes[k], val)
				columnBytes[k-1] = append(columnBytes[k-1], tmpBytes...)
			}
		}

		for j := range series.Columns {
			if j == 0 {
				continue
			}
			// field 数据类型和每行字节数（算上时间戳和一个field）
			datatype := datatypes[j]
			bytesPerLine := 8
			switch datatype {
			case "bool":
				bytesPerLine += 1
				break
			case "int64":
				bytesPerLine += 8
				break
			case "float64":
				bytesPerLine += 8
				break
			case "string":
				bytesPerLine += STRINGBYTELENGTH
				break
			default:
				bytesPerLine += 8
				break
			}
			// 该列总字节数，包括时间戳和一个field
			totalByteLength, err := Int64ToByteArray(int64(bytesPerLine * rowNums))
			if err != nil {
				log.Fatalf("TSCache ResponseToByteArray:convert bytesPerLine to bytes fail:%v", err)
			}

			// 单列共用子表语义段
			result = append(result, []byte(seperateSemanticSegment[i])...)
			result = append(result, []byte(" ")...)
			result = append(result, totalByteLength...)

			// 一列的数据（时间戳和一个field）
			result = append(result, columnBytes[j-1]...)
		}
	}
	return result
}

func TSSCacheByteArrayToResponse(byteArray []byte, datatypes []string) (*Response, int, []uint8, [][]int64, [][]string) {
	/* 没有数据 */
	if len(byteArray) == 0 {
		return nil, 0, nil, nil, nil
	}

	seriesValues := make([][][]interface{}, 0) // 所有子表的数据
	seriesValue := make([][]interface{}, 0)    // 一个子表的所有数据

	newSeries := true //是否是新的表

	singleSemanticSegments := make([]string, 0) // 子表语义段
	columnTotalLength := make([]int64, 0)       // 一张子表中的数据（时间戳+field）的总字节数
	columnIndex := 1                            // 当前数据属于子表中的第几列（首列是时间戳）
	preSeg := ""                                // 前一个子表语义段，是否与 curSeg 相同，若相同说明在同一张子表中，是不同的列

	flagNum := 0
	flagArr := make([]uint8, 0)
	timeRangeArr := make([][]int64, 0) // 每张表的剩余待查询时间范围
	tagArr := make([][]string, 0)

	var curSeg string        // 当前表的语义段
	var curLen int64         // 当前表的数据的总字节数
	index := 0               // byteArray 数组的索引，指示当前要转换的字节的位置
	length := len(byteArray) // Get()获取的总字节数

	//fmt.Println("*")
	perIndex := 0
	isStart := 0
	/* 转换 */
	for index < length {
		/* 结束转换 */
		if index == length-2 { // 索引指向数组的最后两字节
			if byteArray[index] == 13 && byteArray[index+1] == 10 { // "\r\n"，表示Get()返回的字节数组的末尾，结束转换		Get()除了返回查询数据之外，还会在数据末尾添加一个 "\r\n",如果读到这个组合，说明到达数组末尾
				break
			} else {
				log.Fatal(errors.New("expect CRLF in the end of []byte"))
			}
		}
		if isStart == 1 && perIndex == index {
			return nil, 0, nil, nil, nil
		}
		isStart = 1
		perIndex = index

		// 语义段、部分命中的剩余时间范围、总字节数
		curSeg = ""
		curLen = 0
		if byteArray[index] == 123 && byteArray[index+1] == 40 { // "{(" ASCII码	表示语义段的开始位置
			ssStartIdx := index
			for byteArray[index] != 32 { // ' '空格，表示语义段的结束位置的后一位
				index++
			}
			ssEndIdx := index                               // 此时索引指向 len 前面的 空格
			curSeg = string(byteArray[ssStartIdx:ssEndIdx]) // 读取所有表示语义段的字节，直接转换为字符串

			if curSeg == preSeg { // 同一子表中的不同列
				columnIndex++
				newSeries = false
			} else { // 不同子表
				singleSemanticSegments = append(singleSemanticSegments, curSeg)
				columnIndex = 1
				preSeg = curSeg
				newSeries = true
			}

			// todo 时间范围
			//index++ // uint8
			//flag := uint8(byteArray[index])
			//index++
			//flagArr = append(flagArr, flag)
			//if flag == 1 {
			//	flagNum++
			//	singleTimeRange := make([]int64, 2)
			//	ftimeStartIdx := index // 索引指向第一个时间戳
			//	index += 8
			//	ftimeEndIdx := index // 索引指向 len 后面一位的回车符 '\r' ，再后面一位是 '\n'
			//	tmpBytes := byteArray[ftimeStartIdx:ftimeEndIdx]
			//	startTime, err := ByteArrayToInt64(tmpBytes) // 读取 len ，转换为int64
			//	if err != nil {
			//		log.Fatal(err)
			//	}
			//	singleTimeRange[0] = startTime
			//
			//	stimeStartIdx := index // 索引指向第一个时间戳
			//	index += 8
			//	stimeEndIdx := index // 索引指向 len 后面一位的回车符 '\r' ，再后面一位是 '\n'
			//	tmpBytes = byteArray[stimeStartIdx:stimeEndIdx]
			//	endTime, err := ByteArrayToInt64(tmpBytes) // 读取 len ，转换为int64
			//	if err != nil {
			//		log.Fatal(err)
			//	}
			//	singleTimeRange[1] = endTime
			//
			//	//fmt.Printf("%d %d\n", startTime, endTime)
			//
			//	timeRangeArr = append(timeRangeArr, singleTimeRange)
			//} else {
			//	singleTimeRange := make([]int64, 2)
			//	singleTimeRange[0] = 0
			//	singleTimeRange[1] = 0
			//	timeRangeArr = append(timeRangeArr, singleTimeRange)
			//}

			index++              // 空格，后面的8字节是表示一张表中数据总字节数的int64
			lenStartIdx := index // 索引指向 len 的第一个字节
			index += 8
			lenEndIdx := index // 索引指向 len 后面一位的回车符 '\r' ，再后面一位是 '\n'
			tmpBytes := byteArray[lenStartIdx:lenEndIdx]
			serLen, err := ByteArrayToInt64(tmpBytes) // 读取 len ，转换为int64
			if err != nil {
				log.Fatal(err)
			}
			curLen = serLen

			columnTotalLength = append(columnTotalLength, curLen)
		}

		// 每行字节数
		datatype := datatypes[columnIndex]
		bytesPerLine := 8
		switch datatype {
		case "bool":
			bytesPerLine += 1
			break
		case "int64":
			bytesPerLine += 8
			break
		case "float64":
			bytesPerLine += 8
			break
		case "string":
			bytesPerLine += STRINGBYTELENGTH
			break
		default:
			bytesPerLine += 8
			break
		}
		// 数据行数
		lines := int(curLen) / bytesPerLine
		curLine := 0
		if newSeries {
			seriesValue = make([][]interface{}, lines)
		}

		for curLine < lines { // 按列读取一张表中的所有数据

			if newSeries {
				iStartIdx := index
				index += 8
				iEndIdx := index
				tmp, err := ByteArrayToInt64(byteArray[iStartIdx:iEndIdx])
				if err != nil {
					log.Fatal(err)
				}
				str := strconv.FormatInt(tmp, 10)
				jNumber := json.Number(str)
				seriesValue[curLine] = append(seriesValue[curLine], jNumber)
			} else {
				index += 8 // 跳过时间戳
			}

			switch datatype {
			case "string":
				bStartIdx := index
				index += STRINGBYTELENGTH
				bEndIdx := index
				tmp := ByteArrayToString(byteArray[bStartIdx:bEndIdx])
				if err != nil {
					log.Fatal(err)
				}
				seriesValue[curLine] = append(seriesValue[curLine], tmp)
				break
			case "bool":
				bStartIdx := index
				index += 1
				bEndIdx := index
				tmp, err := ByteArrayToBool(byteArray[bStartIdx:bEndIdx])
				if err != nil {
					log.Fatal(err)
				}
				seriesValue[curLine] = append(seriesValue[curLine], tmp)
				break
			case "int64":
				iStartIdx := index
				index += 8
				iEndIdx := index
				tmp, err := ByteArrayToInt64(byteArray[iStartIdx:iEndIdx])
				if err != nil {
					log.Fatal(err)
				}
				str := strconv.FormatInt(tmp, 10)
				jNumber := json.Number(str)
				seriesValue[curLine] = append(seriesValue[curLine], jNumber)
				break
			case "float64":
				fStartIdx := index
				index += 8
				fEndIdx := index
				tmp, err := ByteArrayToFloat64(byteArray[fStartIdx:fEndIdx])
				if err != nil {
					log.Fatal(err)
				}
				str := strconv.FormatFloat(tmp, 'g', -1, 64)
				jNumber := json.Number(str)
				seriesValue[curLine] = append(seriesValue[curLine], jNumber)
				break
			default: // float64
				sStartIdx := index
				index += 8
				sEndIdx := index
				tmp, err := ByteArrayToFloat64(byteArray[sStartIdx:sEndIdx])
				if err != nil {
					log.Fatal(err)
				}
				seriesValue[curLine] = append(seriesValue[curLine], tmp)
				break
			}
			curLine++
		}

		// 一张子表的最后一列转换完成，存入结果
		if columnIndex == len(datatypes)-1 {
			seriesValues = append(seriesValues, seriesValue)
		}

	}
	//seriesValues = append(seriesValues, seriesValue)

	modelsRows := make([]models.Row, 0)

	for i, s := range singleSemanticSegments {
		messages := strings.Split(s, "#")
		/* 处理 ssm */
		ssm := messages[0][2 : len(messages[0])-2] // 去掉SM两侧的 大括号和小括号
		merged := strings.Split(ssm, ",")
		nameIndex := strings.Index(merged[0], ".") // 提取 measurement name
		name := merged[0][:nameIndex]
		tags := make(map[string]string)
		/* 取出所有tag 当前只处理了一个tag的情况*/
		tag := merged[0][nameIndex+1 : len(merged[0])]
		eqIdx := strings.Index(tag, "=") // tag 和 value 由  "=" 连接
		if eqIdx <= 0 {                  // 没有等号说明没有tag
			break
		}
		key := tag[:eqIdx] // Response 中的 tag 结构为 map[string]string
		val := tag[eqIdx+1 : len(tag)]
		tags[key] = val // 存入 tag map

		tmpTagArr := make([]string, 2)
		tmpTagArr[0] = key
		tmpTagArr[1] = val
		tagArr = append(tagArr, tmpTagArr)

		/* 处理sf 如果有聚合函数，列名要用函数名，否则用sf中的列名*/
		columns := make([]string, 0)
		sf := "time[int64]," // sf中去掉了第一列的time，还原时要添上
		sf += messages[1][1 : len(messages[1])-1]
		sg := messages[3][1 : len(messages[3])-1]
		splitSg := strings.Split(sg, ",")
		aggr := splitSg[0]                       // 聚合函数名，小写的
		if strings.Compare(aggr, "empty") != 0 { // 聚合函数不为空，列名应该是聚合函数的名字
			columns = append(columns, "time")
			columns = append(columns, aggr)

			// time mean mean_1 mean_2 mean_3
			fields := strings.Split(sf, ",")
			if len(fields) > 2 {
				for j := 1; j < len(fields)-1; j++ {
					columns = append(columns, fmt.Sprintf("%s_%d", aggr, j))
				}
			}
		} else { // 没有聚合函数，用正常的列名
			fields := strings.Split(sf, ",") // time[int64],randtag[string]...
			for _, f := range fields {
				idx := strings.Index(f, "[") // "[" 前面的字符串是列名，后面的是数据类型
				columnName := f[:idx]
				columns = append(columns, columnName)
			}
		}

		/* 根据一条语义段构造一个 Series */
		seriesTmp := Series{
			Name:    name,
			Tags:    tags,
			Columns: columns,
			Values:  seriesValues[i],
			Partial: false,
		}

		/*  转换成 models.Row 数组 */
		row := SeriesToRow(seriesTmp)
		modelsRows = append(modelsRows, row)
	}

	/* 构造返回结果 */
	result := Result{
		StatementId: 0,
		Series:      modelsRows,
		Messages:    nil,
		Err:         "",
	}
	resp := Response{
		Results: []Result{result},
		Err:     "",
	}

	return &resp, flagNum, flagArr, timeRangeArr, tagArr
}
