package client

import (
	"fmt"
	"github.com/influxdata/influxql"
	"log"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode"
)

// GetInterval 获取 GROUP BY interval
func GetInterval(query string) string {
	parser := influxql.NewParser(strings.NewReader(query))
	stmt, _ := parser.ParseStatement()

	/* 获取 GROUP BY interval */
	s := stmt.(*influxql.SelectStatement)
	interval, err := s.GroupByInterval()
	if err != nil {
		log.Fatalln("GROUP BY INTERVAL ERROR")
	}

	//fmt.Println("GROUP BY interval:\t", interval.String()) // 12m0s

	if interval == 0 {
		return "empty"
	} else {
		//result := fmt.Sprintf("%dm", int(interval.Minutes()))
		//return result
		result := interval.String()
		for idx, ch := range result {
			if unicode.IsLetter(ch) {
				if (idx+1) < len(result) && result[idx+1] == '0' {
					return result[0 : idx+1]
				}
			}
		}

		return result
	}

}

// GroupByTags GROUP BY 后面的 tags 的所有值
func GroupByTags(queryString string, measurementName string) []string {
	matchStr := `(?i).+GROUP BY (.+)`
	conditionExpr := regexp.MustCompile(matchStr)
	if ok, _ := regexp.MatchString(matchStr, queryString); !ok { // 没有 GROUP BY
		return nil
	}
	condExprMatch := conditionExpr.FindStringSubmatch(queryString)
	parseExpr := condExprMatch[1]

	//values := make([]string, 0)
	totalTags := strings.Split(parseExpr, ",")
	tags := make([]string, 0)
	for _, tag := range totalTags {
		tag = strings.TrimSpace(tag)
		if strings.Contains(tag, "time") {
			continue
		}
		if strings.Contains(tag, "\"") { // 去掉双引号
			tag = tag[1 : len(tag)-1]
		}
		tags = append(tags, tag)
	}
	if len(tags) == 0 {
		return nil
	}
	slices.Sort(tags)

	return tags
}

// FieldsAndAggregation 列名 和 聚合函数名称
func FieldsAndAggregation(queryString string, measurementName string) (string, string) {
	var fields []string
	var FGstr string

	/* 用正则匹配从查询语句中取出包含聚合函数和列名的字符串  */
	regStr := `(?i)SELECT\s*(.+)\s*FROM.+`
	regExpr := regexp.MustCompile(regStr)

	if ok, _ := regexp.MatchString(regStr, queryString); ok {
		match := regExpr.FindStringSubmatch(queryString)
		FGstr = match[1] // fields and aggr
	} else {
		return "error", "error"
	}

	var aggr string
	singleField := strings.Split(FGstr, ",")
	if strings.IndexAny(singleField[0], "(") > 0 && strings.IndexAny(singleField[0], "*") < 0 { // 有一或多个聚合函数, 没有通配符 '*'
		/* 获取聚合函数名 */
		index := strings.IndexAny(singleField[0], "(")
		aggr = singleField[0][:index]
		aggr = strings.ToLower(aggr)

		/* 从查询语句获取field(实际的列名) */
		var startIdx int
		var endIdx int
		for i := range singleField {
			for idx, ch := range singleField[i] { // 括号中间的部分是fields，默认没有双引号，不作处理
				if ch == '(' {
					startIdx = idx + 1
				}
				if ch == ')' {
					endIdx = idx
				}
			}
			tmpStr := singleField[i][startIdx:endIdx]
			tmpArr := strings.Split(tmpStr, ",")
			for i := range tmpArr {
				tmpArr[i] = strings.TrimSpace(tmpArr[i])
			}
			fields = append(fields, tmpArr...)
		}

	} else if strings.IndexAny(singleField[0], "(") > 0 && strings.IndexAny(singleField[0], "*") >= 0 { // 有聚合函数，有通配符 '*'
		/* 获取聚合函数名 */
		index := strings.IndexAny(singleField[0], "(")
		aggr = singleField[0][:index]
		aggr = strings.ToLower(aggr)

		/* 获取列名 */
		fieldMap := Fields[measurementName]
		for key := range fieldMap {
			fields = append(fields, key)
		}
		sort.Strings(fields)
	} else if strings.IndexAny(singleField[0], "(") <= 0 && strings.IndexAny(singleField[0], "*") >= 0 { // 没有聚合函数，有通配符
		aggr = "empty"
		/* 获取列名 */
		fieldMap := Fields[measurementName]
		for key := range fieldMap {
			fields = append(fields, key)
		}
		tagMap := TagKV
		for _, tags := range tagMap.Measurement {
			for i := range tags {
				for tagkey, _ := range tags[i].Tag {
					fields = append(fields, tagkey)
				}
			}
		}
		sort.Strings(fields)
		fields = slices.Compact(fields)

	} else { // 没有聚合函数， 没有通配符
		aggr = "empty"
		for i := range singleField {
			singleField[i] = strings.TrimSpace(singleField[i])
		}
		fields = append(fields, singleField...)
	}
	//slices.Sort(fields)
	/* 获取每一列的数据类型 */
	fieldMap := Fields[measurementName]
	for i := range fields {
		datatype := fieldMap[fields[i]]
		if datatype == "" {
			datatype = "string"
		} else if datatype == "float" {
			datatype = "float64"
		} else if datatype == "integer" {
			datatype = "int64"
		}
		fields[i] = fmt.Sprintf("%s[%s]", fields[i], datatype)
	}

	var fieldsStr string
	fieldsStr = strings.Join(fields, ",")

	return fieldsStr, aggr
}

// preOrderTraverseBinaryExpr 遍历语法树，找出所有谓词表达式，去掉多余的空格，存入字符串数组
func preOrderTraverseBinaryExpr(node *influxql.BinaryExpr, tags *[]string, predicates *[]string, datatypes *[]string) (*[]string, *[]string, *[]string) {
	if node.Op != influxql.AND && node.Op != influxql.OR { // 不是由AND或OR连接的，说明表达式不可再分，存入结果数组
		str := node.String()
		//fmt.Println(node.LHS.String())
		// 用字符串获取每个二元表达式的数据类型	可能有问题，具体看怎么用
		if strings.Contains(str, "'") { // 有单引号的都是字符串
			*datatypes = append(*datatypes, "string")
		} else if strings.EqualFold(node.RHS.String(), "true") || strings.EqualFold(node.RHS.String(), "false") { // 忽略大小写，相等就是 bool
			*datatypes = append(*datatypes, "bool")
		} else if strings.Contains(str, ".") { // 带小数点就是 double
			*datatypes = append(*datatypes, "float64")
		} else { // 什么都没有是 int
			*datatypes = append(*datatypes, "int64")
		}

		tmpStr := strings.ReplaceAll(node.LHS.String(), "\"", "")
		*tags = append(*tags, tmpStr)
		str = strings.ReplaceAll(str, " ", "")  //去掉空格
		str = strings.ReplaceAll(str, "\"", "") //去掉双引号
		*predicates = append(*predicates, str)
		return tags, predicates, datatypes
	}

	if node.LHS != nil { //遍历左子树
		binaryExprL := getBinaryExpr(node.LHS.String())
		preOrderTraverseBinaryExpr(binaryExprL, tags, predicates, datatypes)
	} else {
		return tags, predicates, datatypes
	}

	if node.RHS != nil { //遍历右子树
		binaryExprR := getBinaryExpr(node.RHS.String())
		preOrderTraverseBinaryExpr(binaryExprR, tags, predicates, datatypes)
	} else {
		return tags, predicates, datatypes
	}

	return tags, predicates, datatypes
}

/*
字符串转化成二元表达式，用作遍历二叉树的节点
*/
func getBinaryExpr(str string) *influxql.BinaryExpr {
	now := time.Now()
	valuer := influxql.NowValuer{Now: now}
	parsedExpr, _ := influxql.ParseExpr(str)
	condExpr, _, _ := influxql.ConditionExpr(parsedExpr, &valuer)
	binaryExpr := condExpr.(*influxql.BinaryExpr)

	return binaryExpr
}

// PredicatesAndTagConditions 条件谓词，区分出 field 的谓词和 tag 的谓词
func PredicatesAndTagConditions(query string, measurement string, tagMap MeasurementTagMap) (string, []string) {
	//regStr := `(?i).+WHERE(.+)GROUP BY.`
	regStr := `(?i).+WHERE(.+)`
	conditionExpr := regexp.MustCompile(regStr)
	if ok, _ := regexp.MatchString(regStr, query); !ok {
		return "{empty}", nil
	}
	condExprMatch := conditionExpr.FindStringSubmatch(query) // 获取 WHERE 后面的所有表达式，包括谓词和时间范围
	parseExpr := condExprMatch[1]

	now := time.Now()
	valuer := influxql.NowValuer{Now: now}
	expr, _ := influxql.ParseExpr(parseExpr)
	cond, _, _ := influxql.ConditionExpr(expr, &valuer) //提取出谓词

	tagConds := make([]string, 0)
	var result string
	if cond == nil { //没有谓词
		result += fmt.Sprintf("{empty}")
	} else { //从语法树中找出由AND或OR连接的所有独立的谓词表达式
		var conds []string
		var tag []string
		binaryExpr := cond.(*influxql.BinaryExpr)
		var datatype []string

		tags, predicates, datatypes := preOrderTraverseBinaryExpr(binaryExpr, &tag, &conds, &datatype)
		result += "{"
		for i, p := range *predicates {
			isTag := false
			found := false
			for _, t := range tagMap.Measurement[measurement] {
				for tagkey, _ := range t.Tag {
					if (*tags)[i] == tagkey {
						isTag = true
						found = true
						break
					}
				}
				if found {
					break
				}
			}

			if !isTag {
				result += fmt.Sprintf("(%s[%s])", p, (*datatypes)[i])
			} else {
				p = strings.ReplaceAll(p, "'", "")
				tagConds = append(tagConds, p)
			}
		}
		result += "}"
	}

	if len(result) == 2 {
		result = "{empty}"
	}

	sort.Strings(tagConds)
	return result, tagConds
}

// MeasurementName 度量名称
func MeasurementName(queryString string) string {
	regStr := `(?i)FROM(.+)WHERE`
	conditionExpr := regexp.MustCompile(regStr)
	if ok, _ := regexp.MatchString(regStr, queryString); !ok {
		return ""
	}
	condExprMatch := conditionExpr.FindStringSubmatch(queryString) // 获取 WHERE 后面的所有表达式，包括谓词和时间范围
	parseExpr := condExprMatch[1]

	trimStr := strings.TrimSpace(parseExpr)
	//if strings.Contains(trimStr, "\"") { // 去掉双引号
	//	trimStr = trimStr[1 : len(trimStr)-1]
	//}
	trimStr = strings.ReplaceAll(trimStr, "\"", "")
	splitIndex := strings.LastIndex(trimStr, ".") + 1
	measurementName := trimStr[splitIndex:]

	return measurementName
}

var combinations []string

func combinationTagValues(allTagStr [][]string) []string {
	if len(allTagStr) == 0 {
		return []string{}
	}
	combinations = []string{}
	backtrace(allTagStr, 0, "")
	slices.Sort(combinations)
	return combinations
}

func backtrace(allTagStr [][]string, index int, combination string) {
	if index == len(allTagStr) {
		combinations = append(combinations, combination)
	} else {
		tagStr := allTagStr[index]
		valCounts := len(tagStr)
		for i := 0; i < valCounts; i++ {
			backtrace(allTagStr, index+1, combination+","+string(tagStr[i]))
		}
	}
}

// IntegratedSM 重构的构造 SM 字段的方法
func IntegratedSM(measurementName string, tagConds []string, tags []string) string {
	result := ""

	tagValues := make(map[string][]string)
	tagPre := make([]string, 0) // 谓词 tag
	// 谓词 tag 处理
	for i := range tagConds {
		var idx int
		if idx = strings.Index(tagConds[i], "!"); idx < 0 { // "!="
			idx = strings.Index(tagConds[i], "=") // "="
		}
		tagName := tagConds[i][:idx]
		tagPre = append(tagPre, tagName)
	}
	// GROUP BY tag 处理
	values := make([]string, 0)
	for _, tag := range tags {
		if !slices.Contains(tagPre, tag) { // GROUP BY 独有的 tag，获得其所有值
			for _, tagMap := range TagKV.Measurement[measurementName] {
				if len(tagMap.Tag[tag].Values) != 0 {
					values = tagMap.Tag[tag].Values
					for _, val := range values {
						tmpTagValues := fmt.Sprintf("%s=%s", tag, val)
						tagValues[tag] = append(tagValues[tag], tmpTagValues)
					}
					break
				}
			}
		}
	}

	keys := make([]string, 0)
	for key := range tagValues {
		keys = append(keys, key)
	}
	slices.Sort(keys)

	table_num := 1 // 结果中的子表数量
	allTagStr := make([][]string, 0)
	for _, key := range keys {
		val := tagValues[key]
		table_num *= len(val)
		tagStr := make([]string, 0)
		for _, v := range val {
			tmp := fmt.Sprintf("%s.%s", measurementName, v)
			tagStr = append(tagStr, tmp)
		}
		allTagStr = append(allTagStr, tagStr)
	}
	groupByTags := combinationTagValues(allTagStr)
	result += "{"
	//把两种 tag 组合成 SM 字段
	if len(tagConds) > 0 && len(groupByTags) > 0 {
		for i := range tagConds {
			for j := range groupByTags {
				tmp := ""
				if strings.Compare(tagConds[i], groupByTags[j]) >= 0 {
					tmp = fmt.Sprintf("%s.%s%s", measurementName, tagConds[i], groupByTags[j])
				} else {
					groupByTags[j] = groupByTags[j][1:len(groupByTags[j])]
					groupByTags[j] += ","
					tmp = fmt.Sprintf("%s%s.%s", groupByTags[j], measurementName, tagConds[i])
				}
				result += fmt.Sprintf("(%s)", tmp)
			}
		}
	} else if len(tagConds) == 0 && len(groupByTags) > 0 {
		for j := range groupByTags {
			tmp := fmt.Sprintf("%s", groupByTags[j])
			tmp = tmp[1:len(tmp)]
			result += fmt.Sprintf("(%s)", tmp)
		}
	} else if len(tagConds) > 0 && len(groupByTags) == 0 {
		//result += "("
		//tmp := ""
		//for i := range tagConds {
		//	tmp = fmt.Sprintf("%s.%s,", measurementName, tagConds[i])
		//	result += fmt.Sprintf("%s", tmp)
		//}
		//result = result[:len(result)-1]
		//result += ")"
		for i := range tagConds {
			tmp := fmt.Sprintf("(%s.%s)", measurementName, tagConds[i])
			result += fmt.Sprintf("%s", tmp)
		}
	} else {
		result += fmt.Sprintf("(%s.empty)", measurementName)
	}

	result += "}"
	return result
}

func SeperateSM(integratedSM string) []string {
	integratedSM = integratedSM[1 : len(integratedSM)-1] // 去掉大括号
	sepSM := strings.Split(integratedSM, ")")
	sepSM = sepSM[:len(sepSM)-1] // 分割之后数组末尾会多一个空串
	for i := range sepSM {
		sepSM[i] = sepSM[i][1:]
	}
	return sepSM
}

//var mu5 sync.Mutex

// GetSeperateSemanticSegment 获取每张子表的语义段
func GetSeperateSemanticSegment(queryString string) []string {
	results := make([]string, 0)

	//mu5.Lock()
	queryTemplate := GetQueryTemplate(queryString)
	semanticSegment := ""
	if ss, ok := QueryTemplates[queryTemplate]; !ok { // 查询模版中不存在该查询

		semanticSegment = GetSemanticSegment(queryString)
		QueryTemplates[queryTemplate] = semanticSegment
		idx := strings.Index(semanticSegment, "}")
		integratedSM := semanticSegment[:idx+1]
		commonFields := semanticSegment[idx+1:]

		sepSM := SeperateSM(integratedSM)

		for i := range sepSM {
			tmp := fmt.Sprintf("{(%s)}%s", sepSM[i], commonFields)
			results = append(results, tmp)
		}

		SeprateSegments[semanticSegment] = results
		//mu5.Unlock()
		return results
	} else {
		semanticSegment = ss

		if sepseg, ok := SeprateSegments[semanticSegment]; !ok {
			idx := strings.Index(semanticSegment, "}")
			integratedSM := semanticSegment[:idx+1]
			commonFields := semanticSegment[idx+1:]

			sepSM := SeperateSM(integratedSM)

			for i := range sepSM {
				tmp := fmt.Sprintf("{(%s)}%s", sepSM[i], commonFields)
				results = append(results, tmp)
			}

			SeprateSegments[semanticSegment] = results
			//mu5.Unlock()
			return results
		} else {
			//mu5.Unlock()
			return sepseg
		}

	}

}

func GetSeparateSemanticSegmentWithNullTag(seperateSemanticSegment string, nullTags []string) string {
	if len(nullTags) == 0 {
		return ""
	}
	nullSegment := seperateSemanticSegment

	splitSlices := strings.Split(seperateSemanticSegment, nullTags[0])
	startIndex := strings.Index(splitSlices[1], "=") + 1
	branketIndex := strings.Index(splitSlices[1], "}")
	endIndex := strings.Index(splitSlices[1], ",")
	if endIndex < 0 || endIndex > branketIndex {
		endIndex = strings.Index(splitSlices[1], ")")
	}

	splitSlices[1] = strings.Replace(splitSlices[1], splitSlices[1][startIndex:endIndex], "null", 1)
	nullSegment = splitSlices[0] + nullTags[0] + splitSlices[1]

	return nullSegment
}

// GetSemanticSegment 重构根据查询语句生成语义段的功能
func GetSemanticSegment(queryString string) string {
	result := ""

	measurement := MeasurementName(queryString)
	SP, tagConds := PredicatesAndTagConditions(queryString, measurement, TagKV)
	fields, aggr := FieldsAndAggregation(queryString, measurement)
	tags := GroupByTags(queryString, measurement)
	interval := GetInterval(queryString)
	SM := IntegratedSM(measurement, tagConds, tags)

	result = fmt.Sprintf("%s#{%s}#%s#{%s,%s}", SM, fields, SP, aggr, interval)

	return result
}

// GetSemanticSegment 重构根据查询语句生成语义段的功能
func GetSemanticSegmentAndFields(queryString string) (string, string) {
	result := ""

	measurement := MeasurementName(queryString)
	SP, tagConds := PredicatesAndTagConditions(queryString, measurement, TagKV)
	fields, aggr := FieldsAndAggregation(queryString, measurement)
	tags := GroupByTags(queryString, measurement)
	interval := GetInterval(queryString)
	SM := IntegratedSM(measurement, tagConds, tags)

	result = fmt.Sprintf("%s#{%s}#%s#{%s,%s}", SM, fields, SP, aggr, interval)

	return result, fields
}
