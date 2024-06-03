package client

import (
	"fmt"
	"strings"
	"testing"
)

func TestTSCacheResponseToByteArray(t *testing.T) {
	queryString := `SELECT latitude,longitude,elevation,velocity FROM "readings" WHERE "name"='truck_0' or "name"='truck_1' or "name"='truck_10' AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T00:01:10Z' GROUP BY "name"`

	urlString := "192.168.1.101:11211"
	urlArr := strings.Split(urlString, ",")
	conns := InitStsConnsArr(urlArr)
	fmt.Printf("number of conns:%d\n", len(conns))
	TagKV = GetTagKV(c, "iot_small")
	Fields = GetFieldKeys(c, "iot_small")

	datatypes := make([]string, 0)
	datatypes = append(datatypes, "int64")
	_, fields := GetSemanticSegmentAndFields(queryString)
	datatypes = append(datatypes, GetDataTypeArrayFromSF(fields)...)

	query := NewQuery(queryString, "iot_small", "s")
	resp, _ := c.Query(query)

	fmt.Println()
	fmt.Println(resp)
	fmt.Printf("\toriginal response:\n%s\n\n", resp.ToString())

	convertedBytes := TSCacheResponseToByteArray(resp, queryString)
	fmt.Println(convertedBytes)

	convertedResp, _, _, _, _ := TSSCacheByteArrayToResponse(convertedBytes, datatypes)

	fmt.Println(convertedResp)

	fmt.Printf("\tconverted response:\n%s\n\n", convertedResp.ToString())

}

func TestTSSCacheByteArrayToResponse(t *testing.T) {
	tests := []struct {
		name        string
		queryString string
		expected    string
	}{
		{
			name:        "1",
			queryString: `SELECT latitude,longitude,elevation,velocity FROM "readings" WHERE "name"='truck_0' or "name"='truck_1' or "name"='truck_10' AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T00:01:10Z' GROUP BY "name"`,
			expected:    "",
		},
		{
			name:        "2",
			queryString: `SELECT latitude FROM "readings" WHERE "name"='truck_10' AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T00:01:00Z' GROUP BY "name"`,
			expected:    "",
		},
		{
			name:        "3",
			queryString: `SELECT mean(latitude),mean(longitude),mean(elevation),mean(velocity) FROM "readings" WHERE "name"='truck_0' or "name"='truck_1' or "name"='truck_10' AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T01:00:00Z' GROUP BY "name",time(10m)`,
			expected:    "",
		},
		{
			name:        "4",
			queryString: `SELECT mean(latitude),mean(longitude),mean(elevation) FROM "readings" WHERE "name"='truck_0' or "name"='truck_1' or "name"='truck_20' AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T00:10:00Z' GROUP BY "name",time(1m)`,
			expected:    "",
		},
		{
			name:        "5",
			queryString: `SELECT mean(current_load),mean(fuel_state) FROM "diagnostics" WHERE "name"='truck_0' or "name"='truck_1' or "name"='truck_12' AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T01:10:00Z' GROUP BY "name",time(10m)`,
			expected:    "",
		},
		{
			name:        "6",
			queryString: `SELECT mean(velocity),mean(fuel_consumption),mean(grade) FROM "readings" WHERE "name"='truck_0' or "name"='truck_12' AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T01:10:00Z' GROUP BY "name",time(10m)`,
			expected:    "",
		},
		{
			name:        "7",
			queryString: `SELECT mean(velocity),mean(fuel_consumption) FROM "readings" WHERE "name"='truck_1' or "name"='truck_50' or "name"='truck_99' AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T01:10:00Z' GROUP BY "name",time(10m)`,
			expected:    "",
		},
		{
			name:        "8",
			queryString: `SELECT mean(velocity),mean(fuel_consumption),mean(grade) FROM "readings" WHERE "name"='truck_0' or "name"='truck_1' or "name"='truck_2' or "name"='truck_10' or "name"='truck_12' or "name"='truck_22' or "name"='truck_32' or "name"='truck_62' or "name"='truck_92' AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-02T00:00:00Z' GROUP BY "name",time(1h)`,
			expected:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			urlString := "192.168.1.101:11211"
			urlArr := strings.Split(urlString, ",")
			conns := InitStsConnsArr(urlArr)
			fmt.Printf("number of conns:%d\n", len(conns))
			TagKV = GetTagKV(c, "iot_small")
			Fields = GetFieldKeys(c, "iot_small")

			datatypes := make([]string, 0)
			datatypes = append(datatypes, "int64")
			_, fields := GetSemanticSegmentAndFields(tt.queryString)
			datatypes = append(datatypes, GetDataTypeArrayFromSF(fields)...)

			query := NewQuery(tt.queryString, "iot_small", "s")
			resp, _ := c.Query(query)

			fmt.Println()
			fmt.Println(resp)
			fmt.Printf("\n\toriginal response:\n%s\n\n", resp.ToString())

			convertedBytes := TSCacheResponseToByteArray(resp, tt.queryString)
			fmt.Println(convertedBytes)

			convertedResp, _, _, _, _ := TSSCacheByteArrayToResponse(convertedBytes, datatypes)

			fmt.Println(convertedResp)

			fmt.Printf("\n\tconverted response:\n%s\n\n", convertedResp.ToString())

		})
	}

}
