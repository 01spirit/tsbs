package client

import (
	"encoding/json"
	"errors"
	"fmt"
	stscache "github.com/timescale/tsbs/InfluxDB-client/memcache"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestUDPClient_Query(t *testing.T) {
	config := UDPConfig{Addr: "localhost:8089"}
	c, err := NewUDPClient(config)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
	defer c.Close()
	query := Query{}
	_, err = c.Query(query)
	if err == nil {
		t.Error("Querying UDP client should fail")
	}
}

func TestUDPClient_Ping(t *testing.T) {
	config := UDPConfig{Addr: "localhost:8089"}
	c, err := NewUDPClient(config)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
	defer c.Close()

	rtt, version, err := c.Ping(0)
	if rtt != 0 || version != "" || err != nil {
		t.Errorf("unexpected error.  expected (%v, '%v', %v), actual (%v, '%v', %v)", 0, "", nil, rtt, version, err)
	}
}

func TestUDPClient_Write(t *testing.T) {
	config := UDPConfig{Addr: "localhost:8089"}
	c, err := NewUDPClient(config)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
	defer c.Close()

	bp, err := NewBatchPoints(BatchPointsConfig{})
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}

	fields := make(map[string]interface{})
	fields["value"] = 1.0
	pt, _ := NewPoint("cpu", make(map[string]string), fields)
	bp.AddPoint(pt)

	err = c.Write(bp)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
}

func TestUDPClient_BadAddr(t *testing.T) {
	config := UDPConfig{Addr: "foobar@wahoo"}
	c, err := NewUDPClient(config)
	if err == nil {
		defer c.Close()
		t.Error("Expected resolve error")
	}
}

func TestUDPClient_Batches(t *testing.T) {
	var logger writeLogger
	var cl udpclient

	cl.conn = &logger
	cl.payloadSize = 20 // should allow for two points per batch

	// expected point should look like this: "cpu a=1i"
	fields := map[string]interface{}{"a": 1}

	p, _ := NewPoint("cpu", nil, fields, time.Time{})

	bp, _ := NewBatchPoints(BatchPointsConfig{})

	for i := 0; i < 9; i++ {
		bp.AddPoint(p)
	}

	if err := cl.Write(bp); err != nil {
		t.Fatalf("Unexpected error during Write: %v", err)
	}

	if len(logger.writes) != 5 {
		t.Errorf("Mismatched write count: got %v, exp %v", len(logger.writes), 5)
	}
}

func TestUDPClient_Split(t *testing.T) {
	var logger writeLogger
	var cl udpclient

	cl.conn = &logger
	cl.payloadSize = 1 // force one field per point

	fields := map[string]interface{}{"a": 1, "b": 2, "c": 3, "d": 4}

	p, _ := NewPoint("cpu", nil, fields, time.Unix(1, 0))

	bp, _ := NewBatchPoints(BatchPointsConfig{})

	bp.AddPoint(p)

	if err := cl.Write(bp); err != nil {
		t.Fatalf("Unexpected error during Write: %v", err)
	}

	if len(logger.writes) != len(fields) {
		t.Errorf("Mismatched write count: got %v, exp %v", len(logger.writes), len(fields))
	}
}

type writeLogger struct {
	writes [][]byte
}

func (w *writeLogger) Write(b []byte) (int, error) {
	w.writes = append(w.writes, append([]byte(nil), b...))
	return len(b), nil
}

func (w *writeLogger) Close() error { return nil }

func TestClient_Query(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data Response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	query := Query{}
	_, err := c.Query(query)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
}

func TestClient_QueryWithRP(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		params := r.URL.Query()
		if got, exp := params.Get("db"), "db0"; got != exp {
			t.Errorf("unexpected db query parameter: %s != %s", exp, got)
		}
		if got, exp := params.Get("rp"), "rp0"; got != exp {
			t.Errorf("unexpected rp query parameter: %s != %s", exp, got)
		}
		var data Response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	query := NewQueryWithRP("SELECT * FROM m0", "db0", "rp0", "")
	_, err := c.Query(query)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
}

func TestClientDownstream500WithBody_Query(t *testing.T) {
	const err500page = `<html>
	<head>
		<title>500 Internal Server Error</title>
	</head>
	<body>Internal Server Error</body>
</html>`
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err500page))
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	query := Query{}
	_, err := c.Query(query)

	expected := fmt.Sprintf("received status code 500 from downstream server, with response body: %q", err500page)
	if err.Error() != expected {
		t.Errorf("unexpected error.  expected %v, actual %v", expected, err)
	}
}

func TestClientDownstream500_Query(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	query := Query{}
	_, err := c.Query(query)

	expected := "received status code 500 from downstream server"
	if err.Error() != expected {
		t.Errorf("unexpected error.  expected %v, actual %v", expected, err)
	}
}

func TestClientDownstream400WithBody_Query(t *testing.T) {
	const err403page = `<html>
	<head>
		<title>403 Forbidden</title>
	</head>
	<body>Forbidden</body>
</html>`
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(err403page))
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	query := Query{}
	_, err := c.Query(query)

	expected := fmt.Sprintf(`expected json response, got "text/html", with status: %v and response body: %q`, http.StatusForbidden, err403page)
	if err.Error() != expected {
		t.Errorf("unexpected error.  expected %v, actual %v", expected, err)
	}
}

func TestClientDownstream400_Query(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	query := Query{}
	_, err := c.Query(query)

	expected := fmt.Sprintf(`expected json response, got empty body, with status: %v`, http.StatusForbidden)
	if err.Error() != expected {
		t.Errorf("unexpected error.  expected %v, actual %v", expected, err)
	}
}

func TestClient500_Query(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Influxdb-Version", "1.3.1")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"test"}`))
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	query := Query{}
	resp, err := c.Query(query)

	if err != nil {
		t.Errorf("unexpected error.  expected nothing, actual %v", err)
	}

	if resp.Err != "test" {
		t.Errorf(`unexpected response error.  expected "test", actual %v`, resp.Err)
	}
}

func TestClient_ChunkedQuery(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data Response
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Influxdb-Version", "1.3.1")
		w.WriteHeader(http.StatusOK)
		enc := json.NewEncoder(w)
		_ = enc.Encode(data)
		_ = enc.Encode(data)
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, err := NewHTTPClient(config)
	if err != nil {
		t.Fatalf("unexpected error.  expected %v, actual %v", nil, err)
	}

	query := Query{Chunked: true}
	_, err = c.Query(query)
	if err != nil {
		t.Fatalf("unexpected error.  expected %v, actual %v", nil, err)
	}
}

func TestClientDownstream500WithBody_ChunkedQuery(t *testing.T) {
	const err500page = `<html>
	<head>
		<title>500 Internal Server Error</title>
	</head>
	<body>Internal Server Error</body>
</html>`
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err500page))
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, err := NewHTTPClient(config)
	if err != nil {
		t.Fatalf("unexpected error.  expected %v, actual %v", nil, err)
	}

	query := Query{Chunked: true}
	_, err = c.Query(query)

	expected := fmt.Sprintf("received status code 500 from downstream server, with response body: %q", err500page)
	if err.Error() != expected {
		t.Errorf("unexpected error.  expected %v, actual %v", expected, err)
	}
}

func TestClientDownstream500_ChunkedQuery(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	query := Query{Chunked: true}
	_, err := c.Query(query)

	expected := "received status code 500 from downstream server"
	if err.Error() != expected {
		t.Errorf("unexpected error.  expected %v, actual %v", expected, err)
	}
}

func TestClient500_ChunkedQuery(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Influxdb-Version", "1.3.1")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"test"}`))
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	query := Query{Chunked: true}
	resp, err := c.Query(query)

	if err != nil {
		t.Errorf("unexpected error.  expected nothing, actual %v", err)
	}

	if resp.Err != "test" {
		t.Errorf(`unexpected response error.  expected "test", actual %v`, resp.Err)
	}
}

func TestClientDownstream400WithBody_ChunkedQuery(t *testing.T) {
	const err403page = `<html>
	<head>
		<title>403 Forbidden</title>
	</head>
	<body>Forbidden</body>
</html>`
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(err403page))
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	query := Query{Chunked: true}
	_, err := c.Query(query)

	expected := fmt.Sprintf(`expected json response, got "text/html", with status: %v and response body: %q`, http.StatusForbidden, err403page)
	if err.Error() != expected {
		t.Errorf("unexpected error.  expected %v, actual %v", expected, err)
	}
}

func TestClientDownstream400_ChunkedQuery(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	query := Query{Chunked: true}
	_, err := c.Query(query)

	expected := fmt.Sprintf(`expected json response, got empty body, with status: %v`, http.StatusForbidden)
	if err.Error() != expected {
		t.Errorf("unexpected error.  expected %v, actual %v", expected, err)
	}
}

func TestClient_BoundParameters(t *testing.T) {
	var parameterString string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data Response
		r.ParseForm()
		parameterString = r.FormValue("params")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	expectedParameters := map[string]interface{}{
		"testStringParameter": "testStringValue",
		"testNumberParameter": 12.3,
	}

	query := Query{
		Parameters: expectedParameters,
	}

	_, err := c.Query(query)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}

	var actualParameters map[string]interface{}

	err = json.Unmarshal([]byte(parameterString), &actualParameters)
	if err != nil {
		t.Errorf("unexpected error. expected %v, actual %v", nil, err)
	}

	if !reflect.DeepEqual(expectedParameters, actualParameters) {
		t.Errorf("unexpected parameters. expected %v, actual %v", expectedParameters, actualParameters)
	}
}

func TestClient_BasicAuth(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()

		if !ok {
			t.Errorf("basic auth error")
		}
		if u != "username" {
			t.Errorf("unexpected username, expected %q, actual %q", "username", u)
		}
		if p != "password" {
			t.Errorf("unexpected password, expected %q, actual %q", "password", p)
		}
		var data Response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL, Username: "username", Password: "password"}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	query := Query{}
	_, err := c.Query(query)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
}

func TestClient_Ping(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data Response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNoContent)
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	_, _, err := c.Ping(0)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
}

func TestClient_Concurrent_Use(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	var wg sync.WaitGroup
	wg.Add(3)
	n := 1000

	errC := make(chan error)
	go func() {
		defer wg.Done()
		bp, err := NewBatchPoints(BatchPointsConfig{})
		if err != nil {
			errC <- fmt.Errorf("got error %v", err)
			return
		}

		for i := 0; i < n; i++ {
			if err = c.Write(bp); err != nil {
				errC <- fmt.Errorf("got error %v", err)
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		var q Query
		for i := 0; i < n; i++ {
			if _, err := c.Query(q); err != nil {
				errC <- fmt.Errorf("got error %v", err)
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			c.Ping(time.Second)
		}
	}()

	go func() {
		wg.Wait()
		close(errC)
	}()

	for err := range errC {
		if err != nil {
			t.Error(err)
		}
	}
}

func TestClient_Write(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		in, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		} else if have, want := strings.TrimSpace(string(in)), `m0,host=server01 v1=2,v2=2i,v3=2u,v4="foobar",v5=true 0`; have != want {
			t.Errorf("unexpected write protocol: %s != %s", have, want)
		}
		var data Response
		w.WriteHeader(http.StatusNoContent)
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	bp, err := NewBatchPoints(BatchPointsConfig{})
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
	pt, err := NewPoint(
		"m0",
		map[string]string{
			"host": "server01",
		},
		map[string]interface{}{
			"v1": float64(2),
			"v2": int64(2),
			"v3": uint64(2),
			"v4": "foobar",
			"v5": true,
		},
		time.Unix(0, 0).UTC(),
	)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
	bp.AddPoint(pt)
	err = c.Write(bp)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
}

func TestClient_UserAgent(t *testing.T) {
	receivedUserAgent := ""
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedUserAgent = r.UserAgent()

		var data Response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer ts.Close()

	_, err := http.Get(ts.URL)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}

	tests := []struct {
		name      string
		userAgent string
		expected  string
	}{
		{
			name:      "Empty user agent",
			userAgent: "",
			expected:  "InfluxDBClient",
		},
		{
			name:      "Custom user agent",
			userAgent: "Test Influx Client",
			expected:  "Test Influx Client",
		},
	}

	for _, test := range tests {

		config := HTTPConfig{Addr: ts.URL, UserAgent: test.userAgent}
		c, _ := NewHTTPClient(config)
		defer c.Close()

		receivedUserAgent = ""
		query := Query{}
		_, err = c.Query(query)
		if err != nil {
			t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
		}
		if !strings.HasPrefix(receivedUserAgent, test.expected) {
			t.Errorf("Unexpected user agent. expected %v, actual %v", test.expected, receivedUserAgent)
		}

		receivedUserAgent = ""
		bp, _ := NewBatchPoints(BatchPointsConfig{})
		err = c.Write(bp)
		if err != nil {
			t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
		}
		if !strings.HasPrefix(receivedUserAgent, test.expected) {
			t.Errorf("Unexpected user agent. expected %v, actual %v", test.expected, receivedUserAgent)
		}

		receivedUserAgent = ""
		_, err := c.Query(query)
		if err != nil {
			t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
		}
		if receivedUserAgent != test.expected {
			t.Errorf("Unexpected user agent. expected %v, actual %v", test.expected, receivedUserAgent)
		}
	}
}

func TestClient_PointString(t *testing.T) {
	const shortForm = "2006-Jan-02"
	time1, _ := time.Parse(shortForm, "2013-Feb-03")
	tags := map[string]string{"cpu": "cpu-total"}
	fields := map[string]interface{}{"idle": 10.1, "system": 50.9, "user": 39.0}
	p, _ := NewPoint("cpu_usage", tags, fields, time1)

	s := "cpu_usage,cpu=cpu-total idle=10.1,system=50.9,user=39 1359849600000000000"
	if p.String() != s {
		t.Errorf("Point String Error, got %s, expected %s", p.String(), s)
	}

	s = "cpu_usage,cpu=cpu-total idle=10.1,system=50.9,user=39 1359849600000"
	if p.PrecisionString("ms") != s {
		t.Errorf("Point String Error, got %s, expected %s",
			p.PrecisionString("ms"), s)
	}
}

func TestClient_PointWithoutTimeString(t *testing.T) {
	tags := map[string]string{"cpu": "cpu-total"}
	fields := map[string]interface{}{"idle": 10.1, "system": 50.9, "user": 39.0}
	p, _ := NewPoint("cpu_usage", tags, fields)

	s := "cpu_usage,cpu=cpu-total idle=10.1,system=50.9,user=39"
	if p.String() != s {
		t.Errorf("Point String Error, got %s, expected %s", p.String(), s)
	}

	if p.PrecisionString("ms") != s {
		t.Errorf("Point String Error, got %s, expected %s",
			p.PrecisionString("ms"), s)
	}
}

func TestClient_PointName(t *testing.T) {
	tags := map[string]string{"cpu": "cpu-total"}
	fields := map[string]interface{}{"idle": 10.1, "system": 50.9, "user": 39.0}
	p, _ := NewPoint("cpu_usage", tags, fields)

	exp := "cpu_usage"
	if p.Name() != exp {
		t.Errorf("Error, got %s, expected %s",
			p.Name(), exp)
	}
}

func TestClient_PointTags(t *testing.T) {
	tags := map[string]string{"cpu": "cpu-total"}
	fields := map[string]interface{}{"idle": 10.1, "system": 50.9, "user": 39.0}
	p, _ := NewPoint("cpu_usage", tags, fields)

	if !reflect.DeepEqual(tags, p.Tags()) {
		t.Errorf("Error, got %v, expected %v",
			p.Tags(), tags)
	}
}

func TestClient_PointUnixNano(t *testing.T) {
	const shortForm = "2006-Jan-02"
	time1, _ := time.Parse(shortForm, "2013-Feb-03")
	tags := map[string]string{"cpu": "cpu-total"}
	fields := map[string]interface{}{"idle": 10.1, "system": 50.9, "user": 39.0}
	p, _ := NewPoint("cpu_usage", tags, fields, time1)

	exp := int64(1359849600000000000)
	if p.UnixNano() != exp {
		t.Errorf("Error, got %d, expected %d",
			p.UnixNano(), exp)
	}
}

func TestClient_PointFields(t *testing.T) {
	tags := map[string]string{"cpu": "cpu-total"}
	fields := map[string]interface{}{"idle": 10.1, "system": 50.9, "user": 39.0}
	p, _ := NewPoint("cpu_usage", tags, fields)

	pfields, err := p.Fields()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fields, pfields) {
		t.Errorf("Error, got %v, expected %v",
			pfields, fields)
	}
}

func TestBatchPoints_PrecisionError(t *testing.T) {
	_, err := NewBatchPoints(BatchPointsConfig{Precision: "foobar"})
	if err == nil {
		t.Errorf("Precision: foobar should have errored")
	}

	bp, _ := NewBatchPoints(BatchPointsConfig{Precision: "ns"})
	err = bp.SetPrecision("foobar")
	if err == nil {
		t.Errorf("Precision: foobar should have errored")
	}
}

func TestBatchPoints_SettersGetters(t *testing.T) {
	bp, _ := NewBatchPoints(BatchPointsConfig{
		Precision:        "ns",
		Database:         "db",
		RetentionPolicy:  "rp",
		WriteConsistency: "wc",
	})
	if bp.Precision() != "ns" {
		t.Errorf("Expected: %s, got %s", bp.Precision(), "ns")
	}
	if bp.Database() != "db" {
		t.Errorf("Expected: %s, got %s", bp.Database(), "db")
	}
	if bp.RetentionPolicy() != "rp" {
		t.Errorf("Expected: %s, got %s", bp.RetentionPolicy(), "rp")
	}
	if bp.WriteConsistency() != "wc" {
		t.Errorf("Expected: %s, got %s", bp.WriteConsistency(), "wc")
	}

	bp.SetDatabase("db2")
	bp.SetRetentionPolicy("rp2")
	bp.SetWriteConsistency("wc2")
	err := bp.SetPrecision("s")
	if err != nil {
		t.Errorf("Did not expect error: %s", err.Error())
	}

	if bp.Precision() != "s" {
		t.Errorf("Expected: %s, got %s", bp.Precision(), "s")
	}
	if bp.Database() != "db2" {
		t.Errorf("Expected: %s, got %s", bp.Database(), "db2")
	}
	if bp.RetentionPolicy() != "rp2" {
		t.Errorf("Expected: %s, got %s", bp.RetentionPolicy(), "rp2")
	}
	if bp.WriteConsistency() != "wc2" {
		t.Errorf("Expected: %s, got %s", bp.WriteConsistency(), "wc2")
	}
}

func TestClientConcatURLPath(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.String(), "/influxdbproxy/ping") || strings.Contains(r.URL.String(), "/ping/ping") {
			t.Errorf("unexpected error.  expected %v contains in %v", "/influxdbproxy/ping", r.URL)
		}
		var data Response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNoContent)
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer ts.Close()

	url, _ := url.Parse(ts.URL)
	url.Path = path.Join(url.Path, "influxdbproxy")

	fmt.Println("TestClientConcatURLPath: concat with path 'influxdbproxy' result ", url.String())

	c, _ := NewHTTPClient(HTTPConfig{Addr: url.String()})
	defer c.Close()

	_, _, err := c.Ping(0)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}

	_, _, err = c.Ping(0)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
}

func TestClientProxy(t *testing.T) {
	pinged := false
	ts := httptest.NewServer(http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
		if got, want := req.URL.String(), "http://example.com:8086/ping"; got != want {
			t.Errorf("invalid url in request: got=%s want=%s", got, want)
		}
		resp.WriteHeader(http.StatusNoContent)
		pinged = true
	}))
	defer ts.Close()

	proxyURL, _ := url.Parse(ts.URL)
	c, _ := NewHTTPClient(HTTPConfig{
		Addr:  "http://example.com:8086",
		Proxy: http.ProxyURL(proxyURL),
	})
	if _, _, err := c.Ping(0); err != nil {
		t.Fatalf("could not ping server: %s", err)
	}

	if !pinged {
		t.Fatalf("no http request was received")
	}
}

func TestClient_QueryAsChunk(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data Response
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Influxdb-Version", "1.3.1")
		w.WriteHeader(http.StatusOK)
		enc := json.NewEncoder(w)
		_ = enc.Encode(data)
		_ = enc.Encode(data)
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, err := NewHTTPClient(config)
	if err != nil {
		t.Fatalf("unexpected error.  expected %v, actual %v", nil, err)
	}

	query := Query{Chunked: true}
	resp, err := c.QueryAsChunk(query)
	defer resp.Close()
	if err != nil {
		t.Fatalf("unexpected error.  expected %v, actual %v", nil, err)
	}
}

func TestClient_ReadStatementId(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data := Response{
			Results: []Result{{
				StatementId: 1,
				Series:      nil,
				Messages:    nil,
				Err:         "",
			}},
			Err: "",
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Influxdb-Version", "1.3.1")
		w.WriteHeader(http.StatusOK)
		enc := json.NewEncoder(w)
		_ = enc.Encode(data)
		_ = enc.Encode(data)
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, err := NewHTTPClient(config)
	if err != nil {
		t.Fatalf("unexpected error.  expected %v, actual %v", nil, err)
	}

	query := Query{Chunked: true}
	resp, err := c.QueryAsChunk(query)
	defer resp.Close()
	if err != nil {
		t.Fatalf("unexpected error.  expected %v, actual %v", nil, err)
	}

	r, err := resp.NextResponse()

	if err != nil {
		t.Fatalf("expected success, got %s", err)
	}

	if r.Results[0].StatementId != 1 {
		t.Fatalf("expected statement_id = 1, got %d", r.Results[0].StatementId)
	}
}

func TestRepeatSetToStscache(t *testing.T) {
	query1 := `select usage_system,usage_user,usage_guest,usage_nice,usage_guest_nice from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:00:20Z' and hostname='host_0'`
	query2 := `select usage_system,usage_user,usage_guest,usage_nice,usage_guest_nice from test..cpu where time >= '2022-01-01T00:00:20Z' and time < '2022-01-01T00:00:40Z' and hostname='host_0'`

	q1 := NewQuery(query1, DB, "s")
	resp1, _ := c.Query(q1)
	startTime, endTime := GetResponseTimeRange(resp1)
	numOfTab := GetNumOfTable(resp1)

	semanticSegment := GetSemanticSegment(query1)
	respCacheByte := ResponseToByteArray(resp1, query1)
	fmt.Println(resp1.ToString())

	/* 向 stscache set 0-20 的数据 */
	err = stscacheConn.Set(&stscache.Item{Key: semanticSegment, Value: respCacheByte, Time_start: startTime, Time_end: endTime, NumOfTables: numOfTab})
	if err != nil {
		log.Fatalf("Error setting value: %v", err)
	} else {
		log.Printf("STORED.")
	}

	q2 := NewQuery(query2, DB, "s")
	resp2, _ := c.Query(q2)
	startTime2, endTime2 := GetResponseTimeRange(resp2)
	numOfTab2 := GetNumOfTable(resp2)

	semanticSegment2 := GetSemanticSegment(query2)
	respCacheByte2 := ResponseToByteArray(resp2, query2)
	fmt.Println(resp2.ToString())

	/* 向 stscache set 20-40 的数据 */
	err = stscacheConn.Set(&stscache.Item{Key: semanticSegment2, Value: respCacheByte2, Time_start: startTime2, Time_end: endTime2, NumOfTables: numOfTab2})
	if err != nil {
		log.Fatalf("Error setting value: %v", err)
	} else {
		log.Printf("STORED.")
	}

	/* 向 cache get 0-40 的数据 */
	queryToBeGet := `select usage_system,usage_user,usage_guest,usage_nice,usage_guest_nice from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:00:40Z' and hostname='host_0'`
	qgst, qget := GetQueryTimeRange(queryToBeGet)
	values, _, err := stscacheConn.Get(semanticSegment, qgst, qget)
	if errors.Is(err, stscache.ErrCacheMiss) {
		log.Printf("Key not found in cache")
	} else if err != nil {
		log.Fatalf("Error getting value: %v", err)
	} else {
		log.Printf("GET.")
	}

	/* 把查询结果从字节流转换成 Response 结构 */
	convertedResponse, _, _, _, _ := ByteArrayToResponse(values)
	fmt.Println(convertedResponse.ToString())
}

func TestIntegratedClient(t *testing.T) {
	queryToBeSet := `select usage_system,usage_user,usage_guest,usage_nice,usage_guest_nice from devops..cpu where time >= '2022-01-01T01:00:00Z' and time < '2022-01-01T02:00:00Z' and hostname='host_0'`
	queryToBeGet := `select usage_system,usage_user,usage_guest,usage_nice,usage_guest_nice from devops..cpu where time >= '2022-01-01T01:00:00Z' and time < '2022-01-01T03:00:00Z' and hostname='host_0'`

	qm := NewQuery(queryToBeSet, "devops", "s")
	respCache, _ := c.Query(qm)
	startTime, endTime := GetResponseTimeRange(respCache)
	numOfTab := GetNumOfTable(respCache)

	semanticSegment := GetSemanticSegment(queryToBeSet)
	respCacheByte := ResponseToByteArray(respCache, queryToBeSet)
	//fmt.Println(respCache.ToString())
	log.Printf("bytes to be set:%d\n", len(respCacheByte))

	/* 向 stscache set 0-20 的数据 */
	err = stscacheConn.Set(&stscache.Item{Key: semanticSegment, Value: respCacheByte, Time_start: startTime, Time_end: endTime, NumOfTables: numOfTab})
	if err != nil {
		log.Fatalf("Error setting value: %v", err)
	} else {
		log.Printf("out STORED.")
	}

	/* 向 cache get 0-40 的数据，缺失的数据向数据库查询并存入 cache */
	STsCacheClient(c, queryToBeGet)

	/* 向 cache get 0-40 的数据 */
	qgst, qget := GetQueryTimeRange(queryToBeGet)
	values, _, err := stscacheConn.Get(semanticSegment, qgst, qget)
	if errors.Is(err, stscache.ErrCacheMiss) {
		log.Printf("Key not found in cache")
	} else if err != nil {
		log.Fatalf("Error getting value: %v", err)
	} else {
		log.Printf("out GET.")
		log.Printf("bytes get:%d\n", len(values))
	}

	/* 把查询结果从字节流转换成 Response 结构 */
	convertedResponse, _, _, _, _ := ByteArrayToResponse(values)
	crst, cret := GetResponseTimeRange(convertedResponse)
	//fmt.Println(convertedResponse.ToString())
	fmt.Println(crst)
	fmt.Println(cret)

}

func TestIntegratedClientIOT(t *testing.T) {
	queryToBeSet := `SELECT current_load,load_capacity FROM "diagnostics" WHERE ("name"='truck_0') AND TIME >= '2021-01-01T00:00:00Z' AND TIME <= '2021-01-01T01:00:00Z' GROUP BY "name"`
	queryToBeGet := `SELECT current_load,load_capacity FROM "diagnostics" WHERE ("name"='truck_0') AND TIME >= '2021-01-01T00:00:00Z' AND TIME <= '2021-01-01T02:00:00Z' GROUP BY "name"`

	qm := NewQuery(queryToBeSet, "iot", "s")
	respCache, _ := c.Query(qm)
	startTime, endTime := GetResponseTimeRange(respCache)
	numOfTab := GetNumOfTable(respCache)

	semanticSegment := GetSemanticSegment(queryToBeSet)
	respCacheByte := ResponseToByteArray(respCache, queryToBeSet)
	//fmt.Println(respCache.ToString())
	log.Printf("bytes to be set:%d\n", len(respCacheByte))

	/* 向 stscache set 0-20 的数据 */
	err = stscacheConn.Set(&stscache.Item{Key: semanticSegment, Value: respCacheByte, Time_start: startTime, Time_end: endTime, NumOfTables: numOfTab})
	if err != nil {
		log.Fatalf("Error setting value: %v", err)
	} else {
		log.Printf("out STORED.")
	}

	/* 向 cache get 0-40 的数据，缺失的数据向数据库查询并存入 cache */
	STsCacheClient(c, queryToBeGet)

	/* 向 cache get 0-40 的数据 */
	qgst, qget := GetQueryTimeRange(queryToBeGet)
	values, _, err := stscacheConn.Get(semanticSegment, qgst, qget)
	if errors.Is(err, stscache.ErrCacheMiss) {
		log.Printf("Key not found in cache")
	} else if err != nil {
		log.Fatalf("Error getting value: %v", err)
	} else {
		log.Printf("out GET.")
		log.Printf("bytes get:%d\n", len(values))
	}

	/* 把查询结果从字节流转换成 Response 结构 */
	convertedResponse, _, _, _, _ := ByteArrayToResponse(values)
	crst, cret := GetResponseTimeRange(convertedResponse)
	//fmt.Println(convertedResponse.ToString())
	fmt.Println(crst)
	fmt.Println(cret)

}

func TestIntegratedClientIOT100(t *testing.T) {
	queryToBeSet := `SELECT current_load,load_capacity FROM "diagnostics" WHERE TIME >= '2021-01-01T01:00:00Z' AND TIME <= '2021-01-01T2:00:00Z' GROUP BY "name"`
	queryToBeGet := `SELECT current_load,load_capacity FROM "diagnostics" WHERE TIME >= '2021-01-01T00:00:00Z' AND TIME <= '2021-01-01T2:00:00Z' GROUP BY "name"`
	TagKV = GetTagKV(c, "iot")
	Fields = GetFieldKeys(c, "iot")
	STsCacheURLArr := []string{"192.168.1.102:11211"}
	STsConnArr = InitStsConnsArr(STsCacheURLArr)

	qm := NewQuery(queryToBeSet, "iot", "s")
	respCache, _ := c.Query(qm)
	startTime, endTime := GetResponseTimeRange(respCache)
	numOfTab := GetNumOfTable(respCache)

	semanticSegment := GetSemanticSegment(queryToBeSet)
	respCacheByte := ResponseToByteArray(respCache, queryToBeSet)
	//fmt.Println(respCache.ToString())
	fmt.Printf("bytes to be set:%d\n", len(respCacheByte))

	/* 向 stscache set 0-20 的数据 */
	log.Printf("len:%d\n", len(respCacheByte))
	err = stscacheConn.Set(&stscache.Item{Key: semanticSegment, Value: respCacheByte, Time_start: startTime, Time_end: endTime, NumOfTables: numOfTab})
	if err != nil {
		log.Fatalf("Error setting value: %v", err)
	} else {
		log.Printf("out STORED.")
	}

	/* 向 cache get 0-40 的数据，缺失的数据向数据库查询并存入 cache */
	STsCacheClient(c, queryToBeGet)

	/* 向 cache get 0-40 的数据 */
	qgst, qget := GetQueryTimeRange(queryToBeGet)
	values, _, err := stscacheConn.Get(semanticSegment, qgst, qget)
	if errors.Is(err, stscache.ErrCacheMiss) {
		log.Printf("Key not found in cache")
	} else if err != nil {
		log.Fatalf("Error getting value: %v", err)
	} else {
		log.Printf("out GET.")
		log.Printf("bytes get:%d\n", len(values))
	}

	/* 把查询结果从字节流转换成 Response 结构 */
	convertedResponse, _, _, _, _ := ByteArrayToResponse(values)
	crst, cret := GetResponseTimeRange(convertedResponse)
	//fmt.Println(convertedResponse.ToString())
	fmt.Println(crst)
	fmt.Println(cret)

}

func TestIntegratedClientIOTBehindHit(t *testing.T) {
	queryToBeSet := `SELECT current_load,load_capacity FROM "diagnostics" WHERE TIME >= '2021-12-01T01:00:00Z' AND TIME <= '2021-12-02T03:00:00Z' GROUP BY "name"`
	queryToBeGet := `SELECT current_load,load_capacity FROM "diagnostics" WHERE TIME >= '2021-12-01T00:00:00Z' AND TIME <= '2021-12-02T03:00:00Z' GROUP BY "name"`

	qm := NewQuery(queryToBeSet, "iot", "s")
	respCache, _ := c.Query(qm)
	startTime, endTime := GetResponseTimeRange(respCache)
	numOfTab := GetNumOfTable(respCache)

	semanticSegment := GetSemanticSegment(queryToBeSet)
	respCacheByte := ResponseToByteArray(respCache, queryToBeSet)
	//fmt.Println(respCache.ToString())
	fmt.Printf("bytes to be set:%d\n", len(respCacheByte))

	/* 向 stscache set 0-20 的数据 */
	log.Printf("len:%d\n", len(respCacheByte))
	err = stscacheConn.Set(&stscache.Item{Key: semanticSegment, Value: respCacheByte, Time_start: startTime, Time_end: endTime, NumOfTables: numOfTab})
	if err != nil {
		log.Fatalf("Error setting value: %v", err)
	} else {
		log.Printf("out STORED.")
	}

	/* 向 cache get 0-40 的数据，缺失的数据向数据库查询并存入 cache */
	STsCacheClient(c, queryToBeGet)

	/* 向 cache get 0-40 的数据 */
	qgst, qget := GetQueryTimeRange(queryToBeGet)
	values, _, err := stscacheConn.Get(semanticSegment, qgst, qget)
	if errors.Is(err, stscache.ErrCacheMiss) {
		log.Printf("Key not found in cache")
	} else if err != nil {
		log.Fatalf("Error getting value: %v", err)
	} else {
		log.Printf("out GET.")
		log.Printf("bytes get:%d\n", len(values))
	}

	/* 把查询结果从字节流转换成 Response 结构 */
	convertedResponse, _, _, _, _ := ByteArrayToResponse(values)
	crst, cret := GetResponseTimeRange(convertedResponse)
	//fmt.Println(convertedResponse.ToString())
	fmt.Println(crst)
	fmt.Println(cret)

}

func TestIntegratedClientIOT2(t *testing.T) {
	query1 := `SELECT latitude,longitude,elevation FROM "readings" WHERE TIME >= '2021-12-29T16:30:00Z' AND TIME <= '2021-12-31T16:30:00Z' GROUP BY "name"`
	query2 := `SELECT latitude,longitude,elevation FROM "readings" WHERE TIME >= '2021-12-29T10:30:00Z' AND TIME < '2021-12-29T16:30:00Z' GROUP BY "name"`
	query3 := `SELECT latitude,longitude,elevation FROM "readings" WHERE TIME >= '2021-12-28T16:30:00Z' AND TIME <= '2021-12-29T16:30:00Z' GROUP BY "name"`
	qm := NewQuery(query1, "iot", "s")
	respCache, _ := c.Query(qm)
	startTime, endTime := GetResponseTimeRange(respCache)
	numOfTab := GetNumOfTable(respCache)

	semanticSegment := GetSemanticSegment(query1)
	respCacheByte := ResponseToByteArray(respCache, query1)
	//fmt.Println(respCache.ToString())
	fmt.Printf("bytes to be set:%d\n", len(respCacheByte))

	/* 向 stscache set 0-20 的数据 */
	log.Printf("len:%d\n", len(respCacheByte))
	err = stscacheConn.Set(&stscache.Item{Key: semanticSegment, Value: respCacheByte, Time_start: startTime, Time_end: endTime, NumOfTables: numOfTab})
	if err != nil {
		log.Fatalf("Error setting value: %v", err)
	} else {
		log.Printf("STORED.")
	}

	q2 := NewQuery(query2, "iot", "s")
	respCache2, _ := c.Query(q2)
	startTime2, endTime2 := GetResponseTimeRange(respCache2)
	numOfTab2 := GetNumOfTable(respCache2)

	semanticSegment2 := GetSemanticSegment(query2)
	respCacheByte2 := ResponseToByteArray(respCache2, query2)
	//fmt.Println(respCache.ToString())
	fmt.Printf("bytes to be set:%d\n", len(respCacheByte2))

	log.Printf("len:%d\n", len(respCacheByte2))
	err = stscacheConn.Set(&stscache.Item{Key: semanticSegment2, Value: respCacheByte2, Time_start: startTime2, Time_end: endTime2, NumOfTables: numOfTab2})
	if err != nil {
		log.Fatalf("Error setting value: %v", err)
	} else {
		log.Printf("STORED.")
	}

	/* 向 cache get 0-40 的数据 */
	qgst, qget := GetQueryTimeRange(query3)
	values, _, err := stscacheConn.Get(semanticSegment, qgst, qget)
	if errors.Is(err, stscache.ErrCacheMiss) {
		log.Printf("Key not found in cache")
	} else if err != nil {
		log.Fatalf("Error getting value: %v", err)
	} else {
		log.Printf("out GET.")
		log.Printf("bytes get:%d\n", len(values))
	}

	/* 把查询结果从字节流转换成 Response 结构 */
	convertedResponse, _, _, _, _ := ByteArrayToResponse(values)
	crst, cret := GetResponseTimeRange(convertedResponse)
	//fmt.Println(convertedResponse.ToString())
	fmt.Println(crst)
	fmt.Println(cret)
}

func TestMultiThreadSTsCache(t *testing.T) {
	//queryToBeSet := `SELECT current_load,load_capacity FROM "diagnostics" WHERE TIME >= '2021-01-01T02:00:00Z' AND TIME <= '2021-01-01T22:00:00Z' GROUP BY "name"`
	queryToBeGet := `SELECT current_load,load_capacity FROM "diagnostics" WHERE TIME >= '2021-01-01T00:00:00Z' AND TIME <= '2021-01-02T00:01:00Z' GROUP BY "name"`

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			STsCacheClient(c, queryToBeGet)
			//query := NewQuery(queryToBeGet, "iot", "s")
			//resp, _ := c.Query(query)
			//ss := GetSemanticSegment(queryToBeGet)
			//st, et := GetQueryTimeRange(queryToBeGet)
			//numOfTable := len(resp.Results[0].Series)
			//val := ResponseToByteArray(resp, queryToBeGet)
			//err := stscacheConn.Set(&stscache.Item{
			//	Key:         ss,
			//	Value:       val,
			//	Time_start:  st,
			//	Time_end:    et,
			//	NumOfTables: int64(numOfTable),
			//})
			//if err != nil {
			//	log.Fatal(err)
			//}
			defer wg.Done()
		}()
		//log.Println(i)
	}
	wg.Wait()
}

func TestSetSTsCache(t *testing.T) {
	//queryToBeSet := `SELECT current_load,load_capacity FROM "diagnostics" WHERE TIME >= '2021-01-01T02:00:00Z' AND TIME <= '2021-01-01T22:00:00Z' GROUP BY "name"`
	//queryToBeGet := `SELECT latitude,longitude,elevation FROM "readings" WHERE TIME >= '2021-12-29T11:00:00Z' AND TIME <= '2021-12-30T11:00:00Z' GROUP BY "name"`

	queries := []string{
		`SELECT latitude,longitude,elevation FROM "readings" WHERE TIME >= '2021-12-27T04:00:00Z' AND TIME <= '2021-12-28T04:00:00Z' GROUP BY "name"`,
		`SELECT latitude,longitude,elevation FROM "readings" WHERE TIME >= '2021-12-30T20:30:00Z' AND TIME <= '2021-12-31T20:30:00Z' GROUP BY "name"`,
		`SELECT latitude,longitude,elevation FROM "readings" WHERE TIME >= '2021-12-21T06:00:00Z' AND TIME <= '2021-12-21T08:00:00Z' GROUP BY "name"`,
		`SELECT latitude,longitude,elevation FROM "readings" WHERE TIME >= '2021-12-31T06:00:00Z' AND TIME <= '2021-12-31T12:00:00Z' GROUP BY "name"`,
		`SELECT latitude,longitude,elevation FROM "readings" WHERE TIME >= '2021-12-30T20:00:00Z' AND TIME <= '2021-12-31T20:00:00Z' GROUP BY "name"`,
		`SELECT latitude,longitude,elevation FROM "readings" WHERE TIME >= '2021-12-18T07:00:00Z' AND TIME <= '2021-12-18T08:00:00Z' GROUP BY "name"`,
		`SELECT latitude,longitude,elevation FROM "readings" WHERE TIME >= '2021-12-29T22:30:00Z' AND TIME <= '2021-12-30T22:30:00Z' GROUP BY "name"`,
		`SELECT latitude,longitude,elevation FROM "readings" WHERE TIME >= '2021-12-29T22:30:00Z' AND TIME <= '2021-12-30T22:30:00Z' GROUP BY "name"`,
		`SELECT latitude,longitude,elevation FROM "readings" WHERE TIME >= '2021-12-30T18:30:00Z' AND TIME <= '2021-12-31T06:30:00Z' GROUP BY "name"`,
		`SELECT latitude,longitude,elevation FROM "readings" WHERE TIME >= '2021-12-31T14:30:00Z' AND TIME <= '2021-12-31T20:30:00Z' GROUP BY "name"`,
		//`SELECT latitude,longitude,elevation FROM "readings" WHERE TIME >= '2021-12-03T23:00:00Z' AND TIME <= '2021-12-31T23:00:00Z' GROUP BY "name"`,
		`SELECT latitude,longitude,elevation FROM "readings" WHERE TIME >= '2021-12-24T04:00:00Z' AND TIME <= '2021-12-24T16:00:00Z' GROUP BY "name"`,
		`SELECT latitude,longitude,elevation FROM "readings" WHERE TIME >= '2021-12-31T00:00:00Z' AND TIME <= '2022-01-01T00:00:00Z' GROUP BY "name"`,
		`SELECT latitude,longitude,elevation FROM "readings" WHERE TIME >= '2021-12-29T11:00:00Z' AND TIME <= '2021-12-30T11:00:00Z' GROUP BY "name"`,
	}

	for _, queryString := range queries {
		log.Println(queryString)
		query := NewQuery(queryString, "iot", "s")
		resp, _ := c.Query(query)
		ss := GetSemanticSegment(queryString)
		st, et := GetQueryTimeRange(queryString)
		numOfTable := len(resp.Results[0].Series)
		val := ResponseToByteArray(resp, queryString)
		err := stscacheConn.Set(&stscache.Item{
			Key:         ss,
			Value:       val,
			Time_start:  st,
			Time_end:    et,
			NumOfTables: int64(numOfTable),
		})
		if err != nil {
			log.Fatal(err)
		}
	}

}

func TestInitStsConns(t *testing.T) {
	queryString := `SELECT current_load,load_capacity FROM "diagnostics" WHERE TIME >= '2021-01-01T01:00:00Z' AND TIME <= '2021-01-01T02:00:00Z' GROUP BY "name"`

	conns := InitStsConns()
	log.Printf("number of conns:%d\n", len(conns))
	for i, conn := range conns {
		log.Printf("index:%d\ttimeout:%d\n", i, conn.Timeout)
		query := NewQuery(queryString, "iot", "s")
		resp, _ := c.Query(query)
		ss := GetSemanticSegment(queryString)
		st, et := GetQueryTimeRange(queryString)
		numOfTable := len(resp.Results[0].Series)
		val := ResponseToByteArray(resp, queryString)
		err := conn.Set(&stscache.Item{
			Key:         ss,
			Value:       val,
			Time_start:  st,
			Time_end:    et,
			NumOfTables: int64(numOfTable),
		})
		if err != nil {
			log.Fatal(err)
		} else {
			log.Println("success.")
		}
	}
}

func TestInitStsConnsArr(t *testing.T) {
	queryString := `SELECT current_load,load_capacity FROM "diagnostics" WHERE TIME >= '2021-01-01T01:00:00Z' AND TIME <= '2021-01-01T02:00:00Z' GROUP BY "name"`
	urlString := "192.168.1.102:11211,192.168.1.102:11212"
	urlArr := strings.Split(urlString, ",")
	conns := InitStsConnsArr(urlArr)
	log.Printf("number of conns:%d\n", len(conns))
	for i, conn := range conns {
		log.Printf("index:%d\ttimeout:%d\n", i, conn.Timeout)
		query := NewQuery(queryString, "iot", "s")
		resp, _ := c.Query(query)
		ss := GetSemanticSegment(queryString)
		st, et := GetQueryTimeRange(queryString)
		numOfTable := len(resp.Results[0].Series)
		val := ResponseToByteArray(resp, queryString)
		err := conn.Set(&stscache.Item{
			Key:         ss,
			Value:       val,
			Time_start:  st,
			Time_end:    et,
			NumOfTables: int64(numOfTable),
		})
		if err != nil {
			log.Fatal(err)
		} else {
			log.Println("success.")
		}
	}
}

func BenchmarkIntegratedClient(b *testing.B) {
	queryToBeGet := `select usage_system,usage_user,usage_guest,usage_nice,usage_guest_nice from test..cpu where time >= '2022-01-01T00:00:00Z' and time < '2022-01-01T00:00:40Z' and hostname='host_0'`
	semanticSegment := GetSemanticSegment(queryToBeGet)
	for i := 0; i < b.N; i++ {
		/* 向 cache get 0-40 的数据 */
		qgst, qget := GetQueryTimeRange(queryToBeGet)
		_, _, err := stscacheConn.Get(semanticSegment, qgst, qget)
		if errors.Is(err, stscache.ErrCacheMiss) {
			log.Printf("Key not found in cache")
		} else if err != nil {
			log.Fatalf("Error getting value: %v", err)
		} else {
			log.Printf("GET.")
		}
	}
}

func TestClient_QueryFromDatabase(t *testing.T) {
	queryString := `SELECT current_load,load_capacity FROM "diagnostics" WHERE TIME >= '2021-01-01T01:00:00Z' AND TIME <= '2021-01-01T02:00:00Z' GROUP BY "name"`
	query := NewQuery(queryString, "iot", "s")
	length, resp, err := c.QueryFromDatabase(query)
	fmt.Println(resp.ToString())
	fmt.Println(length)
	fmt.Println(err)
}

func TestSTsCacheClient(t *testing.T) {
	querySet := `SELECT mean(velocity),mean(fuel_consumption) FROM "readings" WHERE ("name"='truck_1' or "name"='truck_50' or "name"='truck_99') AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T01:00:00Z' GROUP BY "name",time(10m)`
	queryGet := `SELECT mean(velocity),mean(fuel_consumption) FROM "readings" WHERE ("name"='truck_1' or "name"='truck_50' or "name"='truck_99') AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T02:00:00Z' GROUP BY "name",time(10m)`

	cacheUrlString := "192.168.1.102:11211"
	urlArr := strings.Split(cacheUrlString, ",")
	conns := InitStsConnsArr(urlArr)
	DB = "iot_small"
	fmt.Printf("number of conns:%d\n", len(conns))
	TagKV = GetTagKV(c, "iot_small")
	Fields = GetFieldKeys(c, "iot_small")
	STsConnArr = InitStsConnsArr(urlArr)
	var dbConn, _ = NewHTTPClient(HTTPConfig{
		Addr: "http://192.168.1.103:8086",
	})

	query := NewQuery(querySet, "iot_medium", "s")
	resp1, err := dbConn.Query(query)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("\tresp1:\n", resp1.ToString())
	}
	values := ResponseToByteArray(resp1, querySet)
	numOfTab := GetNumOfTable(resp1)

	partialSegment := ""
	fields := ""
	metric := ""
	queryTemplate, startTime, endTime, _ := GetQueryTemplate(querySet)
	partialSegment, fields, metric = GetPartialSegmentAndFields(querySet)
	QueryTemplateToPartialSegment[queryTemplate] = partialSegment
	SegmentToFields[partialSegment] = fields
	SegmentToMetric[partialSegment] = metric

	// 用于 Get 的语义段
	//semanticSegment := GetTotalSegment(metric, tags, partialSegment)

	// 用于 Set 的语义段
	starSegment := GetStarSegment(metric, partialSegment)

	err = STsConnArr[0].Set(&stscache.Item{Key: starSegment, Value: values, Time_start: startTime, Time_end: endTime, NumOfTables: numOfTab})

	resp2, _, _ := STsCacheClient(dbConn, queryGet)
	fmt.Println("\tres2:\n", resp2.ToString())

}

func TestSTsCacheClient2(t *testing.T) {
	querySet1 := `SELECT mean(velocity),mean(fuel_consumption) FROM "readings" WHERE ("name"='truck_1') AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T00:20:00Z' GROUP BY "name",time(10m)`
	querySet2 := `SELECT mean(velocity),mean(fuel_consumption) FROM "readings" WHERE ("name"='truck_50') AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T01:00:00Z' GROUP BY "name",time(10m)`
	querySet3 := `SELECT mean(velocity),mean(fuel_consumption) FROM "readings" WHERE ("name"='truck_99') AND TIME >= '2022-01-01T00:40:00Z' AND TIME < '2022-01-01T01:00:00Z' GROUP BY "name",time(10m)`

	queryGet := `SELECT mean(velocity),mean(fuel_consumption) FROM "readings" WHERE ("name"='truck_1' or "name"='truck_50' or "name"='truck_99') AND TIME >= '2022-01-01T00:00:00Z' AND TIME < '2022-01-01T01:00:00Z' GROUP BY "name",time(10m)`

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

	query1 := NewQuery(querySet1, "iot_medium", "s")
	resp1, err := dbConn.Query(query1)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("\tresp1:\n", resp1.ToString())
	}
	//values1 := ResponseToByteArray(resp1, querySet1)
	numOfTab1 := GetNumOfTable(resp1)

	partialSegment := ""
	fields := ""
	metric := ""
	queryTemplate1, startTime1, endTime1, tags1 := GetQueryTemplate(querySet1)
	partialSegment, fields, metric = GetPartialSegmentAndFields(querySet1)
	QueryTemplateToPartialSegment[queryTemplate1] = partialSegment
	SegmentToFields[partialSegment] = fields
	SegmentToMetric[partialSegment] = metric
	fields = "time[int64]," + fields
	datatypes1 := GetDataTypeArrayFromSF(fields)

	values1 := ResponseToByteArrayWithParams(resp1, datatypes1, tags1, metric, partialSegment)

	// 用于 Get 的语义段
	//semanticSegment := GetTotalSegment(metric, tags, partialSegment)
	// 用于 Set 的语义段
	starSegment := GetStarSegment(metric, partialSegment)

	err = STsConnArr[0].Set(&stscache.Item{Key: starSegment, Value: values1, Time_start: startTime1, Time_end: endTime1, NumOfTables: numOfTab1})

	query2 := NewQuery(querySet2, "iot_medium", "s")
	resp2, err := dbConn.Query(query2)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("\tresp2:\n", resp2.ToString())
	}
	//values2 := ResponseToByteArray(resp2, querySet2)
	numOfTab2 := GetNumOfTable(resp2)

	queryTemplate2, startTime2, endTime2, tags2 := GetQueryTemplate(querySet2)
	partialSegment, fields, metric = GetPartialSegmentAndFields(querySet2)
	QueryTemplateToPartialSegment[queryTemplate2] = partialSegment
	SegmentToFields[partialSegment] = fields
	SegmentToMetric[partialSegment] = metric
	fields = "time[int64]," + fields
	datatypes2 := GetDataTypeArrayFromSF(fields)

	values2 := ResponseToByteArrayWithParams(resp2, datatypes2, tags2, metric, partialSegment)

	err = STsConnArr[0].Set(&stscache.Item{Key: starSegment, Value: values2, Time_start: startTime2, Time_end: endTime2, NumOfTables: numOfTab2})

	query3 := NewQuery(querySet3, "iot_medium", "s")
	resp3, err := dbConn.Query(query3)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("\tresp3:\n", resp3.ToString())
	}
	//values3 := ResponseToByteArray(resp3, querySet3)
	numOfTab3 := GetNumOfTable(resp3)

	queryTemplate3, startTime3, endTime3, tags3 := GetQueryTemplate(querySet3)
	partialSegment, fields, metric = GetPartialSegmentAndFields(querySet3)
	QueryTemplateToPartialSegment[queryTemplate3] = partialSegment
	SegmentToFields[partialSegment] = fields
	SegmentToMetric[partialSegment] = metric
	fields = "time[int64]," + fields
	datatypes3 := GetDataTypeArrayFromSF(fields)

	values3 := ResponseToByteArrayWithParams(resp3, datatypes3, tags3, metric, partialSegment)

	err = STsConnArr[0].Set(&stscache.Item{Key: starSegment, Value: values3, Time_start: startTime3, Time_end: endTime3, NumOfTables: numOfTab3})

	respGet, _, _ := STsCacheClient(dbConn, queryGet)
	fmt.Println("\tresp get:\n", respGet.ToString())

	queryG := NewQuery(queryGet, "iot_medium", "s")
	respG, err := dbConn.Query(queryG)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("\tresp G:\n", respG.ToString())
	}

}
