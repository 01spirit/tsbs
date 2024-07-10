// Package client (v2) is the current official Go client for InfluxDB.
package client // import "github.com/influxdata/influxdb1-client/v2"

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	stscache "github.com/timescale/tsbs/InfluxDB-client/memcache"
	"github.com/timescale/tsbs/InfluxDB-client/models"
	"sync"

	//"github.com/influxdata/influxdb1-client/models"
	"io"
	"io/ioutil"
	"log"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"
)

type ContentEncoding string

// 连接数据库
var c, err = NewHTTPClient(HTTPConfig{
	Addr: "http://192.168.1.103:8086",
})

var stscacheConn = stscache.New("192.168.1.102:11211")

var TagKV MeasurementTagMap
var Fields map[string]map[string]string

// 查询模版对应除 SM 之外的部分语义段
var QueryTemplateToPartialSegment = make(map[string]string)
var SegmentToMetric = make(map[string]string)

var QueryTemplates = make(map[string]string) // 存放查询模版及其语义段；查询模板只替换了时间范围，语义段没变
var SegmentToFields = make(map[string]string)
var SeprateSegments = make(map[string][]string) // 完整语义段和单独语义段的映射

var UseCache = "db"

var MaxThreadNum = 64

const STRINGBYTELENGTH = 32

// 数据库名称
var (
	//MYDB = "NOAA_water_database"
	TESTDB   = "test"
	DB       = "iot_small"
	CPUDB    = "devops"
	IOTDB    = "iot"
	username = "root"
	password = "12345678"
)

var STsCacheURL string

const (
	DefaultEncoding ContentEncoding = ""
	GzipEncoding    ContentEncoding = "gzip"
)

// HTTPConfig is the config data needed to create an HTTP Client.
type HTTPConfig struct {
	// Addr should be of the form "http://host:port"
	// or "http://[ipv6-host%zone]:port".
	Addr string

	// Username is the influxdb username, optional.
	Username string

	// Password is the influxdb password, optional.
	Password string

	// UserAgent is the http User Agent, defaults to "InfluxDBClient".
	UserAgent string

	// Timeout for influxdb writes, defaults to no timeout.
	Timeout time.Duration

	// InsecureSkipVerify gets passed to the http client, if true, it will
	// skip https certificate verification. Defaults to false.
	InsecureSkipVerify bool

	// TLSConfig allows the user to set their own TLS config for the HTTP
	// Client. If set, this option overrides InsecureSkipVerify.
	TLSConfig *tls.Config

	// Proxy configures the Proxy function on the HTTP client.
	Proxy func(req *http.Request) (*url.URL, error)

	// WriteEncoding specifies the encoding of write request
	WriteEncoding ContentEncoding
}

// BatchPointsConfig is the config data needed to create an instance of the BatchPoints struct.
type BatchPointsConfig struct {
	// Precision is the write precision of the points, defaults to "ns".
	Precision string

	// Database is the database to write points to.
	Database string

	// RetentionPolicy is the retention policy of the points.
	RetentionPolicy string

	// Write consistency is the number of servers required to confirm write.
	WriteConsistency string
}

// Client is a client interface for writing & querying the database.
type Client interface {
	// Ping checks that status of cluster, and will always return 0 time and no
	// error for UDP clients.
	Ping(timeout time.Duration) (time.Duration, string, error)

	// Write takes a BatchPoints object and writes all Points to InfluxDB.
	Write(bp BatchPoints) error

	// Query makes an InfluxDB Query on the database. This will fail if using
	// the UDP client.
	Query(q Query) (*Response, error)

	QueryFromDatabase(query Query) (int64, *Response, error)

	// QueryAsChunk makes an InfluxDB Query on the database. This will fail if using
	// the UDP client.
	QueryAsChunk(q Query) (*ChunkedResponse, error)

	// Close releases any resources a Client may be using.
	Close() error
}

// NewHTTPClient returns a new Client from the provided config.
// Client is safe for concurrent use by multiple goroutines.
func NewHTTPClient(conf HTTPConfig) (Client, error) {
	if conf.UserAgent == "" {
		conf.UserAgent = "InfluxDBClient"
	}

	u, err := url.Parse(conf.Addr)
	if err != nil {
		return nil, err
	} else if u.Scheme != "http" && u.Scheme != "https" {
		m := fmt.Sprintf("Unsupported protocol scheme: %s, your address"+
			" must start with http:// or https://", u.Scheme)
		return nil, errors.New(m)
	}

	switch conf.WriteEncoding {
	case DefaultEncoding, GzipEncoding:
	default:
		return nil, fmt.Errorf("unsupported encoding %s", conf.WriteEncoding)
	}

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: conf.InsecureSkipVerify,
		},
		Proxy: conf.Proxy,
	}
	if conf.TLSConfig != nil {
		tr.TLSClientConfig = conf.TLSConfig
	}
	return &client{
		url:       *u,
		username:  conf.Username,
		password:  conf.Password,
		useragent: conf.UserAgent,
		httpClient: &http.Client{
			Timeout:   conf.Timeout,
			Transport: tr,
		},
		transport: tr,
		encoding:  conf.WriteEncoding,
	}, nil
}

// Ping will check to see if the server is up with an optional timeout on waiting for leader.
// Ping returns how long the request took, the version of the server it connected to, and an error if one occurred.
func (c *client) Ping(timeout time.Duration) (time.Duration, string, error) {
	now := time.Now()

	u := c.url
	u.Path = path.Join(u.Path, "ping")

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return 0, "", err
	}

	req.Header.Set("User-Agent", c.useragent)

	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	if timeout > 0 {
		params := req.URL.Query()
		params.Set("wait_for_leader", fmt.Sprintf("%.0fs", timeout.Seconds()))
		req.URL.RawQuery = params.Encode()
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()

	//body, err := ioutil.ReadAll(resp.Body)
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, "", err
	}

	if resp.StatusCode != http.StatusNoContent {
		var err = errors.New(string(body))
		return 0, "", err
	}

	version := resp.Header.Get("X-Influxdb-Version")
	return time.Since(now), version, nil
}

// Close releases the client's resources.
func (c *client) Close() error {
	c.transport.CloseIdleConnections()
	return nil
}

// client is safe for concurrent use as the fields are all read-only
// once the client is instantiated.
type client struct {
	// N.B - if url.UserInfo is accessed in future modifications to the
	// methods on client, you will need to synchronize access to url.
	url        url.URL
	username   string
	password   string
	useragent  string
	httpClient *http.Client
	transport  *http.Transport
	encoding   ContentEncoding
}

// BatchPoints is an interface into a batched grouping of points to write into
// InfluxDB together. BatchPoints is NOT thread-safe, you must create a separate
// batch for each goroutine.
type BatchPoints interface {
	// AddPoint adds the given point to the Batch of points.
	AddPoint(p *Point)
	// AddPoints adds the given points to the Batch of points.
	AddPoints(ps []*Point)
	// Points lists the points in the Batch.
	Points() []*Point

	// Precision returns the currently set precision of this Batch.
	Precision() string
	// SetPrecision sets the precision of this batch.
	SetPrecision(s string) error

	// Database returns the currently set database of this Batch.
	Database() string
	// SetDatabase sets the database of this Batch.
	SetDatabase(s string)

	// WriteConsistency returns the currently set write consistency of this Batch.
	WriteConsistency() string
	// SetWriteConsistency sets the write consistency of this Batch.
	SetWriteConsistency(s string)

	// RetentionPolicy returns the currently set retention policy of this Batch.
	RetentionPolicy() string
	// SetRetentionPolicy sets the retention policy of this Batch.
	SetRetentionPolicy(s string)
}

// NewBatchPoints returns a BatchPoints interface based on the given config.
func NewBatchPoints(conf BatchPointsConfig) (BatchPoints, error) {
	if conf.Precision == "" {
		conf.Precision = "ns"
	}
	if _, err := time.ParseDuration("1" + conf.Precision); err != nil {
		return nil, err
	}
	bp := &batchpoints{
		database:         conf.Database,
		precision:        conf.Precision,
		retentionPolicy:  conf.RetentionPolicy,
		writeConsistency: conf.WriteConsistency,
	}
	return bp, nil
}

type batchpoints struct {
	points           []*Point
	database         string
	precision        string
	retentionPolicy  string
	writeConsistency string
}

func (bp *batchpoints) AddPoint(p *Point) {
	bp.points = append(bp.points, p)
}

func (bp *batchpoints) AddPoints(ps []*Point) {
	bp.points = append(bp.points, ps...)
}

func (bp *batchpoints) Points() []*Point {
	return bp.points
}

func (bp *batchpoints) Precision() string {
	return bp.precision
}

func (bp *batchpoints) Database() string {
	return bp.database
}

func (bp *batchpoints) WriteConsistency() string {
	return bp.writeConsistency
}

func (bp *batchpoints) RetentionPolicy() string {
	return bp.retentionPolicy
}

func (bp *batchpoints) SetPrecision(p string) error {
	if _, err := time.ParseDuration("1" + p); err != nil {
		return err
	}
	bp.precision = p
	return nil
}

func (bp *batchpoints) SetDatabase(db string) {
	bp.database = db
}

func (bp *batchpoints) SetWriteConsistency(wc string) {
	bp.writeConsistency = wc
}

func (bp *batchpoints) SetRetentionPolicy(rp string) {
	bp.retentionPolicy = rp
}

// Point represents a single data point.
type Point struct {
	pt models.Point
}

// NewPoint returns a point with the given timestamp. If a timestamp is not
// given, then data is sent to the database without a timestamp, in which case
// the server will assign local time upon reception. NOTE: it is recommended to
// send data with a timestamp.
func NewPoint(
	name string,
	tags map[string]string,
	fields map[string]interface{},
	t ...time.Time,
) (*Point, error) {
	var T time.Time
	if len(t) > 0 {
		T = t[0]
	}

	pt, err := models.NewPoint(name, models.NewTags(tags), fields, T)
	if err != nil {
		return nil, err
	}
	return &Point{
		pt: pt,
	}, nil
}

// String returns a line-protocol string of the Point.
func (p *Point) String() string {
	return p.pt.String()
}

// PrecisionString returns a line-protocol string of the Point,
// with the timestamp formatted for the given precision.
func (p *Point) PrecisionString(precision string) string {
	return p.pt.PrecisionString(precision)
}

// Name returns the measurement name of the point.
func (p *Point) Name() string {
	return string(p.pt.Name())
}

// Tags returns the tags associated with the point.
func (p *Point) Tags() map[string]string {
	return p.pt.Tags().Map()
}

// Time return the timestamp for the point.
func (p *Point) Time() time.Time {
	return p.pt.Time()
}

// UnixNano returns timestamp of the point in nanoseconds since Unix epoch.
func (p *Point) UnixNano() int64 {
	return p.pt.UnixNano()
}

// Fields returns the fields for the point.
func (p *Point) Fields() (map[string]interface{}, error) {
	return p.pt.Fields()
}

// NewPointFrom returns a point from the provided models.Point.
func NewPointFrom(pt models.Point) *Point {
	return &Point{pt: pt}
}

func (c *client) Write(bp BatchPoints) error {
	var b bytes.Buffer

	var w io.Writer
	if c.encoding == GzipEncoding {
		w = gzip.NewWriter(&b)
	} else {
		w = &b
	}

	for _, p := range bp.Points() { //数据点批量写入
		if p == nil {
			continue
		}
		if _, err := io.WriteString(w, p.pt.PrecisionString(bp.Precision())); err != nil { //向 writer 写入一条数据(sring)
			return err
		}

		if _, err := w.Write([]byte{'\n'}); err != nil { //每条数据换一行
			return err
		}
	}

	// gzip writer should be closed to flush data into underlying buffer
	if c, ok := w.(io.Closer); ok {
		if err := c.Close(); err != nil {
			return err
		}
	}

	//组合一个写入请求
	u := c.url
	u.Path = path.Join(u.Path, "write")

	req, err := http.NewRequest("POST", u.String(), &b)
	if err != nil {
		return err
	}
	if c.encoding != DefaultEncoding {
		req.Header.Set("Content-Encoding", string(c.encoding))
	}
	req.Header.Set("Content-Type", "")
	req.Header.Set("User-Agent", c.useragent)
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	params := req.URL.Query()
	params.Set("db", bp.Database())
	params.Set("rp", bp.RetentionPolicy())
	params.Set("precision", bp.Precision())
	params.Set("consistency", bp.WriteConsistency())
	req.URL.RawQuery = params.Encode()

	//发送请求，接受响应
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	//body, err := ioutil.ReadAll(resp.Body)
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		var err = errors.New(string(body))
		return err
	}

	return nil
}

// Query defines a query to send to the server.
type Query struct {
	Command         string
	Database        string
	RetentionPolicy string
	Precision       string
	Chunked         bool // chunked是数据存储和查询的方式，用于大量数据的读写操作，把数据划分成较小的块存储，而不是单条记录	，块内数据点数量固定
	ChunkSize       int
	Parameters      map[string]interface{}
}

// Params is a type alias to the query parameters.
type Params map[string]interface{}

// NewQuery returns a query object.
// The database and precision arguments can be empty strings if they are not needed for the query.
func NewQuery(command, database, precision string) Query {
	return Query{
		Command:    command,
		Database:   database,
		Precision:  precision,                    // autogen
		Parameters: make(map[string]interface{}), // 参数化查询 ?
	}
}

// NewQueryWithRP returns a query object.
// The database, retention policy, and precision arguments can be empty strings if they are not needed
// for the query. Setting the retention policy only works on InfluxDB versions 1.6 or greater.
func NewQueryWithRP(command, database, retentionPolicy, precision string) Query {
	return Query{
		Command:         command,
		Database:        database,
		RetentionPolicy: retentionPolicy,
		Precision:       precision,
		Parameters:      make(map[string]interface{}),
	}
}

// NewQueryWithParameters returns a query object.
// The database and precision arguments can be empty strings if they are not needed for the query.
// parameters is a map of the parameter names used in the command to their values.
func NewQueryWithParameters(command, database, precision string, parameters map[string]interface{}) Query {
	return Query{
		Command:    command,
		Database:   database,
		Precision:  precision,
		Parameters: parameters,
	}
}

// Response represents a list of statement results.
type Response struct {
	Results []Result
	Err     string `json:"error,omitempty"`
}

// Error returns the first error from any statement.
// It returns nil if no errors occurred on any statements.
func (r *Response) Error() error {
	if r.Err != "" {
		return errors.New(r.Err)
	}
	for _, result := range r.Results {
		if result.Err != "" {
			return errors.New(result.Err)
		}
	}
	return nil
}

// Message represents a user message.
type Message struct {
	Level string
	Text  string
}

// Result represents a resultset returned from a single statement.
type Result struct {
	StatementId int `json:"statement_id"`
	Series      []models.Row
	Messages    []*Message
	Err         string `json:"error,omitempty"`
}

func (c *client) QueryFromDatabase(q Query) (int64, *Response, error) {
	var length int64
	req, err := c.createDefaultRequest(q)
	if err != nil {
		return 0, nil, err
	}
	params := req.URL.Query()
	if q.Chunked { //查询结果是否分块
		params.Set("chunked", "true")
		if q.ChunkSize > 0 {
			params.Set("chunk_size", strconv.Itoa(q.ChunkSize))
		}
		req.URL.RawQuery = params.Encode()
	}
	resp, err := c.httpClient.Do(req) // 发送请求
	length = resp.ContentLength
	if err != nil {
		return 0, nil, err
	}
	defer func() {
		io.Copy(ioutil.Discard, resp.Body) // https://github.com/influxdata/influxdb1-client/issues/58
		resp.Body.Close()
	}()

	if err := checkResponse(resp); err != nil {
		return 0, nil, err
	}

	var response Response
	if q.Chunked { // 分块
		cr := NewChunkedResponse(resp.Body)
		for {
			r, err := cr.NextResponse()
			if err != nil {
				if err == io.EOF { // 结束
					break
				}
				// If we got an error while decoding the response, send that back.
				return 0, nil, err
			}

			if r == nil {
				break
			}

			response.Results = append(response.Results, r.Results...) // 把所有结果添加到 response.Results 数组中
			if r.Err != "" {
				response.Err = r.Err
				break
			}
		}
	} else { // 不分块，普通查询
		dec := json.NewDecoder(resp.Body) // 响应是 json 格式，需要进行解码，创建一个 Decoder，参数是 JSON 的 Reader
		dec.UseNumber()                   // 解码时把数字字符串转换成 Number 的字面值
		decErr := dec.Decode(&response)   // 解码，结果存入自定义的 Response, Response结构体和 json 的字段对应

		// ignore this error if we got an invalid status code
		if decErr != nil && decErr.Error() == "EOF" && resp.StatusCode != http.StatusOK {
			decErr = nil
		}
		// If we got a valid decode error, send that back
		if decErr != nil {
			return 0, nil, fmt.Errorf("unable to decode json: received status code %d err: %s", resp.StatusCode, decErr)
		}
	}

	// If we don't have an error in our json response, and didn't get statusOK
	// then send back an error
	if resp.StatusCode != http.StatusOK && response.Error() == nil {
		return 0, &response, fmt.Errorf("received status code %d from server", resp.StatusCode)
	}
	return length, &response, nil
}

// Query sends a command to the server and returns the Response.
func (c *client) Query(q Query) (*Response, error) {
	req, err := c.createDefaultRequest(q)
	if err != nil {
		return nil, err
	}
	params := req.URL.Query()
	if q.Chunked { //查询结果是否分块
		params.Set("chunked", "true")
		if q.ChunkSize > 0 {
			params.Set("chunk_size", strconv.Itoa(q.ChunkSize))
		}
		req.URL.RawQuery = params.Encode()
	}
	resp, err := c.httpClient.Do(req) // 发送请求
	if err != nil {
		return nil, err
	}
	defer func() {
		io.Copy(ioutil.Discard, resp.Body) // https://github.com/influxdata/influxdb1-client/issues/58
		resp.Body.Close()
	}()

	if err := checkResponse(resp); err != nil {
		return nil, err
	}

	var response Response
	if q.Chunked { // 分块
		cr := NewChunkedResponse(resp.Body)
		for {
			r, err := cr.NextResponse()
			if err != nil {
				if err == io.EOF { // 结束
					break
				}
				// If we got an error while decoding the response, send that back.
				return nil, err
			}

			if r == nil {
				break
			}

			response.Results = append(response.Results, r.Results...) // 把所有结果添加到 response.Results 数组中
			if r.Err != "" {
				response.Err = r.Err
				break
			}
		}
	} else { // 不分块，普通查询
		dec := json.NewDecoder(resp.Body) // 响应是 json 格式，需要进行解码，创建一个 Decoder，参数是 JSON 的 Reader
		dec.UseNumber()                   // 解码时把数字字符串转换成 Number 的字面值
		decErr := dec.Decode(&response)   // 解码，结果存入自定义的 Response, Response结构体和 json 的字段对应

		// ignore this error if we got an invalid status code
		if decErr != nil && decErr.Error() == "EOF" && resp.StatusCode != http.StatusOK {
			decErr = nil
		}
		// If we got a valid decode error, send that back
		if decErr != nil {
			return nil, fmt.Errorf("unable to decode json: received status code %d err: %s", resp.StatusCode, decErr)
		}
	}

	// If we don't have an error in our json response, and didn't get statusOK
	// then send back an error
	if resp.StatusCode != http.StatusOK && response.Error() == nil {
		return &response, fmt.Errorf("received status code %d from server", resp.StatusCode)
	}
	return &response, nil
}

// QueryAsChunk sends a command to the server and returns the Response.
func (c *client) QueryAsChunk(q Query) (*ChunkedResponse, error) {
	req, err := c.createDefaultRequest(q)
	if err != nil {
		return nil, err
	}
	params := req.URL.Query()
	params.Set("chunked", "true")
	if q.ChunkSize > 0 {
		params.Set("chunk_size", strconv.Itoa(q.ChunkSize))
	}
	req.URL.RawQuery = params.Encode()
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if err := checkResponse(resp); err != nil {
		return nil, err
	}
	return NewChunkedResponse(resp.Body), nil // 把HTTP响应的 reader 传入，进行解码
}

// 检验响应合法性
func checkResponse(resp *http.Response) error {
	// If we lack a X-Influxdb-Version header, then we didn't get a response from influxdb
	// but instead some other service. If the error code is also a 500+ code, then some
	// downstream loadbalancer/proxy/etc had an issue and we should report that.
	if resp.Header.Get("X-Influxdb-Version") == "" && resp.StatusCode >= http.StatusInternalServerError {
		body, err := io.ReadAll(resp.Body)
		if err != nil || len(body) == 0 {
			return fmt.Errorf("received status code %d from downstream server", resp.StatusCode)
		}

		return fmt.Errorf("received status code %d from downstream server, with response body: %q", resp.StatusCode, body)
	}

	// If we get an unexpected content type, then it is also not from influx direct and therefore
	// we want to know what we received and what status code was returned for debugging purposes.
	if cType, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type")); cType != "application/json" {
		// Read up to 1kb of the body to help identify downstream errors and limit the impact of things
		// like downstream serving a large file
		body, err := ioutil.ReadAll(io.LimitReader(resp.Body, 1024))
		if err != nil || len(body) == 0 {
			return fmt.Errorf("expected json response, got empty body, with status: %v", resp.StatusCode)
		}

		return fmt.Errorf("expected json response, got %q, with status: %v and response body: %q", cType, resp.StatusCode, body)
	}
	return nil
}

// 创造默认查询请求
func (c *client) createDefaultRequest(q Query) (*http.Request, error) {
	u := c.url
	u.Path = path.Join(u.Path, "query")

	jsonParameters, err := json.Marshal(q.Parameters)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", u.String(), nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "")
	req.Header.Set("User-Agent", c.useragent)

	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	params := req.URL.Query()
	params.Set("q", q.Command)
	params.Set("db", q.Database)
	if q.RetentionPolicy != "" {
		params.Set("rp", q.RetentionPolicy)
	}
	params.Set("params", string(jsonParameters))

	if q.Precision != "" {
		params.Set("epoch", q.Precision)
	}
	req.URL.RawQuery = params.Encode()

	return req, nil

}

// duplexReader reads responses and writes it to another writer while
// satisfying the reader interface.
type duplexReader struct {
	r io.ReadCloser
	w io.Writer
}

func (r *duplexReader) Read(p []byte) (n int, err error) {
	n, err = r.r.Read(p)
	if err == nil {
		r.w.Write(p[:n])
	}
	return n, err
}

// Close closes the response.
func (r *duplexReader) Close() error {
	return r.r.Close()
}

// ChunkedResponse represents a response from the server that
// uses chunking to stream the output.
type ChunkedResponse struct {
	dec    *json.Decoder
	duplex *duplexReader
	buf    bytes.Buffer
}

// NewChunkedResponse reads a stream and produces responses from the stream.
func NewChunkedResponse(r io.Reader) *ChunkedResponse {
	rc, ok := r.(io.ReadCloser)
	if !ok {
		rc = ioutil.NopCloser(r)
	}
	resp := &ChunkedResponse{}
	resp.duplex = &duplexReader{r: rc, w: &resp.buf} //把 reader 中的数据写入 buffer
	resp.dec = json.NewDecoder(resp.duplex)          //解码
	resp.dec.UseNumber()
	return resp
}

// NextResponse reads the next line of the stream and returns a response.
func (r *ChunkedResponse) NextResponse() (*Response, error) {
	var response Response
	if err := r.dec.Decode(&response); err != nil {
		if err == io.EOF {
			return nil, err
		}
		// A decoding error happened. This probably means the server crashed
		// and sent a last-ditch error message to us. Ensure we have read the
		// entirety of the connection to get any remaining error text.
		io.Copy(ioutil.Discard, r.duplex)
		return nil, errors.New(strings.TrimSpace(r.buf.String()))
	}

	r.buf.Reset()
	return &response, nil
}

// Close closes the response.
func (r *ChunkedResponse) Close() error {
	return r.duplex.Close()
}

var CacheHash = make(map[string]int)

//var CacheHashMtx sync.Mutex

// GetCacheHashValue 根据 fields 选择不同的 cache
func GetCacheHashValue(fields string) int {
	//CacheHashMtx.Lock()
	CacheNum := len(STsConnArr)

	if CacheNum == 0 {
		CacheNum = 1
	}
	if _, ok := CacheHash[fields]; !ok {
		value := len(CacheHash) % CacheNum
		CacheHash[fields] = value
	}
	hashValue := CacheHash[fields]
	// CacheHashMtx.Unlock()
	return hashValue
}

// STsCacheClient /* STsCache */
/*
	1. 客户端接收查询语句
	2. 客户端向 cache 系统查询，得到部分结果
	3. 生成这条查询语句的模版，把时间范围用占位符替换
	4. 得到要向数据库查询的时间范围，带入模版，向数据库查询剩余数据
	5. 客户端把剩余数据存入 cache
*/
var mtx sync.Mutex

// var STsConnArr = InitStsConns()
var STsConnArr []*stscache.Client

func InitStsConns() []*stscache.Client {
	conns := make([]*stscache.Client, 0)
	for i := 0; i < MaxThreadNum; i++ {
		//conns = append(conns, stscache.New("192.168.1.102:11211"))
		conns = append(conns, stscache.New(STsCacheURL))
	}
	return conns
}

func InitStsConnsArr(urlArr []string) []*stscache.Client {
	conns := make([]*stscache.Client, 0)
	for i := 0; i < len(urlArr); i++ {
		conns = append(conns, stscache.New(urlArr[i]))
	}
	return conns
}

var num = 0

func STsCacheClient(conn Client, queryString string) (*Response, uint64, uint8) {

	CacheNum := len(STsConnArr)

	if CacheNum == 0 {
		CacheNum = 1
	}

	byteLength := uint64(0)
	hitKind := uint8(0)

	/* 原始查询语句替换掉时间之后的的模版 */
	queryTemplate, startTime, endTime, tags := GetQueryTemplate(queryString) // 时间用 '?' 代替

	/* 从查询模版获取语义段，或构造语义段并存入查询模版 */

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

	// 用于 Set 的语义段
	starSegment := GetStarSegment(metric, partialSegment)

	//semanticSegment := ""
	//fields := ""
	//if ss, ok := QueryTemplates[queryTemplate]; !ok { // 查询模版中不存在该查询
	//
	//	//semanticSegment = GetSemanticSegment(queryString)
	//	semanticSegment, fields = GetSemanticSegmentAndFields(queryString)
	//	//log.Printf("ss:%d\t%s\n", len(semanticSegment), semanticSegment)
	//	/* 存入全局 map */
	//
	//	QueryTemplates[queryTemplate] = semanticSegment
	//	SegmentToFields[semanticSegment] = fields
	//
	//} else {
	//	semanticSegment = ss
	//	fields = SegmentToFields[semanticSegment]
	//}

	//fmt.Printf("fields:\t%s\n", fields)

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

			err = STsConnArr[CacheIndex].Set(&stscache.Item{Key: starSegment, Value: remainValues, Time_start: startTime, Time_end: endTime, NumOfTables: numOfTab})

			if err != nil {
				//log.Printf("Error setting value: %v\nQUERY STRING:\t%s\n", err, queryString)
			} else {
				//log.Printf("STORED.")
			}

		} else { // 查数据库为空

			//num++
			//fmt.Println("miss number: ", num)

			// todo 对于数据库中没有的数据，向cache中插入空值
			singleSemanticSegment := GetSingleSegment(metric, partialSegment, tags)
			emptyValues := make([]byte, 0)
			for _, ss := range singleSemanticSegment {
				zero, _ := Int64ToByteArray(int64(0))
				emptyValues = append(emptyValues, []byte(ss)...)
				emptyValues = append(emptyValues, []byte(" ")...)
				emptyValues = append(emptyValues, zero...)
			}

			numOfTab := int64(len(singleSemanticSegment))
			err = STsConnArr[CacheIndex].Set(&stscache.Item{Key: starSegment, Value: emptyValues, Time_start: startTime, Time_end: endTime, NumOfTables: numOfTab})
			if err != nil {
				//log.Printf("Error setting value: %v\nQUERY STRING:\t%s\n", err, queryString)
			} else {
				//log.Printf("STORED.")
			}

			//fmt.Printf("\tdatabase miss 1:%s\n", queryString)
		}

		return resp, byteLength, hitKind

	} else { // 缓存部分命中或完全命中
		/* 把查询结果从字节流转换成 Response 结构 */
		convertedResponse, flagNum, flagArr, timeRangeArr, tagArr := ByteArrayToResponseWithDatatype(values, datatypes)

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

				//num++
				//fmt.Println("miss number: ", num)
				// todo 对于数据库中没有的数据，向cache中插入空值

				singleSemanticSegment := GetSingleSegment(metric, partialSegment, remainTags)
				emptyValues := make([]byte, 0)
				for _, ss := range singleSemanticSegment {
					zero, _ := Int64ToByteArray(int64(0))
					emptyValues = append(emptyValues, []byte(ss)...)
					emptyValues = append(emptyValues, []byte(" ")...)
					emptyValues = append(emptyValues, zero...)
				}

				numOfTab := int64(len(singleSemanticSegment))
				err = STsConnArr[CacheIndex].Set(&stscache.Item{Key: starSegment, Value: emptyValues, Time_start: startTime, Time_end: endTime, NumOfTables: numOfTab})
				if err != nil {
					//log.Printf("Error setting value: %v\nQUERY STRING:\t%s\n", err, queryString)
				} else {
					//log.Printf("STORED.")
				}

				//fmt.Printf("\tdatabase miss 2:%s\n", remainQueryString)

				return convertedResponse, byteLength, hitKind
			}

			//remainByteArr := ResponseToByteArray(remainResp, queryString)
			remainByteArr := RemainResponseToByteArrayWithParams(remainResp, datatypes, remainTags, metric, partialSegment)
			//remainByteArr := ResponseToByteArrayWithParams(remainResp, datatypes, remainTags, metric, partialSegment)
			//fmt.Println(remainQuery, "\nlen:", len(remainByteArr))

			numOfTableR := len(remainResp.Results)

			err = STsConnArr[CacheIndex].Set(&stscache.Item{
				Key:         starSegment,
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
