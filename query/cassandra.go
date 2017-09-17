package query

import (
	"fmt"
	"sync"
	"time"
)

// Cassandra encodes a Cassandra request. This will be serialized for use
// by the query_benchmarker program.
type Cassandra struct {
	HumanLabel       []byte
	HumanDescription []byte
	ID               int64

	MeasurementName []byte // e.g. "cpu"
	FieldName       []byte // e.g. "usage_user"
	AggregationType []byte // e.g. "avg" or "sum". used literally in the cassandra query.
	TimeStart       time.Time
	TimeEnd         time.Time
	GroupByDuration time.Duration
	ForEveryN       []byte // e.g. "hostname,1"
	WhereClause     []byte // e.g. "usage_user,>,90.0"
	OrderBy         []byte // e.g. "timestamp_ns DESC"
	Limit           int
	TagSets         [][]string // semantically, each subgroup is OR'ed and they are all AND'ed together
}

//CassandraPool is a sync.Pool of Cassandra Query types
var CassandraPool sync.Pool = sync.Pool{
	New: func() interface{} {
		return &Cassandra{
			HumanLabel:       []byte{},
			HumanDescription: []byte{},
			MeasurementName:  []byte{},
			FieldName:        []byte{},
			AggregationType:  []byte{},
			ForEveryN:        []byte{},
			WhereClause:      []byte{},
			OrderBy:          []byte{},
			TagSets:          [][]string{},
		}
	},
}

// NewCassandra returns a new Cassandra Query instance
func NewCassandra() *Cassandra {
	return CassandraPool.Get().(*Cassandra)
}

// String produces a debug-ready description of a Query.
func (q *Cassandra) String() string {
	return fmt.Sprintf("HumanLabel: %s, HumanDescription: %s, MeasurementName: %s, AggregationType: %s, TimeStart: %s, TimeEnd: %s, GroupByDuration: %s, TagSets: %s", q.HumanLabel, q.HumanDescription, q.MeasurementName, q.AggregationType, q.TimeStart, q.TimeEnd, q.GroupByDuration, q.TagSets)
}

// HumanLabelName returns the human readable name of this Query
func (q *Cassandra) HumanLabelName() []byte {
	return q.HumanLabel
}

// HumanDescriptionName returns the human readable description of this Query
func (q *Cassandra) HumanDescriptionName() []byte {
	return q.HumanDescription
}

func (q *Cassandra) Release() {
	q.HumanLabel = q.HumanLabel[:0]
	q.HumanDescription = q.HumanDescription[:0]

	q.MeasurementName = q.MeasurementName[:0]
	q.FieldName = q.FieldName[:0]
	q.AggregationType = q.AggregationType[:0]
	q.GroupByDuration = 0
	q.TimeStart = time.Time{}
	q.TimeEnd = time.Time{}
	q.ForEveryN = q.ForEveryN[:0]
	q.WhereClause = q.WhereClause[:0]
	q.OrderBy = q.OrderBy[:0]
	q.Limit = 0
	q.TagSets = q.TagSets[:0]

	CassandraPool.Put(q)
}