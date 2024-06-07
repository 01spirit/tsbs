package utils

import (
	"fmt"
	client "github.com/timescale/tsbs/InfluxDB-client/v2"
	"math/rand"
	"time"
)

const (
	// ErrEndBeforeStart is the error message for when a TimeInterval's end time
	// would be before its start.
	ErrEndBeforeStart = "end time before start time"

	errWindowTooLargeFmt = "random window equal to or larger than TimeInterval: window %v, interval %v"

	minute = time.Minute
	hour   = time.Hour
	day    = 24 * time.Hour
	week   = 7 * day
	month  = 4*week + 2*day
)

var ZipFianTimeDuration = []time.Duration{
	6 * hour, 12 * hour, 1 * day, 2 * day, 3 * day, 5 * day, 1 * week, 2 * week, 3 * week, 1 * month,
}

//var ZipFianTimeDuration = []time.Duration{
//	day, 12 * hour, 6 * hour, 2 * hour, hour, 2 * day, 3 * day, 4 * day, 5 * day, week,
//}

// TimeInterval represents an interval of time in UTC. That is, regardless of
// what timezone(s) are used for the beginning and end times, they will be
// converted to UTC and methods will return them as such.
type TimeInterval struct {
	start time.Time
	end   time.Time
}

// NewTimeInterval creates a new TimeInterval for a given start and end. If end
// is a time.Time before start, then an error is returned.
func NewTimeInterval(start, end time.Time) (*TimeInterval, error) {
	if end.Before(start) {
		return nil, fmt.Errorf(ErrEndBeforeStart)
	}
	return &TimeInterval{start.UTC(), end.UTC()}, nil
}

// Duration returns the time.Duration of the TimeInterval.
func (ti *TimeInterval) Duration() time.Duration {
	return ti.end.Sub(ti.start)
}

// Overlap detects whether the given TimeInterval overlaps with this
// TimeInterval, assuming an inclusive start boundary and exclusive end
// boundary.
func (ti *TimeInterval) Overlap(other *TimeInterval) bool {
	s1 := ti.Start()
	e1 := ti.End()

	s2 := other.Start()
	e2 := other.End()

	// If the two TimeIntervals share opposite boundaries, then they do not
	// overlap since the end is exclusive
	if e1 == s2 || e2 == s1 {
		return false
	}

	// If the start and end of the first are both before the start of the
	// second, they do not overlap.
	if s1.Before(s2) && e1.Before(s2) {
		return false
	}

	// Same as the previous case, just reversed.
	if s2.Before(s1) && e2.Before(s1) {
		return false
	}

	// Everything else must overlap
	return true
}

// RandWindow creates a TimeInterval of duration `window` at a uniformly-random
// start time within the time period represented by this TimeInterval.
func (ti *TimeInterval) RandWindow(window time.Duration) (*TimeInterval, error) {
	lower := ti.start.UnixNano()
	upper := ti.end.Add(-window).UnixNano()

	if upper <= lower {
		return nil, fmt.Errorf(errWindowTooLargeFmt, window, ti.end.Sub(ti.start))

	}

	start := lower + rand.Int63n(upper-lower)
	end := start + window.Nanoseconds()

	x, err := NewTimeInterval(time.Unix(0, start), time.Unix(0, end))
	if err != nil {
		return nil, err
	} else if x.Duration() != window {
		// Unless the logic above this changes, this should not happen, so
		// we panic in that case.
		panic("generated TimeInterval's duration does not equal window")
	}

	return x, nil
}

// MustRandWindow is the form of RandWindow that cannot error; if it does error,
// it causes a panic.
func (ti *TimeInterval) MustRandWindow(window time.Duration) *TimeInterval {
	res, err := ti.RandWindow(window)
	if err != nil {
		panic(err.Error())
	}
	return res
}

// DistributionRand 用 Latest分布 生成查询的结束时间， 用 Zipfian分布 生成查询的时间区间长度，并以此求出查询的起始时间
func (ti *TimeInterval) DistributionRand(zipNum int64, latestNum int64) *TimeInterval {
	duration := ZipFianTimeDuration[zipNum].Nanoseconds() // Zipfian分布生成时间区间
	totalStartTime := ti.start.UnixNano()                 // 启动项参数中设置的整体查询的 起始时间 和 结束时间
	totalEndTime := ti.end.UnixNano() - 1
	//fmt.Println(ti.end)

	queryEndTime := totalEndTime - ((time.Hour.Nanoseconds() * 24) * (5*365 - latestNum - 1)) // Latest分布生成结束时间
	queryStartTime := queryEndTime - duration

	if queryStartTime < totalStartTime {
		queryStartTime = totalStartTime
	}

	if queryEndTime <= queryStartTime {
		queryEndTime = totalEndTime
		queryStartTime = queryEndTime - 24*time.Hour.Nanoseconds()
		//queryEndTime = queryStartTime + 24*time.Hour.Nanoseconds()
	}

	x, err := NewTimeInterval(time.Unix(0, queryStartTime), time.Unix(0, queryEndTime))
	if err != nil {
		panic(err.Error())
	}

	return x
}

func (ti *TimeInterval) DistributionRandWithOldData(zipNum int64, latestNum int64, newOrOld int) *TimeInterval {
	duration := ZipFianTimeDuration[zipNum].Nanoseconds() // Zipfian分布生成时间区间
	// 启动项参数中设置的整体查询的 起始时间 和 结束时间

	if newOrOld == 0 {
		totalStartTime := ti.start.UnixNano()
		totalEndTime := ti.end.UnixNano() - 1
		//fmt.Println(ti.end)
		//fmt.Printf("start time:\t%d\tend time:\t%d\n", totalStartTime, totalEndTime)
		fmt.Printf("start time:\t%s\tend time:\t%s\n", client.NanoTimeInt64ToString(totalStartTime), client.NanoTimeInt64ToString(totalEndTime))

		queryEndTime := totalEndTime - ((time.Hour.Nanoseconds() * 12) * (365*2 - latestNum - 1)) // Latest分布生成结束时间
		queryStartTime := queryEndTime - duration
		fmt.Printf("start time:\t%s\tend time:\t%s\n", client.NanoTimeInt64ToString(queryStartTime), client.NanoTimeInt64ToString(queryEndTime))
		if queryStartTime < totalStartTime {
			queryStartTime = totalStartTime
		}

		if queryEndTime <= queryStartTime {
			queryEndTime = totalEndTime
			queryStartTime = queryEndTime - 24*time.Hour.Nanoseconds()
			//queryEndTime = queryStartTime + 24*time.Hour.Nanoseconds()
		}

		x, err := NewTimeInterval(time.Unix(0, queryStartTime), time.Unix(0, queryEndTime))
		if err != nil {
			panic(err.Error())
		}

		fmt.Printf("zipnum:\t%d\tlatestnum:\t%d\tnew:\t%d\n", zipNum, latestNum, newOrOld)
		fmt.Printf("start time:\t%s\tend time:\t%s\n", client.NanoTimeInt64ToString(x.start.UnixNano()), client.NanoTimeInt64ToString(x.end.UnixNano()))
		return x
	} else {
		totalStartTime := ti.start.UnixNano()
		totalEndTime := totalStartTime + time.Hour.Nanoseconds()*24*90

		queryEndTime := totalEndTime - ((time.Hour.Nanoseconds() * 12) * (90*2 - latestNum - 1)) // Latest分布生成结束时间
		queryStartTime := queryEndTime - duration
		if queryStartTime < totalStartTime {
			queryStartTime = totalStartTime
		}

		if queryEndTime <= queryStartTime {
			queryEndTime = totalEndTime
			queryStartTime = queryEndTime - 24*time.Hour.Nanoseconds()
			//queryEndTime = queryStartTime + 24*time.Hour.Nanoseconds()
		}

		x, err := NewTimeInterval(time.Unix(0, queryStartTime), time.Unix(0, queryEndTime))
		if err != nil {
			panic(err.Error())
		}

		fmt.Printf("zipnum:\t%d\tlatestnum:\t%d\tnew:\t%d\n", zipNum, latestNum, newOrOld)
		fmt.Printf("start time:\t%s\tend time:\t%s\n", client.NanoTimeInt64ToString(x.start.UnixNano()), client.NanoTimeInt64ToString(x.end.UnixNano()))
		return x
	}

}

// Start returns the starting time in UTC.
func (ti *TimeInterval) Start() time.Time {
	return ti.start
}

// StartUnixNano returns the start time as nanoseconds.
func (ti *TimeInterval) StartUnixNano() int64 {
	return ti.start.UnixNano()
}

// StartUnixMillis returns the start time as milliseconds.
func (ti *TimeInterval) StartUnixMillis() int64 {
	return ti.start.UTC().UnixNano() / int64(time.Millisecond)
}

// StartString formats the start  of the TimeInterval according to RFC3339.
func (ti *TimeInterval) StartString() string {
	return ti.start.Format(time.RFC3339)
}

// End returns the starting time in UTC.
func (ti *TimeInterval) End() time.Time {
	return ti.end
}

// EndUnixNano returns the end time as nanoseconds.
func (ti *TimeInterval) EndUnixNano() int64 {
	return ti.end.UnixNano()
}

// EndUnixMillis returns the end time as milliseconds.
func (ti *TimeInterval) EndUnixMillis() int64 {
	return ti.end.UTC().UnixNano() / int64(time.Millisecond)
}

// EndString formats the end of the TimeInterval according to RFC3339.
func (ti *TimeInterval) EndString() string {
	return ti.end.Format(time.RFC3339)
}
