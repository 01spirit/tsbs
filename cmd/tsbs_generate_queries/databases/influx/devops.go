package influx

import (
	"fmt"
	"strings"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/databases"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/pkg/query"
)

// Devops produces Influx-specific queries for all the devops query types.
type Devops struct {
	*BaseGenerator
	*devops.Core
}

func (d *Devops) getHostWhereWithHostnames(hostnames []string) string {
	hostnameClauses := []string{}
	for _, s := range hostnames {
		hostnameClauses = append(hostnameClauses, fmt.Sprintf("\"hostname\" = '%s'", s))
	}

	combinedHostnameClause := strings.Join(hostnameClauses, " or ")
	return "(" + combinedHostnameClause + ")"
}

func (d *Devops) getHostWhereString(nHosts int) string {
	hostnames, err := d.GetRandomHosts(nHosts)
	databases.PanicIfErr(err)
	return d.getHostWhereWithHostnames(hostnames)
}

func (d *Devops) getSelectClausesAggMetrics(agg string, metrics []string) []string {
	selectClauses := make([]string, len(metrics))
	for i, m := range metrics {
		selectClauses[i] = fmt.Sprintf("%s(%s)", agg, m)
	}

	return selectClauses
}

// GroupByTime selects the MAX for numMetrics metrics under 'cpu',
// per minute for nhosts hosts,
// e.g. in pseudo-SQL:
//
// SELECT minute, max(metric1), ..., max(metricN)
// FROM cpu
// WHERE (hostname = '$HOSTNAME_1' OR ... OR hostname = '$HOSTNAME_N')
// AND time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY minute ORDER BY minute ASC
func (d *Devops) GroupByTime(qi query.Query, nHosts, numMetrics int, timeRange time.Duration) {
	interval := d.Interval.MustRandWindow(timeRange)
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	databases.PanicIfErr(err)
	selectClauses := d.getSelectClausesAggMetrics("max", metrics)
	whereHosts := d.getHostWhereString(nHosts)

	humanLabel := fmt.Sprintf("Influx %d cpu metric(s), random %4d hosts, random %s by 1m", numMetrics, nHosts, timeRange)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	influxql := fmt.Sprintf("SELECT %s from cpu where %s and time >= '%s' and time <= '%s' group by time(1m)", strings.Join(selectClauses, ", "), whereHosts, interval.StartString(), interval.EndString())
	d.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

// GroupByOrderByLimit benchmarks a query that has a time WHERE clause, that groups by a truncated date, orders by that date, and takes a limit:
// SELECT date_trunc('minute', time) AS t, MAX(cpu) FROM cpu
// WHERE time < '$TIME'
// GROUP BY t ORDER BY t DESC
// LIMIT $LIMIT
func (d *Devops) GroupByOrderByLimit(qi query.Query) {
	interval := d.Interval.MustRandWindow(time.Hour)
	where := fmt.Sprintf("WHERE time < '%s'", interval.EndString())

	humanLabel := "Influx max cpu over last 5 min-intervals (random end)"
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	influxql := fmt.Sprintf(`SELECT max(usage_user) from cpu %s group by time(1m) limit 5`, where)
	d.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

// GroupByTimeAndPrimaryTag selects the AVG of numMetrics metrics under 'cpu' per device per hour for a day,
// e.g. in pseudo-SQL:
//
// SELECT AVG(metric1), ..., AVG(metricN)
// FROM cpu
// WHERE time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY hour, hostname ORDER BY hour, hostname
func (d *Devops) GroupByTimeAndPrimaryTag(qi query.Query, numMetrics int) {
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	databases.PanicIfErr(err)
	interval := d.Interval.MustRandWindow(devops.DoubleGroupByDuration)
	selectClauses := d.getSelectClausesAggMetrics("mean", metrics)

	humanLabel := devops.GetDoubleGroupByLabel("Influx", numMetrics)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	influxql := fmt.Sprintf("SELECT %s from cpu where time >= '%s' and time <= '%s' group by time(1h),hostname", strings.Join(selectClauses, ", "), interval.StartString(), interval.EndString())
	d.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

// MaxAllCPU selects the MAX of all metrics under 'cpu' per hour for nhosts hosts,
// e.g. in pseudo-SQL:
//
// SELECT MAX(metric1), ..., MAX(metricN)
// FROM cpu WHERE (hostname = '$HOSTNAME_1' OR ... OR hostname = '$HOSTNAME_N')
// AND time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY hour ORDER BY hour
func (d *Devops) MaxAllCPU(qi query.Query, nHosts int, duration time.Duration) {
	interval := d.Interval.MustRandWindow(duration)
	whereHosts := d.getHostWhereString(nHosts)
	selectClauses := d.getSelectClausesAggMetrics("max", devops.GetAllCPUMetrics())

	humanLabel := devops.GetMaxAllLabel("Influx", nHosts)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	influxql := fmt.Sprintf("SELECT %s from cpu where %s and time >= '%s' and time <= '%s' group by time(1h)", strings.Join(selectClauses, ","), whereHosts, interval.StartString(), interval.EndString())
	d.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

// LastPointPerHost finds the last row for every host in the dataset
func (d *Devops) LastPointPerHost(qi query.Query) {
	humanLabel := "Influx last row per host"
	humanDesc := humanLabel + ": cpu"
	influxql := "SELECT * from cpu group by \"hostname\" order by time desc limit 1"
	d.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

// HighCPUForHosts populates a query that gets CPU metrics when the CPU has high
// usage between a time period for a number of hosts (if 0, it will search all hosts),
// e.g. in pseudo-SQL:
//
// SELECT * FROM cpu
// WHERE usage_user > 90.0
// AND time >= '$TIME_START' AND time < '$TIME_END'
// AND (hostname = '$HOST' OR hostname = '$HOST2'...)
func (d *Devops) HighCPUForHosts(qi query.Query, nHosts int) {
	interval := d.Interval.MustRandWindow(devops.HighCPUDuration)

	var hostWhereClause string
	if nHosts == 0 {
		hostWhereClause = ""
	} else {
		hostWhereClause = fmt.Sprintf("and %s", d.getHostWhereString(nHosts))
	}

	humanLabel, err := devops.GetHighCPULabel("Influx", nHosts)
	databases.PanicIfErr(err)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	influxql := fmt.Sprintf("SELECT * from cpu where usage_user > 90.0 %s and time >= '%s' and time <= '%s'", hostWhereClause, interval.StartString(), interval.EndString())
	d.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (d *Devops) SimpleCPU(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := d.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	var influxql string

	if zipNum < 5 {
		influxql = fmt.Sprintf(
			`SELECT mean(usage_nice),mean(usage_steal),mean(usage_guest) FROM "cpu" WHERE %s AND TIME >= '%s' AND TIME < '%s' GROUP BY "hostname",time(5m)`,
			d.getHostWhereString(TagNum), interval.StartString(), interval.EndString())
	} else {
		influxql = fmt.Sprintf(
			`SELECT mean(usage_nice),mean(usage_steal),mean(usage_guest) FROM "cpu" WHERE %s AND TIME >= '%s' AND TIME < '%s' GROUP BY "hostname",time(15m)`,
			d.getHostWhereString(TagNum), interval.StartString(), interval.EndString())
	}

	humanLabel := "Influx Simple CPU"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (d *Devops) ThreeField3(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := d.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	var influxql string

	if zipNum < 5 {
		influxql = fmt.Sprintf(
			`SELECT mean(usage_system),mean(usage_idle),mean(usage_nice) FROM "cpu" WHERE %s AND TIME >= '%s' AND TIME < '%s' GROUP BY "hostname",time(15m)`,
			d.getHostWhereString(TagNum), interval.StartString(), interval.EndString())
	} else {
		influxql = fmt.Sprintf(
			`SELECT mean(usage_system),mean(usage_idle),mean(usage_nice) FROM "cpu" WHERE %s AND TIME >= '%s' AND TIME < '%s' GROUP BY "hostname",time(60m)`,
			d.getHostWhereString(TagNum), interval.StartString(), interval.EndString())
	}

	humanLabel := "Influx Three Field 3"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (d *Devops) FiveField1(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := d.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	var influxql string

	if zipNum < 5 {
		influxql = fmt.Sprintf(
			`SELECT mean(usage_user),mean(usage_system),mean(usage_idle),mean(usage_nice),mean(usage_iowait) FROM "cpu" WHERE %s AND TIME >= '%s' AND TIME < '%s' GROUP BY "hostname",time(15m)`,
			d.getHostWhereString(TagNum), interval.StartString(), interval.EndString())
	} else {
		influxql = fmt.Sprintf(
			`SELECT mean(usage_user),mean(usage_system),mean(usage_idle),mean(usage_nice),mean(usage_iowait) FROM "cpu" WHERE %s AND TIME >= '%s' AND TIME < '%s' GROUP BY "hostname",time(60m)`,
			d.getHostWhereString(TagNum), interval.StartString(), interval.EndString())
	}

	humanLabel := "Influx Five Field 1"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (d *Devops) ThreeField(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := d.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	var influxql string

	if zipNum < 5 {
		influxql = fmt.Sprintf(
			`SELECT mean(usage_user),mean(usage_system),mean(usage_idle) FROM "cpu" WHERE %s AND TIME >= '%s' AND TIME < '%s' GROUP BY "hostname",time(15m)`,
			d.getHostWhereString(TagNum), interval.StartString(), interval.EndString())
	} else {
		influxql = fmt.Sprintf(
			`SELECT mean(usage_user),mean(usage_system),mean(usage_idle) FROM "cpu" WHERE %s AND TIME >= '%s' AND TIME < '%s' GROUP BY "hostname",time(60m)`,
			d.getHostWhereString(TagNum), interval.StartString(), interval.EndString())
	}

	humanLabel := "Influx Three Field"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (d *Devops) ThreeField2(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := d.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	var influxql string

	if zipNum < 5 {
		influxql = fmt.Sprintf(
			`SELECT mean(usage_idle),mean(usage_nice),mean(usage_iowait) FROM "cpu" WHERE %s AND TIME >= '%s' AND TIME < '%s' GROUP BY "hostname",time(15m)`,
			d.getHostWhereString(TagNum), interval.StartString(), interval.EndString())
	} else {
		influxql = fmt.Sprintf(
			`SELECT mean(usage_idle),mean(usage_nice),mean(usage_iowait) FROM "cpu" WHERE %s AND TIME >= '%s' AND TIME < '%s' GROUP BY "hostname",time(60m)`,
			d.getHostWhereString(TagNum), interval.StartString(), interval.EndString())
	}

	humanLabel := "Influx Three Field 2"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (d *Devops) TenField(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := d.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	var influxql string

	if zipNum < 5 {
		influxql = fmt.Sprintf(
			`SELECT mean(usage_user),mean(usage_system),mean(usage_idle),mean(usage_nice),mean(usage_iowait),mean(usage_irq),mean(usage_softirq),mean(usage_steal),mean(usage_guest),mean(usage_guest_nice) FROM "cpu" WHERE %s AND TIME >= '%s' AND TIME < '%s' GROUP BY "hostname",time(15m)`,
			d.getHostWhereString(TagNum), interval.StartString(), interval.EndString())
	} else {
		influxql = fmt.Sprintf(
			`SELECT mean(usage_user),mean(usage_system),mean(usage_idle),mean(usage_nice),mean(usage_iowait),mean(usage_irq),mean(usage_softirq),mean(usage_steal),mean(usage_guest),mean(usage_guest_nice) FROM "cpu" WHERE %s AND TIME >= '%s' AND TIME < '%s' GROUP BY "hostname",time(60m)`,
			d.getHostWhereString(TagNum), interval.StartString(), interval.EndString())
	}

	humanLabel := "Influx Ten Field "
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (d *Devops) TenFieldWithPredicate(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	interval := d.Interval.DistributionRandWithOldData(zipNum, latestNum, newOrOld)
	var influxql string

	if zipNum < 5 {
		influxql = fmt.Sprintf(
			`SELECT usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice FROM "cpu" WHERE %s AND usage_user > 90 AND usage_guest > 90 AND TIME >= '%s' AND TIME < '%s' GROUP BY "hostname"`,
			d.getHostWhereString(TagNum), interval.StartString(), interval.EndString())
	} else {
		influxql = fmt.Sprintf(
			`SELECT usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice FROM "cpu" WHERE %s AND usage_user > 90 AND usage_guest > 90 AND TIME >= '%s' AND TIME < '%s' GROUP BY "hostname"`,
			d.getHostWhereString(TagNum), interval.StartString(), interval.EndString())
	}

	humanLabel := "Influx Ten Field with Predicate"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (d *Devops) CPUQueries(qi query.Query, zipNum int64, latestNum int64, newOrOld int) {
	switch index % 6 {
	case 0:
		d.ThreeField3(qi, zipNum, latestNum, newOrOld)
		break
	case 1:
		d.FiveField1(qi, zipNum, latestNum, newOrOld)
		break
	case 2:
		d.ThreeField(qi, zipNum, latestNum, newOrOld)
		break
	case 3:
		d.ThreeField2(qi, zipNum, latestNum, newOrOld)
		break
	case 4:
		d.TenField(qi, zipNum, latestNum, newOrOld)
		break
	case 5:
		d.TenFieldWithPredicate(qi, zipNum, latestNum, newOrOld)
		break
	default:
		d.FiveField1(qi, zipNum, latestNum, newOrOld)
		break
	}

	fmt.Printf("number:\t%d\n", index)
	index++
}
