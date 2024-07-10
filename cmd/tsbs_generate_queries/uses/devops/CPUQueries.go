package devops

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type CPUQueries struct {
	core utils.QueryGenerator
}

func NewCPUQueries(core utils.QueryGenerator) utils.QueryFiller {
	return &CPUQueries{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (d *CPUQueries) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := d.core.(CPUQueriesFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.CPUQueries(q, zipNum, latestNum, newOrOld)
	return q
}
