package devops

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type SimpleCPU struct {
	core utils.QueryGenerator
}

func NewSimpleCPU(core utils.QueryGenerator) utils.QueryFiller {
	return &SimpleCPU{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (d *SimpleCPU) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := d.core.(SimpleCPUFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.SimpleCPU(q, zipNum, latestNum, newOrOld)
	return q
}
