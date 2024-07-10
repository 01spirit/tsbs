package devops

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type ThreeField2 struct {
	core utils.QueryGenerator
}

func NewThreeField2(core utils.QueryGenerator) utils.QueryFiller {
	return &ThreeField2{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (d *ThreeField2) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := d.core.(ThreeField2Filler)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.ThreeField2(q, zipNum, latestNum, newOrOld)
	return q
}
