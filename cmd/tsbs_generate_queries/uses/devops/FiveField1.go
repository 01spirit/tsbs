package devops

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type FiveField1 struct {
	core utils.QueryGenerator
}

func NewFiveField1(core utils.QueryGenerator) utils.QueryFiller {
	return &FiveField1{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (d *FiveField1) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := d.core.(FiveField1Filler)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.FiveField1(q, zipNum, latestNum, newOrOld)
	return q
}
