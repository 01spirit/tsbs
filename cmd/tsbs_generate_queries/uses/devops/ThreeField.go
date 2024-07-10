package devops

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type ThreeField struct {
	core utils.QueryGenerator
}

func NewThreeField(core utils.QueryGenerator) utils.QueryFiller {
	return &ThreeField{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (d *ThreeField) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := d.core.(ThreeFieldFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.ThreeField(q, zipNum, latestNum, newOrOld)
	return q
}
