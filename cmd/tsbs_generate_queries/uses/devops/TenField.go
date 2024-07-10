package devops

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type TenField struct {
	core utils.QueryGenerator
}

func NewTenField(core utils.QueryGenerator) utils.QueryFiller {
	return &TenField{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (d *TenField) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := d.core.(TenFieldFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.TenField(q, zipNum, latestNum, newOrOld)
	return q
}
