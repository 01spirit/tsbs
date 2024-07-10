package devops

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type TenFieldWithPredicate struct {
	core utils.QueryGenerator
}

func NewTenFieldWithPredicate(core utils.QueryGenerator) utils.QueryFiller {
	return &TenFieldWithPredicate{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (d *TenFieldWithPredicate) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := d.core.(TenFieldWithPredicateFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.TenFieldWithPredicate(q, zipNum, latestNum, newOrOld)
	return q
}
