package iot

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type OnlyFieldQueries struct {
	core utils.QueryGenerator
}

func NewOnlyFieldQueries(core utils.QueryGenerator) utils.QueryFiller {
	return &OnlyFieldQueries{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (i *OnlyFieldQueries) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := i.core.(OnlyFieldQueriesFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.OnlyFieldQueries(q, zipNum, latestNum, newOrOld)
	return q
}
