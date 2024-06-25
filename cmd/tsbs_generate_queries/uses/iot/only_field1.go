package iot

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type OnlyField1 struct {
	core utils.QueryGenerator
}

func NewOnlyField1(core utils.QueryGenerator) utils.QueryFiller {
	return &OnlyField1{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (i *OnlyField1) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := i.core.(OnlyField1Filler)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.OnlyField1(q, zipNum, latestNum, newOrOld)
	return q
}
