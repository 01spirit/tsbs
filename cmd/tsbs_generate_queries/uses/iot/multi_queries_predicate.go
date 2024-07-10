package iot

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type MultiQueriesPredicate struct {
	core utils.QueryGenerator
}

func NewMultiQueriesPredicate(core utils.QueryGenerator) utils.QueryFiller {
	return &MultiQueriesPredicate{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (i *MultiQueriesPredicate) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := i.core.(MultiQueriesPredicateFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.MultiQueriesPredicate(q, zipNum, latestNum, newOrOld)
	return q
}
