package iot

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type ReadingsVelocityPredicate struct {
	core utils.QueryGenerator
}

func NewReadingsVelocityPredicate(core utils.QueryGenerator) utils.QueryFiller {
	return &ReadingsVelocityPredicate{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (i *ReadingsVelocityPredicate) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := i.core.(ReadingsVelocityPredicateFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.ReadingsVelocityPredicate(q, zipNum, latestNum, newOrOld)
	return q
}
