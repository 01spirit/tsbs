package iot

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type ReadingsPosition struct {
	core utils.QueryGenerator
}

func NewReadingsPosition(core utils.QueryGenerator) utils.QueryFiller {
	return &ReadingsPosition{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (i *ReadingsPosition) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := i.core.(ReadingsPositionFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.ReadingsPosition(q, zipNum, latestNum, newOrOld)
	return q
}
