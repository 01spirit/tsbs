package iot

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type ReadingsVelocityAndFuel2 struct {
	core utils.QueryGenerator
}

func NewReadingsVelocityAndFuel2(core utils.QueryGenerator) utils.QueryFiller {
	return &ReadingsVelocityAndFuel2{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (i *ReadingsVelocityAndFuel2) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := i.core.(ReadingsVelocityAndFuel2Filler)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.ReadingsVelocityAndFuel2(q, zipNum, latestNum, newOrOld)
	return q
}
