package iot

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

// SimpleIoT 自己实现的简单的查询
type ReadingsAvgFuelConsumption struct {
	core utils.QueryGenerator
}

func NewReadingsAvgFuelConsumption(core utils.QueryGenerator) utils.QueryFiller {
	return &ReadingsAvgFuelConsumption{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (i *ReadingsAvgFuelConsumption) Fill(q query.Query, zipNum int64, latestNum int64) query.Query {
	fc, ok := i.core.(ReadingsAvgFuelConsumptionFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.ReadingsAvgFuelConsumption(q, zipNum, latestNum)
	return q
}
