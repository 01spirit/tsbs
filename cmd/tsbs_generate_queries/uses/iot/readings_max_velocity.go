package iot

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

// SimpleIoT 自己实现的简单的查询
type ReadingsMaxVelocity struct {
	core utils.QueryGenerator
}

func NewReadingsMaxVelocity(core utils.QueryGenerator) utils.QueryFiller {
	return &ReadingsMaxVelocity{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (i *ReadingsMaxVelocity) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := i.core.(ReadingsMaxVelocityFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.ReadingsMaxVelocity(q, zipNum, latestNum)
	return q
}
