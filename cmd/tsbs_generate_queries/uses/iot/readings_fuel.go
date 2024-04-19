package iot

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

// SimpleIoT 自己实现的简单的查询
type ReadingsFuel struct {
	core utils.QueryGenerator
}

func NewReadingsFuel(core utils.QueryGenerator) utils.QueryFiller {
	return &ReadingsFuel{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (i *ReadingsFuel) Fill(q query.Query, zipNum int64, latestNum int64) query.Query {
	fc, ok := i.core.(ReadingsFuelFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.ReadingsFuel(q, zipNum, latestNum)
	return q
}
