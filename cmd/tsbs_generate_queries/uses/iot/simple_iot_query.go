package iot

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

// SimpleIoT 自己实现的简单的查询
type SimpleIoT struct {
	core utils.QueryGenerator
}

func NewSimpleIoT(core utils.QueryGenerator) utils.QueryFiller {
	return &SimpleIoT{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (i *SimpleIoT) Fill(q query.Query) query.Query {
	fc, ok := i.core.(SimpleIoTFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.SimpleIoT(q)
	return q
}
