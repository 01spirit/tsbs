package iot

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type DiagnosticsLoad struct {
	core utils.QueryGenerator
}

func NewDiagnosticsLoad(core utils.QueryGenerator) utils.QueryFiller {
	return &DiagnosticsLoad{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (i *DiagnosticsLoad) Fill(q query.Query, zipNum int64, latestNum int64) query.Query {
	fc, ok := i.core.(DiagnosticsLoadFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.DiagnosticsLoad(q, 10, zipNum, latestNum)
	return q
}
