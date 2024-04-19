package iot

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type DiagnosticsFuel struct {
	core utils.QueryGenerator
}

func NewDiagnosticsFuel(core utils.QueryGenerator) utils.QueryFiller {
	return &DiagnosticsFuel{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (i *DiagnosticsFuel) Fill(q query.Query, zipNum int64, latestNum int64) query.Query {
	fc, ok := i.core.(DiagnosticsFuelFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.DiagnosticsFuel(q, zipNum, latestNum)
	return q
}
