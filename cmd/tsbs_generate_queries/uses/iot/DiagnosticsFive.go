package iot

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type DiagnosticsFive struct {
	core utils.QueryGenerator
}

func NewDiagnosticsFive(core utils.QueryGenerator) utils.QueryFiller {
	return &DiagnosticsFive{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (i *DiagnosticsFive) Fill(q query.Query, zipNum int64, latestNum int64, newOrOld int) query.Query {
	fc, ok := i.core.(DiagnosticsFiveFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.DiagnosticsFive(q, zipNum, latestNum, newOrOld)
	return q
}
