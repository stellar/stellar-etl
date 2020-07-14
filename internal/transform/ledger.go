package transform

import (
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/internal/utils"
)

//ConvertLedger converts a ledger from the history archive ingestion system into a form suitable for BigQuery
func ConvertLedger(inputLedgerMeta xdr.LedgerCloseMeta) LedgerOutput {
	ledger := inputLedgerMeta.V0
	ledgerHeader := ledger.LedgerHeader
	transformedLedger := LedgerOutput{
		Sequence:           int32(ledgerHeader.Header.LedgerSeq),
		LedgerHash:         utils.HashToHexString(ledgerHeader.Hash),
		PreviousLedgerHash: utils.HashToHexString(ledgerHeader.Header.PreviousLedgerHash),
	}
	return transformedLedger
}
