package input

import (
	"context"

	"github.com/stellar/stellar-etl/v2/internal/utils"

	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/xdr"
)

// GetLedgers returns a slice of ledger close metas for the ledgers in the provided range (inclusive on both ends)
func GetLedgers(start, end uint32, limit int64, env utils.EnvironmentDetails, useCaptiveCore bool) ([]utils.HistoryArchiveLedgerAndLCM, error) {
	ctx := context.Background()
	backend, err := utils.CreateLedgerBackend(ctx, useCaptiveCore, env)
	if err != nil {
		return []utils.HistoryArchiveLedgerAndLCM{}, err
	}

	ledgerSlice := []utils.HistoryArchiveLedgerAndLCM{}
	err = backend.PrepareRange(ctx, ledgerbackend.BoundedRange(start, end))
	panicIf(err)
	for seq := start; seq <= end; seq++ {
		lcm, err := backend.GetLedger(ctx, seq)
		if err != nil {
			return []utils.HistoryArchiveLedgerAndLCM{}, err
		}

		var ext xdr.TransactionHistoryEntryExt
		var transactionResultPair []xdr.TransactionResultPair

		switch lcm.V {
		case 0:
			ext = xdr.TransactionHistoryEntryExt{
				V:                0,
				GeneralizedTxSet: nil,
			}
			for _, transactionResultMeta := range lcm.V0.TxProcessing {
				transactionResultPair = append(transactionResultPair, transactionResultMeta.Result)
			}
		case 1:
			ext = xdr.TransactionHistoryEntryExt{
				V:                1,
				GeneralizedTxSet: &lcm.V1.TxSet,
			}
			for _, transactionResultMeta := range lcm.V1.TxProcessing {
				transactionResultPair = append(transactionResultPair, transactionResultMeta.Result)
			}
		}

		ledger := historyarchive.Ledger{
			Header: lcm.LedgerHeaderHistoryEntry(),
			Transaction: xdr.TransactionHistoryEntry{
				LedgerSeq: lcm.LedgerHeaderHistoryEntry().Header.LedgerSeq,
				TxSet: xdr.TransactionSet{
					PreviousLedgerHash: lcm.LedgerHeaderHistoryEntry().Header.PreviousLedgerHash,
					Txs:                lcm.TransactionEnvelopes(),
				},
				Ext: ext,
			},
			TransactionResult: xdr.TransactionHistoryResultEntry{
				LedgerSeq: lcm.LedgerHeaderHistoryEntry().Header.LedgerSeq,
				TxResultSet: xdr.TransactionResultSet{
					Results: transactionResultPair,
				},
				Ext: xdr.TransactionHistoryResultEntryExt{},
			},
		}

		ledgerLCM := utils.HistoryArchiveLedgerAndLCM{
			Ledger: ledger,
			LCM:    lcm,
		}

		ledgerSlice = append(ledgerSlice, ledgerLCM)
		if int64(len(ledgerSlice)) >= limit && limit >= 0 {
			break
		}
	}

	return ledgerSlice, nil
}
