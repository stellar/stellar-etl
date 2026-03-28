package input

import (
	"context"

	"github.com/stellar/stellar-etl/v2/internal/utils"
)

// GetLedgersHistoryArchive returns a slice of ledger close metas for the ledgers in the provided range (inclusive on both ends)
func GetLedgersHistoryArchive(start, end uint32, limit int64, env utils.EnvironmentDetails, useCaptiveCore bool) ([]utils.HistoryArchiveLedgerAndLCM, error) {
	backend, err := utils.CreateBackend(start, end, env.ArchiveURLs)
	if err != nil {
		return []utils.HistoryArchiveLedgerAndLCM{}, err
	}

	ledgerSlice := []utils.HistoryArchiveLedgerAndLCM{}
	ctx := context.Background()
	for seq := start; seq <= end; seq++ {
		ledger, err := backend.GetLedgerArchive(ctx, seq)
		if err != nil {
			return []utils.HistoryArchiveLedgerAndLCM{}, err
		}

		ledgerLCM := utils.HistoryArchiveLedgerAndLCM{
			Ledger: ledger,
		}

		ledgerSlice = append(ledgerSlice, ledgerLCM)
		if int64(len(ledgerSlice)) >= limit && limit >= 0 {
			break
		}
	}

	return ledgerSlice, nil
}
