package input

import (
	"context"

	"github.com/stellar/stellar-etl/internal/utils"

	"github.com/stellar/go/historyarchive"
)

// GetLedgers returns a slice of ledger close metas for the ledgers in the provided range (inclusive on both ends)
func GetLedgers(start, end uint32, limit int64, isTest bool, isFuturenet bool) ([]historyarchive.Ledger, error) {
	env := utils.GetEnvironmentDetails(isTest, isFuturenet)
	backend, err := utils.CreateBackend(start, end, env.ArchiveURLs)
	if err != nil {
		return []historyarchive.Ledger{}, err
	}

	ledgerSlice := []historyarchive.Ledger{}
	ctx := context.Background()
	for seq := start; seq <= end; seq++ {
		ledger, err := backend.GetLedgerArchive(ctx, seq)
		if err != nil {
			return []historyarchive.Ledger{}, err
		}

		ledgerSlice = append(ledgerSlice, ledger)
		if int64(len(ledgerSlice)) >= limit && limit >= 0 {
			break
		}
	}

	return ledgerSlice, nil
}
