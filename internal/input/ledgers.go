package input

import (
	"context"

	"github.com/stellar/stellar-etl/internal/utils"

	"github.com/stellar/go/xdr"
)

// GetLedgers returns a slice of ledger close metas for the ledgers in the provided range (inclusive on both ends)
func GetLedgers(start, end uint32, limit int64, isTest bool) ([]xdr.LedgerCloseMeta, error) {
	env := utils.GetEnvironmentDetails(isTest)
	backend, err := utils.CreateBackend(start, end, env.ArchiveURLs)
	if err != nil {
		return []xdr.LedgerCloseMeta{}, err
	}

	metaSlice := []xdr.LedgerCloseMeta{}
	ctx := context.Background()
	for seq := start; seq <= end; seq++ {
		ledger, err := backend.GetLedger(ctx, seq)
		if err != nil {
			return []xdr.LedgerCloseMeta{}, err
		}

		metaSlice = append(metaSlice, ledger)
		if int64(len(metaSlice)) >= limit && limit >= 0 {
			break
		}
	}

	return metaSlice, nil
}
