package input

import (
	"context"
	"io"

	"github.com/stellar/go/exp/ingest/adapters"
	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/xdr"
)

// GetAccounts returns a slice of ledger close metas for the ledgers in the provided range (inclusive on both ends)
func GetAccounts(start, end uint32, limit int64) ([]xdr.LedgerEntry, error) {
	archiveStellarURL := "http://history.stellar.org/prd/core-live/core_live_001"
	archive, err := historyarchive.Connect(
		archiveStellarURL,
		historyarchive.ConnectOptions{Context: context.Background()},
	)
	if err != nil {
		return []xdr.LedgerEntry{}, err
	}

	historyAdapter := adapters.MakeHistoryArchiveAdapter(archive)
	latestNum, err := historyAdapter.GetLatestLedgerSequence()
	if err != nil {
		return []xdr.LedgerEntry{}, err
	}

	err = validateLedgerRange(start, end, latestNum)
	if err != nil {
		return []xdr.LedgerEntry{}, err
	}

	accSlice := []xdr.LedgerEntry{}
	for seq := start; seq <= end; seq++ {
		changeReader, err := historyAdapter.GetState(context.Background(), seq)
		if err != nil {
			return []xdr.LedgerEntry{}, err
		}

		for {
			change, err := changeReader.Read()
			if err == io.EOF {
				break
			}

			if err != nil {
				return []xdr.LedgerEntry{}, err
			}

			if change.Type == xdr.LedgerEntryTypeAccount {
				accSlice = append(accSlice, *change.Post)
				if int64(len(accSlice)) >= limit && limit >= 0 {
					break
				}
			}
		}

		changeReader.Close()
		if int64(len(accSlice)) >= limit && limit >= 0 {
			break
		}
	}

	return accSlice, nil
}
