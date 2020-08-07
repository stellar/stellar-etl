package input

import (
	"context"

	"github.com/stellar/go/network"

	"github.com/stellar/stellar-etl/internal/utils"

	"github.com/stellar/go/exp/ingest/adapters"
	ingestio "github.com/stellar/go/exp/ingest/io"
	"github.com/stellar/go/exp/ingest/ledgerbackend"
	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/xdr"
)

var archiveStellarURL = "http://history.stellar.org/prd/core-live/core_live_001"

// GetAccounts returns a slice of ledger close metas for the ledgers in the provided range (inclusive on both ends)
func GetAccounts(start, end uint32, limit int64, execPath, configPath string) ([]xdr.LedgerEntry, error) {
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

	// If the start is set to the genesis ledger, everything is read from the bucket list up to the nearest checkpoint. The limit flag is ignored
	if start == 1 {
		checkpointSeq, err := utils.GetCheckpointNum(end, latestNum)
		if err != nil {
			return []xdr.LedgerEntry{}, err
		}

		return readBucketList(archive, checkpointSeq, xdr.LedgerEntryTypeAccount)
	}

	// Use captive core to read a range otherwise
	return readFromCaptive(start, end, limit, execPath, configPath, xdr.LedgerEntryTypeAccount)
}

func readFromCaptive(start, end uint32, limit int64, execPath, configPath string, entryType xdr.LedgerEntryType) ([]xdr.LedgerEntry, error) {
	captiveBackend, err := ledgerbackend.NewCaptive(
		execPath,
		configPath,
		network.PublicNetworkPassphrase,
		[]string{archiveStellarURL},
	)
	if err != nil {
		return []xdr.LedgerEntry{}, err
	}

	bRange := ledgerbackend.BoundedRange(start, end)

	err = captiveBackend.PrepareRange(bRange)
	if err != nil {
		return []xdr.LedgerEntry{}, err
	}

	changeCache := ingestio.NewLedgerEntryChangeCache()
	for seq := start; seq <= end; seq++ {
		changeReader, err := ingestio.NewLedgerChangeReader(captiveBackend, network.PublicNetworkPassphrase, seq)
		if err != nil {
			return []xdr.LedgerEntry{}, err
		}

		for {
			change, err := changeReader.Read()
			if err == ingestio.EOF {
				break
			}

			if err != nil {
				return []xdr.LedgerEntry{}, err
			}

			if change.Type == entryType {
				changeCache.AddChange(change)
			}
		}
	}

	compactedChanges := changeCache.GetChanges()

	// Need to keep the limit flag as a cap if it is not negative
	entryLimit := int64(len(compactedChanges))
	if limit >= 0 {
		entryLimit = limit
	}

	accountEntries := make([]xdr.LedgerEntry, entryLimit)
	// For point in time state, we always want to get the data after the change;
	for i := 0; int64(i) < entryLimit; i++ {
		accountEntries[i] = *(compactedChanges[i].Post)
	}

	return accountEntries, nil
}

func readBucketList(archive *historyarchive.Archive, checkpointSeq uint32, entryType xdr.LedgerEntryType) ([]xdr.LedgerEntry, error) {
	changeReader, err := ingestio.MakeSingleLedgerStateReader(context.Background(), archive, checkpointSeq)
	defer changeReader.Close()
	if err != nil {
		return []xdr.LedgerEntry{}, err
	}

	entrySlice := []xdr.LedgerEntry{}
	for {
		change, err := changeReader.Read()
		if err == ingestio.EOF {
			break
		}

		if err != nil {
			return []xdr.LedgerEntry{}, err
		}

		if change.Type == entryType {
			entrySlice = append(entrySlice, *change.Post)
		}
	}

	return entrySlice, nil
}
