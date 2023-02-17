package input

import (
	"context"

	"github.com/stellar/stellar-etl/internal/utils"

	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"
)

// GetLedgers returns a slice of serialized ledger close metas for the ledgers in the provided range (inclusive on both ends)
func GetLedgers(start, end uint32, limit int64, isTest bool) ([]xdr.SerializedLedgerCloseMeta, error) {
	// env := utils.GetEnvironmentDetails(isTest)
	// backend, err := utils.CreateBackend(start, end, env.ArchiveURLs)
	// if err != nil {
	// 	return []xdr.LedgerCloseMeta{}, err
	// }

	bucket := "gcs://horizon-archive-poc-slcm"
	backend, err := utils.CreateGCSBackend(bucket)
	if err != nil {
		log.Error("Error creating GCS backend:", err)
		return []xdr.SerializedLedgerCloseMeta{}, err
	}

	// TODO
	// if *continueFromLatestLedger {
	// 	if start != 0 {
	// 		log.Fatalf("-start-ledger and -continue cannot both be set")
	// 	}
	// 	start = readLatestLedger(target)
	// 	log.Infof("continue flag was enabled, next ledger found was %v", start)
	// }

	var ledgerRange ledgerbackend.Range
	if end == 0 {
		ledgerRange = ledgerbackend.UnboundedRange(start)
	} else {
		ledgerRange = ledgerbackend.BoundedRange(start, end)
	}

	log.Infof("preparing to export %s", ledgerRange)
	latest, _ := backend.GetLatestLedgerSequence(context.Background())
	err = utils.ValidateLedgerRange(start, end, latest)
	if err != nil {
		log.Error(err)
		return []xdr.SerializedLedgerCloseMeta{}, err
	}

	metaSlice := []xdr.SerializedLedgerCloseMeta{}
	ctx := context.Background()
	for seq := start; seq <= end; seq++ {
		ledger, err := backend.GetLedger(ctx, seq)
		if err != nil {
			return []xdr.SerializedLedgerCloseMeta{}, err
		}

		metaSlice = append(metaSlice, ledger)
		if int64(len(metaSlice)) >= limit && limit >= 0 {
			break
		}
	}

	return metaSlice, nil
}
