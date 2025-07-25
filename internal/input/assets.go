package input

import (
	"context"

	"github.com/stellar/stellar-etl/v2/internal/utils"

	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/xdr"
)

type AssetTransformInput struct {
	Operation        xdr.Operation
	OperationIndex   int32
	TransactionIndex int32
	LedgerSeqNum     int32
	LedgerCloseMeta  xdr.LedgerCloseMeta
}

// GetPaymentOperations returns a slice of payment operations that can include new assets from the ledgers in the provided range (inclusive on both ends)
func GetPaymentOperations(start, end uint32, limit int64, env utils.EnvironmentDetails, useCaptiveCore bool) ([]AssetTransformInput, error) {
	ctx := context.Background()
	backend, err := utils.CreateLedgerBackend(ctx, useCaptiveCore, env)
	if err != nil {
		return []AssetTransformInput{}, err
	}

	assetSlice := []AssetTransformInput{}
	err = backend.PrepareRange(ctx, ledgerbackend.BoundedRange(start, end))
	panicIf(err)
	for seq := start; seq <= end; seq++ {
		// Get ledger from sequence number
		ledger, err := backend.GetLedger(ctx, seq)
		if err != nil {
			return []AssetTransformInput{}, err
		}

		transactionSet := ledger.TransactionEnvelopes()

		for txIndex, transaction := range transactionSet {
			for opIndex, op := range transaction.Operations() {
				if op.Body.Type == xdr.OperationTypePayment || op.Body.Type == xdr.OperationTypeManageSellOffer {
					assetSlice = append(assetSlice, AssetTransformInput{
						Operation:        op,
						OperationIndex:   int32(opIndex),
						TransactionIndex: int32(txIndex),
						LedgerSeqNum:     int32(seq),
						LedgerCloseMeta:  ledger,
					})
				}

			}

		}
		if int64(len(assetSlice)) >= limit && limit >= 0 {
			break
		}
	}

	return assetSlice, nil
}
