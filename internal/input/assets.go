package input

import (
	"context"

	"github.com/stellar/stellar-etl/internal/transform"
	"github.com/stellar/stellar-etl/internal/utils"

	"github.com/stellar/go/xdr"
)

type AssetTransformInput struct {
	Operation        xdr.Operation
	OperationIndex   int32
	TransactionIndex int32
	LedgerSeqNum     int32
}

// GetPaymentOperations returns a slice of payment operations that can include new assets from the ledgers in the provided range (inclusive on both ends)
func GetPaymentOperations(start, end uint32, limit int64, isTest bool, isFuture bool) ([]AssetTransformInput, error) {
	env := utils.GetEnvironmentDetails(isTest, isFuture)
	backend, err := utils.CreateBackend(start, end, env.ArchiveURLs)
	if err != nil {
		return []AssetTransformInput{}, err
	}

	assetSlice := []AssetTransformInput{}
	ctx := context.Background()
	for seq := start; seq <= end; seq++ {
		// Get ledger from sequence number
		ledger, err := backend.GetLedgerArchive(ctx, seq)
		if err != nil {
			return []AssetTransformInput{}, err
		}

		transactionSet := transform.GetTransactionSet(ledger)

		for txIndex, transaction := range transactionSet {
			for opIndex, op := range transaction.Operations() {
				if op.Body.Type == xdr.OperationTypePayment || op.Body.Type == xdr.OperationTypeManageSellOffer {
					assetSlice = append(assetSlice, AssetTransformInput{
						Operation:        op,
						OperationIndex:   int32(opIndex),
						TransactionIndex: int32(txIndex),
						LedgerSeqNum:     int32(seq),
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
