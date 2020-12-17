package transform

import (
	"fmt"
	"hash/fnv"

	ingestio "github.com/stellar/go/ingest/io"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/internal/toid"
)

//TransformAsset converts an asset from the history archive ingestion system into a form suitable for BigQuery
func TransformAsset(operation xdr.Operation, operationIndex int32, transaction ingestio.LedgerTransaction, ledgerSeq int32) (AssetOutput, error) {
	operationID := toid.New(ledgerSeq, int32(transaction.Index), operationIndex).ToInt64()

	opType := operation.Body.Type
	if opType != xdr.OperationTypePayment {
		return AssetOutput{}, fmt.Errorf("Operation of type %d cannot issue an asset (id %d)", opType, operationID)
	}

	op, ok := operation.Body.GetPaymentOp()
	if !ok {
		return AssetOutput{}, fmt.Errorf("Could not access Payment info for this operation (id %d)", operationID)
	}

	var outputAssetType, outputAssetCode, outputAssetIssuer string
	err := op.Asset.Extract(&outputAssetType, &outputAssetCode, &outputAssetIssuer)
	if err != nil {
		return AssetOutput{}, fmt.Errorf("Could not extract asset from this operation (id %d)", operationID)
	}

	outputAssetID, err := hashAsset(outputAssetCode, outputAssetIssuer)
	if err != nil {
		return AssetOutput{}, fmt.Errorf("Unable to hash asset for payment operation (id %d)", operationID)
	}

	return AssetOutput{
		AssetCode:   outputAssetCode,
		AssetIssuer: outputAssetIssuer,
		AssetType:   outputAssetType,
		AssetID:     outputAssetID,
	}, nil
}

func hashAsset(assetCode, assetIssuer string) (uint64, error) {
	asset := fmt.Sprintf("%s:%s", assetCode, assetIssuer)
	fnvHasher := fnv.New64a()
	if _, err := fnvHasher.Write([]byte(asset)); err != nil {
		return 0, err
	}

	hash := fnvHasher.Sum64()
	return hash, nil
}
