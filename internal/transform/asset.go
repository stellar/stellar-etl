package transform

import (
	"fmt"
	"hash/fnv"

	"github.com/dgryski/go-farm"
	"github.com/stellar/stellar-etl/internal/toid"
	"github.com/stellar/stellar-etl/internal/utils"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

// TransformAsset converts an asset from a payment operation into a form suitable for BigQuery
func TransformAsset(operation xdr.Operation, operationIndex int32, transaction ingest.LedgerTransaction, ledgerSeq int32, ledgerCloseMeta xdr.LedgerCloseMeta) (AssetOutput, error) {
	operationID := toid.New(ledgerSeq, int32(transaction.Index), operationIndex).ToInt64()

	opType := operation.Body.Type
	if opType != xdr.OperationTypePayment {
		return AssetOutput{}, fmt.Errorf("Operation of type %d cannot issue an asset (id %d)", opType, operationID)
	}

	op, ok := operation.Body.GetPaymentOp()
	if !ok {
		return AssetOutput{}, fmt.Errorf("Could not access Payment info for this operation (id %d)", operationID)
	}

	outputAsset, err := transformSingleAsset(op.Asset, ledgerCloseMeta)
	if err != nil {
		return AssetOutput{}, fmt.Errorf("%s (id %d)", err.Error(), operationID)
	}

	return outputAsset, nil
}

func transformSingleAsset(asset xdr.Asset, ledgerCloseMeta xdr.LedgerCloseMeta) (AssetOutput, error) {
	var outputAssetType, outputAssetCode, outputAssetIssuer string
	err := asset.Extract(&outputAssetType, &outputAssetCode, &outputAssetIssuer)
	if err != nil {
		return AssetOutput{}, fmt.Errorf("Could not extract asset from this operation")
	}

	outputAssetID, err := hashAsset(outputAssetCode, outputAssetIssuer)
	if err != nil {
		return AssetOutput{}, fmt.Errorf("Unable to hash asset for payment operation")
	}

	outputCloseTime, err := utils.TimePointToUTCTimeStamp(ledgerCloseMeta.MustV0().LedgerHeader.Header.ScpValue.CloseTime)
	if err != nil {
		return AssetOutput{}, err
	}

	farmAssetID := FarmHashAsset(outputAssetCode, outputAssetIssuer, outputAssetType)

	return AssetOutput{
		AssetCode:      outputAssetCode,
		AssetIssuer:    outputAssetIssuer,
		AssetType:      outputAssetType,
		AssetID:        outputAssetID,
		ID:             farmAssetID,
		LedgerClosedAt: outputCloseTime,
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

func FarmHashAsset(assetCode, assetIssuer, assetType string) int64 {
	asset := fmt.Sprintf("%s%s%s", assetCode, assetIssuer, assetType)
	hash := farm.Fingerprint64([]byte(asset))

	return int64(hash)
}
