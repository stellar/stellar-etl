package transform

import (
	"fmt"
	"hash/fnv"

	"github.com/dgryski/go-farm"
	"github.com/stellar/stellar-etl/internal/toid"

	"github.com/stellar/go/xdr"
)

// TransformAsset converts an asset from a payment operation into a form suitable for BigQuery
func TransformAsset(operation xdr.Operation, operationIndex int32, transactionIndex int32, ledgerSeq int32) (AssetOutput, error) {
	operationID := toid.New(ledgerSeq, int32(transactionIndex), operationIndex).ToInt64()

	opType := operation.Body.Type
	if opType != xdr.OperationTypePayment && opType != xdr.OperationTypeManageSellOffer {
		return AssetOutput{}, fmt.Errorf("operation of type %d cannot issue an asset (id %d)", opType, operationID)
	}

	op := xdr.Asset{}
	switch opType {
	case xdr.OperationTypeManageSellOffer:
		opSellOf, ok := operation.Body.GetManageSellOfferOp()
		if ok {
			return AssetOutput{}, fmt.Errorf("operation of type ManageSellOfferOp cannot issue an asset (id %d)", operationID)
		}
		op = opSellOf.Selling

	case xdr.OperationTypePayment:
		opPayment, ok := operation.Body.GetPaymentOp()
		if !ok {
			return AssetOutput{}, fmt.Errorf("could not access Payment info for this operation (id %d)", operationID)
		}
		op = opPayment.Asset

	}

	outputAsset, err := transformSingleAsset(op)
	if err != nil {
		return AssetOutput{}, fmt.Errorf("%s (id %d)", err.Error(), operationID)
	}

	return outputAsset, nil
}

func transformSingleAsset(asset xdr.Asset) (AssetOutput, error) {
	var outputAssetType, outputAssetCode, outputAssetIssuer string
	err := asset.Extract(&outputAssetType, &outputAssetCode, &outputAssetIssuer)
	if err != nil {
		return AssetOutput{}, fmt.Errorf("could not extract asset from this operation")
	}

	outputAssetID, err := hashAsset(outputAssetCode, outputAssetIssuer)
	if err != nil {
		return AssetOutput{}, fmt.Errorf("unable to hash asset for payment operation")
	}

	farmAssetID := FarmHashAsset(outputAssetCode, outputAssetIssuer, outputAssetType)

	return AssetOutput{
		AssetCode:   outputAssetCode,
		AssetIssuer: outputAssetIssuer,
		AssetType:   outputAssetType,
		AssetID:     outputAssetID,
		ID:          farmAssetID,
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
