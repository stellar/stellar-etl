package transform

import (
	"fmt"

	"github.com/dgryski/go-farm"
	"github.com/stellar/stellar-etl/v2/internal/toid"
	"github.com/stellar/stellar-etl/v2/internal/utils"

	"github.com/stellar/go/xdr"
)

// TransformAsset converts an asset from a payment operation into a form suitable for BigQuery
func TransformAsset(operation xdr.Operation, operationIndex int32, transactionIndex int32, ledgerSeq int32, lcm xdr.LedgerCloseMeta) (AssetOutput, error) {
	operationID := toid.New(ledgerSeq, int32(transactionIndex), operationIndex).ToInt64()

	opType := operation.Body.Type
	if opType != xdr.OperationTypePayment && opType != xdr.OperationTypeManageSellOffer {
		return AssetOutput{}, fmt.Errorf("operation of type %d cannot issue an asset (id %d)", opType, operationID)
	}

	asset := xdr.Asset{}
	switch opType {
	case xdr.OperationTypeManageSellOffer:
		opSellOf, ok := operation.Body.GetManageSellOfferOp()
		if !ok {
			return AssetOutput{}, fmt.Errorf("operation of type ManageSellOfferOp cannot issue an asset (id %d)", operationID)
		}
		asset = opSellOf.Selling

	case xdr.OperationTypePayment:
		opPayment, ok := operation.Body.GetPaymentOp()
		if !ok {
			return AssetOutput{}, fmt.Errorf("could not access Payment info for this operation (id %d)", operationID)
		}
		asset = opPayment.Asset

	}

	outputAsset, err := transformSingleAsset(asset)
	if err != nil {
		return AssetOutput{}, fmt.Errorf("%s (id %d)", err.Error(), operationID)
	}

	outputCloseTime, err := utils.GetCloseTime(lcm)
	if err != nil {
		return AssetOutput{}, err
	}
	outputAsset.ClosedAt = outputCloseTime
	outputAsset.LedgerSequence = utils.GetLedgerSequence(lcm)

	return outputAsset, nil
}

func transformSingleAsset(asset xdr.Asset) (AssetOutput, error) {
	var outputAssetType, outputAssetCode, outputAssetIssuer string
	err := asset.Extract(&outputAssetType, &outputAssetCode, &outputAssetIssuer)
	if err != nil {
		return AssetOutput{}, fmt.Errorf("could not extract asset from this operation")
	}

	farmAssetID := FarmHashAsset(outputAssetCode, outputAssetIssuer, outputAssetType)

	return AssetOutput{
		AssetCode:   outputAssetCode,
		AssetIssuer: outputAssetIssuer,
		AssetType:   outputAssetType,
		AssetID:     farmAssetID,
	}, nil
}

func FarmHashAsset(assetCode, assetIssuer, assetType string) int64 {
	asset := fmt.Sprintf("%s%s%s", assetCode, assetIssuer, assetType)
	hash := farm.Fingerprint64([]byte(asset))

	return int64(hash)
}
