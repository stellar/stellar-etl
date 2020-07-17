package transform

import (
	"encoding/base64"
	"fmt"
	"strconv"

	"github.com/stellar/go/amount"
	ingestio "github.com/stellar/go/exp/ingest/io"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/internal/utils"
)

//TransformOperation converts an operation from the history archive ingestion system into a form suitable for BigQuery
func TransformOperation(operation xdr.Operation, operationIndex int32, transaction ingestio.LedgerTransaction) (OperationOutput, error) {
	outputSourceAccount, err := utils.GetAccountAddressFromMuxedAccount(getOperationSourceAccount(operation, transaction))
	if err != nil {
		return OperationOutput{}, err
	}

	outputOperationType := int32(operation.Body.Type)
	if outputOperationType < 0 {
		return OperationOutput{}, fmt.Errorf("The operation type (%d) is negative for  operation %d", outputOperationType, operationIndex)
	}

	outputDetails, err := extractOperationDetails(operation, transaction, operationIndex)
	if err != nil {
		return OperationOutput{}, err
	}

	transformedOperation := OperationOutput{
		SourceAccount:    outputSourceAccount,
		Type:             outputOperationType,
		ApplicationOrder: operationIndex + 1,
		OperationDetails: outputDetails,
	}

	return transformedOperation, nil
}

func getOperationSourceAccount(operation xdr.Operation, transaction ingestio.LedgerTransaction) xdr.MuxedAccount {
	sourceAccount := operation.SourceAccount
	if sourceAccount != nil {
		return *sourceAccount
	}

	return transaction.Envelope.SourceAccount()
}

func addAssetDetailsToOperationDetails(operationDetails *Details, asset xdr.Asset, prefix string) error {
	var assetType, issuer, code string
	err := asset.Extract(&assetType, &issuer, &code)
	if err != nil {
		return err
	}

	switch prefix {
	case "buying":
		operationDetails.BuyingAssetType = assetType
		if asset.Type != xdr.AssetTypeAssetTypeNative {
			operationDetails.BuyingAssetIssuer = issuer
			operationDetails.BuyingAssetCode = code
		}

	case "selling":
		operationDetails.SellingAssetType = assetType
		if asset.Type != xdr.AssetTypeAssetTypeNative {
			operationDetails.SellingAssetIssuer = issuer
			operationDetails.SellingAssetCode = code
		}

	case "source":
		operationDetails.SourceAssetType = assetType
		if asset.Type != xdr.AssetTypeAssetTypeNative {
			operationDetails.SourceAssetIssuer = issuer
			operationDetails.SourceAssetCode = code
		}

	default:
		operationDetails.AssetType = assetType
		if asset.Type != xdr.AssetTypeAssetTypeNative {
			operationDetails.AssetIssuer = issuer
			operationDetails.AssetCode = code
		}

	}
	return nil
}
func addOperationFlagToOperationDetails(operationDetails Details, flag int32, prefix string) {
	var intFlags []int32
	var stringFlags []string

	if (flag & int32(xdr.AccountFlagsAuthRequiredFlag)) > 0 {
		intFlags = append(intFlags, int32(xdr.AccountFlagsAuthRequiredFlag))
		stringFlags = append(stringFlags, "auth_required")
	}

	if (flag & int32(xdr.AccountFlagsAuthRevocableFlag)) > 0 {
		intFlags = append(intFlags, int32(xdr.AccountFlagsAuthRevocableFlag))
		stringFlags = append(stringFlags, "auth_revocable")
	}

	if (flag & int32(xdr.AccountFlagsAuthImmutableFlag)) > 0 {
		intFlags = append(intFlags, int32(xdr.AccountFlagsAuthImmutableFlag))
		stringFlags = append(stringFlags, "auth_immutable")
	}

	switch prefix {
	case "set":
		operationDetails.SetFlags = intFlags
		operationDetails.SetFlagsString = stringFlags

	case "clear":
		operationDetails.ClearFlags = intFlags
		operationDetails.ClearFlagsString = stringFlags
	}
}

func extractOperationDetails(operation xdr.Operation, transaction ingestio.LedgerTransaction, operationIndex int32) (Details, error) {
	outputDetails := Details{}
	sourceAccount := getOperationSourceAccount(operation, transaction)
	sourceAccountAddress, _ := utils.GetAccountAddressFromMuxedAccount(sourceAccount)
	operationType := operation.Body.Type
	allOperationResults, ok := transaction.Result.OperationResults()
	if !ok {
		return Details{}, fmt.Errorf("Could not access any results for this transaction")
	}

	currentOperationResult := allOperationResults[operationIndex]
	switch operationType {
	case xdr.OperationTypeCreateAccount:
		op := operation.Body.MustCreateAccountOp()
		outputDetails.Funder = sourceAccountAddress
		outputDetails.Account = op.Destination.Address()
		outputDetails.StartingBalance = float64(op.StartingBalance)

	case xdr.OperationTypePayment:
		op := operation.Body.MustPaymentOp()
		outputDetails.From = sourceAccountAddress
		toAccountAddress, err := utils.GetAccountAddressFromMuxedAccount(op.Destination)
		if err != nil {
			return Details{}, err
		}

		outputDetails.To = toAccountAddress
		outputDetails.Amount = float64(op.Amount)
		err = addAssetDetailsToOperationDetails(&outputDetails, op.Asset, "")
		if err != nil {
			return Details{}, err
		}

	case xdr.OperationTypePathPaymentStrictReceive:
		op := operation.Body.MustPathPaymentStrictReceiveOp()
		outputDetails.From = sourceAccountAddress
		toAccountAddress, err := utils.GetAccountAddressFromMuxedAccount(op.Destination)
		if err != nil {
			return Details{}, err
		}
		outputDetails.To = toAccountAddress

		outputDetails.Amount = float64(op.DestAmount)
		outputDetails.SourceAmount = float64(0)
		outputDetails.SourceMax = float64(op.SendMax)
		addAssetDetailsToOperationDetails(&outputDetails, op.DestAsset, "")
		addAssetDetailsToOperationDetails(&outputDetails, op.SendAsset, "source")

		if transaction.Result.Successful() {
			result := currentOperationResult.MustTr().MustPathPaymentStrictReceiveResult()
			outputDetails.SourceAmount = float64(result.SendAmount())
		}

		var path = []AssetOutput{}
		for _, pathAsset := range op.Path {
			var assetType, issuer, code string
			err := pathAsset.Extract(&assetType, &issuer, &code)
			if err != nil {
				return Details{}, err
			}
			path = append(path, AssetOutput{
				AssetType:   assetType,
				AssetIssuer: issuer,
				AssetCode:   code,
			})
		}
		outputDetails.Path = path

	case xdr.OperationTypePathPaymentStrictSend:
		op := operation.Body.MustPathPaymentStrictSendOp()
		outputDetails.From = sourceAccountAddress
		toAccountAddress, err := utils.GetAccountAddressFromMuxedAccount(op.Destination)
		if err != nil {
			return Details{}, err
		}
		outputDetails.To = toAccountAddress

		outputDetails.Amount = float64(0)
		outputDetails.SourceAmount = float64(op.SendAmount)
		outputDetails.DestinationMin = amount.String(op.DestMin)
		addAssetDetailsToOperationDetails(&outputDetails, op.DestAsset, "")
		addAssetDetailsToOperationDetails(&outputDetails, op.SendAsset, "source")

		if transaction.Result.Successful() {
			result := currentOperationResult.MustTr().MustPathPaymentStrictSendResult()
			outputDetails.Amount = float64(result.DestAmount())
		}

		var path = []AssetOutput{}
		for _, pathAsset := range op.Path {
			var assetType, issuer, code string
			err := pathAsset.Extract(&assetType, &issuer, &code)
			if err != nil {
				return Details{}, err
			}

			path = append(path, AssetOutput{
				AssetType:   assetType,
				AssetIssuer: issuer,
				AssetCode:   code,
			})
		}
		outputDetails.Path = path

	case xdr.OperationTypeManageBuyOffer:
		op := operation.Body.MustManageBuyOfferOp()
		outputDetails.OfferID = int64(op.OfferId)
		outputDetails.Amount = float64(op.BuyAmount)
		parsedPrice, err := strconv.ParseFloat(op.Price.String(), 64)
		if err != nil {
			return Details{}, err
		}

		outputDetails.Price = parsedPrice
		outputDetails.PriceR = Price{
			Numerator:   int32(op.Price.N),
			Denominator: int32(op.Price.D),
		}
		addAssetDetailsToOperationDetails(&outputDetails, op.Buying, "buying")
		addAssetDetailsToOperationDetails(&outputDetails, op.Selling, "selling")

	case xdr.OperationTypeManageSellOffer:
		op := operation.Body.MustManageSellOfferOp()
		outputDetails.OfferID = int64(op.OfferId)
		outputDetails.Amount = float64(op.Amount)
		parsedPrice, err := strconv.ParseFloat(op.Price.String(), 64)
		if err != nil {
			return Details{}, err
		}

		outputDetails.Price = parsedPrice
		outputDetails.PriceR = Price{
			Numerator:   int32(op.Price.N),
			Denominator: int32(op.Price.D),
		}
		addAssetDetailsToOperationDetails(&outputDetails, op.Buying, "buying")
		addAssetDetailsToOperationDetails(&outputDetails, op.Selling, "selling")
		
	case xdr.OperationTypeCreatePassiveSellOffer:
		op := operation.Body.MustCreatePassiveSellOfferOp()
		outputDetails.Amount = float64(op.Amount)
		parsedPrice, err := strconv.ParseFloat(op.Price.String(), 64)
		if err != nil {
			return Details{}, err
		}

		outputDetails.Price = parsedPrice
		outputDetails.PriceR = Price{
			Numerator:   int32(op.Price.N),
			Denominator: int32(op.Price.D),
		}
		addAssetDetailsToOperationDetails(&outputDetails, op.Buying, "buying")
		addAssetDetailsToOperationDetails(&outputDetails, op.Selling, "selling")

	case xdr.OperationTypeSetOptions:
		op := operation.Body.MustSetOptionsOp()

		if op.InflationDest != nil {
			outputDetails.InflationDest = op.InflationDest.Address()
		}

		if op.SetFlags != nil && *op.SetFlags > 0 {
			addOperationFlagToOperationDetails(outputDetails, int32(*op.SetFlags), "set")
		}

		if op.ClearFlags != nil && *op.ClearFlags > 0 {
			addOperationFlagToOperationDetails(outputDetails, int32(*op.ClearFlags), "clear")
		}

		if op.MasterWeight != nil {
			outputDetails.MasterKeyWeight = int32(*op.MasterWeight)
		}

		if op.LowThreshold != nil {
			outputDetails.LowThreshold = int32(*op.LowThreshold)
		}

		if op.MedThreshold != nil {
			outputDetails.MedThreshold = int32(*op.MedThreshold)
		}

		if op.HighThreshold != nil {
			outputDetails.HighThreshold = int32(*op.HighThreshold)
		}

		if op.HomeDomain != nil {
			outputDetails.HomeDomain = string(*op.HomeDomain)
		}

		if op.Signer != nil {
			outputDetails.SignerKey = op.Signer.Key.Address()
			outputDetails.SignerWeight = int32(op.Signer.Weight)
		}

	case xdr.OperationTypeChangeTrust:
		op := operation.Body.MustChangeTrustOp()
		addAssetDetailsToOperationDetails(&outputDetails, op.Line, "")
		outputDetails.Trustor = sourceAccountAddress
		outputDetails.Trustee = outputDetails.AssetIssuer
		outputDetails.Limit = float64(op.Limit)

	case xdr.OperationTypeAllowTrust:
		op := operation.Body.MustAllowTrustOp()
		addAssetDetailsToOperationDetails(&outputDetails, op.Asset.ToAsset(sourceAccount.ToAccountId()), "")
		outputDetails.Trustee = sourceAccountAddress
		outputDetails.Trustor = op.Trustor.Address()
		outputDetails.Authorize = xdr.TrustLineFlags(op.Authorize).IsAuthorized()

	case xdr.OperationTypeAccountMerge:
		aid := operation.Body.MustDestination().ToAccountId()
		outputDetails.Account = sourceAccountAddress
		outputDetails.Into = aid.Address()

	case xdr.OperationTypeInflation:
		// inflation is deprecated
	case xdr.OperationTypeManageData:
		op := operation.Body.MustManageDataOp()
		outputDetails.Name = string(op.DataName)
		if op.DataValue != nil {
			outputDetails.Value = base64.StdEncoding.EncodeToString(*op.DataValue)
		} else {
			outputDetails.Value = ""
		}

	case xdr.OperationTypeBumpSequence:
		op := operation.Body.MustBumpSequenceOp()
		outputDetails.BumpTo = fmt.Sprintf("%d", op.BumpTo)

	default:
		return Details{}, fmt.Errorf("Unknown operation type: %s", operation.Body.Type.String())
	}
	return outputDetails, nil
}
