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
	var assetType, code, issuer string
	err := asset.Extract(&assetType, &code, &issuer)
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
func convertPathToAssetOutput(initialPath []xdr.Asset) []AssetOutput {
	if len(initialPath) == 0 {
		return nil
	}
	var path = []AssetOutput{}
	for _, pathAsset := range initialPath {
		var assetType, code, issuer string
		err := pathAsset.Extract(&assetType, &code, &issuer)
		if err != nil {
			return nil
		}

		path = append(path, AssetOutput{
			AssetType:   assetType,
			AssetIssuer: issuer,
			AssetCode:   code,
		})
	}
	return path
}
func addOperationFlagToOperationDetails(operationDetails *Details, flag uint32, prefix string) {
	var intFlags []int32
	var stringFlags []string

	if (int64(flag) & int64(xdr.AccountFlagsAuthRequiredFlag)) > 0 {
		intFlags = append(intFlags, int32(xdr.AccountFlagsAuthRequiredFlag))
		stringFlags = append(stringFlags, "auth_required")
	}

	if (int64(flag) & int64(xdr.AccountFlagsAuthRevocableFlag)) > 0 {
		intFlags = append(intFlags, int32(xdr.AccountFlagsAuthRevocableFlag))
		stringFlags = append(stringFlags, "auth_revocable")
	}

	if (int64(flag) & int64(xdr.AccountFlagsAuthImmutableFlag)) > 0 {
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
	sourceAccountAddress, err := utils.GetAccountAddressFromMuxedAccount(sourceAccount)
	if err != nil {
		return Details{}, err
	}

	operationType := operation.Body.Type
	allOperationResults, ok := transaction.Result.OperationResults()
	if !ok {
		return Details{}, fmt.Errorf("Could not access any results for this transaction")
	}

	currentOperationResult := allOperationResults[operationIndex]
	switch operationType {
	case xdr.OperationTypeCreateAccount:
		op, ok := operation.Body.GetCreateAccountOp()
		if !ok {
			return Details{}, fmt.Errorf("Could not access CreateAccount info for this operation (index %d)", operationIndex)
		}

		outputDetails.Funder = sourceAccountAddress
		outputDetails.Account = op.Destination.Address()
		outputDetails.StartingBalance = utils.ConvertStroopValueToReal(op.StartingBalance)

	case xdr.OperationTypePayment:
		op, ok := operation.Body.GetPaymentOp()
		if !ok {
			return Details{}, fmt.Errorf("Could not access Payment info for this operation (index %d)", operationIndex)
		}

		outputDetails.From = sourceAccountAddress
		toAccountAddress, err := utils.GetAccountAddressFromMuxedAccount(op.Destination)
		if err != nil {
			return Details{}, err
		}

		outputDetails.To = toAccountAddress
		outputDetails.Amount = utils.ConvertStroopValueToReal(op.Amount)
		err = addAssetDetailsToOperationDetails(&outputDetails, op.Asset, "")
		if err != nil {
			return Details{}, err
		}

	case xdr.OperationTypePathPaymentStrictReceive:
		op, ok := operation.Body.GetPathPaymentStrictReceiveOp()
		if !ok {
			return Details{}, fmt.Errorf("Could not access PathPaymentStrictReceive info for this operation (index %d)", operationIndex)
		}

		outputDetails.From = sourceAccountAddress
		toAccountAddress, err := utils.GetAccountAddressFromMuxedAccount(op.Destination)
		if err != nil {
			return Details{}, err
		}

		outputDetails.To = toAccountAddress
		outputDetails.Amount = utils.ConvertStroopValueToReal(op.DestAmount)
		outputDetails.SourceMax = utils.ConvertStroopValueToReal(op.SendMax)
		addAssetDetailsToOperationDetails(&outputDetails, op.DestAsset, "")
		addAssetDetailsToOperationDetails(&outputDetails, op.SendAsset, "source")

		if transaction.Result.Successful() {
			resultBody, ok := currentOperationResult.GetTr()
			if !ok {
				return Details{}, fmt.Errorf("Could not access result body for this operation (index %d)", operationIndex)
			}
			result, ok := resultBody.GetPathPaymentStrictReceiveResult()
			if !ok {
				return Details{}, fmt.Errorf("Could not access PathPaymentStrictReceive result info for this operation (index %d)", operationIndex)
			}
			outputDetails.SourceAmount = utils.ConvertStroopValueToReal(result.SendAmount())
		}

		outputDetails.Path = convertPathToAssetOutput(op.Path)

	case xdr.OperationTypePathPaymentStrictSend:
		op, ok := operation.Body.GetPathPaymentStrictSendOp()
		if !ok {
			return Details{}, fmt.Errorf("Could not access PathPaymentStrictSend info for this operation (index %d)", operationIndex)
		}

		outputDetails.From = sourceAccountAddress
		toAccountAddress, err := utils.GetAccountAddressFromMuxedAccount(op.Destination)
		if err != nil {
			return Details{}, err
		}

		outputDetails.To = toAccountAddress
		outputDetails.SourceAmount = utils.ConvertStroopValueToReal(op.SendAmount)
		outputDetails.DestinationMin = amount.String(op.DestMin)
		addAssetDetailsToOperationDetails(&outputDetails, op.DestAsset, "")
		addAssetDetailsToOperationDetails(&outputDetails, op.SendAsset, "source")

		if transaction.Result.Successful() {
			resultBody, ok := currentOperationResult.GetTr()
			if !ok {
				return Details{}, fmt.Errorf("Could not access result body for this operation (index %d)", operationIndex)
			}
			result, ok := resultBody.GetPathPaymentStrictSendResult()
			if !ok {
				return Details{}, fmt.Errorf("Could not access GetPathPaymentStrictSendResult result info for this operation (index %d)", operationIndex)
			}
			outputDetails.Amount = utils.ConvertStroopValueToReal(result.DestAmount())
		}

		outputDetails.Path = convertPathToAssetOutput(op.Path)

	case xdr.OperationTypeManageBuyOffer:
		op, ok := operation.Body.GetManageBuyOfferOp()
		if !ok {
			return Details{}, fmt.Errorf("Could not access ManageBuyOffer info for this operation (index %d)", operationIndex)
		}

		outputDetails.OfferID = int64(op.OfferId)
		outputDetails.Amount = utils.ConvertStroopValueToReal(op.BuyAmount)
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
		op, ok := operation.Body.GetManageSellOfferOp()
		if !ok {
			return Details{}, fmt.Errorf("Could not access ManageSellOffer info for this operation (index %d)", operationIndex)
		}

		outputDetails.OfferID = int64(op.OfferId)
		outputDetails.Amount = utils.ConvertStroopValueToReal(op.Amount)
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
		op, ok := operation.Body.GetCreatePassiveSellOfferOp()
		if !ok {
			return Details{}, fmt.Errorf("Could not access CreatePassiveSellOffer info for this operation (index %d)", operationIndex)
		}

		outputDetails.Amount = utils.ConvertStroopValueToReal(op.Amount)
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
		op, ok := operation.Body.GetSetOptionsOp()
		if !ok {
			return Details{}, fmt.Errorf("Could not access GetSetOptions info for this operation (index %d)", operationIndex)
		}

		if op.InflationDest != nil {
			outputDetails.InflationDest = op.InflationDest.Address()
		}

		if op.SetFlags != nil && *op.SetFlags > 0 {
			addOperationFlagToOperationDetails(&outputDetails, uint32(*op.SetFlags), "set")
		}

		if op.ClearFlags != nil && *op.ClearFlags > 0 {
			addOperationFlagToOperationDetails(&outputDetails, uint32(*op.ClearFlags), "clear")
		}

		if op.MasterWeight != nil {
			outputDetails.MasterKeyWeight = uint32(*op.MasterWeight)
		}

		if op.LowThreshold != nil {
			outputDetails.LowThreshold = uint32(*op.LowThreshold)
		}

		if op.MedThreshold != nil {
			outputDetails.MedThreshold = uint32(*op.MedThreshold)
		}

		if op.HighThreshold != nil {
			outputDetails.HighThreshold = uint32(*op.HighThreshold)
		}

		if op.HomeDomain != nil {
			outputDetails.HomeDomain = string(*op.HomeDomain)
		}

		if op.Signer != nil {
			outputDetails.SignerKey = op.Signer.Key.Address()
			outputDetails.SignerWeight = uint32(op.Signer.Weight)
		}

	case xdr.OperationTypeChangeTrust:
		op, ok := operation.Body.GetChangeTrustOp()
		if !ok {
			return Details{}, fmt.Errorf("Could not access GetChangeTrust info for this operation (index %d)", operationIndex)
		}

		addAssetDetailsToOperationDetails(&outputDetails, op.Line, "")
		outputDetails.Trustor = sourceAccountAddress
		outputDetails.Trustee = outputDetails.AssetIssuer
		outputDetails.Limit = utils.ConvertStroopValueToReal(op.Limit)

	case xdr.OperationTypeAllowTrust:
		op, ok := operation.Body.GetAllowTrustOp()
		if !ok {
			return Details{}, fmt.Errorf("Could not access AllowTrust info for this operation (index %d)", operationIndex)
		}

		addAssetDetailsToOperationDetails(&outputDetails, op.Asset.ToAsset(sourceAccount.ToAccountId()), "")
		outputDetails.Trustee = sourceAccountAddress
		outputDetails.Trustor = op.Trustor.Address()
		shouldAuth := xdr.TrustLineFlags(op.Authorize).IsAuthorized()
		outputDetails.Authorize = shouldAuth

	case xdr.OperationTypeAccountMerge:
		destinationAccount, ok := operation.Body.GetDestination()
		if !ok {
			return Details{}, fmt.Errorf("Could not access Destination info for this operation (index %d)", operationIndex)
		}

		destinationAccountAddress, err := utils.GetAccountAddressFromMuxedAccount(destinationAccount)
		if err != nil {
			return Details{}, err
		}

		outputDetails.Account = sourceAccountAddress
		outputDetails.Into = destinationAccountAddress

	case xdr.OperationTypeInflation:
		// Inflation operations don't have information that affects the details struct
	case xdr.OperationTypeManageData:
		op, ok := operation.Body.GetManageDataOp()
		if !ok {
			return Details{}, fmt.Errorf("Could not access GetManageData info for this operation (index %d)", operationIndex)
		}

		outputDetails.Name = string(op.DataName)
		if op.DataValue != nil {
			outputDetails.Value = base64.StdEncoding.EncodeToString(*op.DataValue)
		} else {
			outputDetails.Value = ""
		}

	case xdr.OperationTypeBumpSequence:
		op, ok := operation.Body.GetBumpSequenceOp()
		if !ok {
			return Details{}, fmt.Errorf("Could not access BumpSequence info for this operation (index %d)", operationIndex)
		}
		outputDetails.BumpTo = fmt.Sprintf("%d", op.BumpTo)

	default:
		return Details{}, fmt.Errorf("Unknown operation type: %s", operation.Body.Type.String())
	}
	return outputDetails, nil
}
