package transform

import (
	"encoding/base64"
	"fmt"
	"strconv"

	"github.com/guregu/null"
	"github.com/pkg/errors"
	"github.com/stellar/stellar-etl/internal/toid"
	"github.com/stellar/stellar-etl/internal/utils"

	"github.com/stellar/go/amount"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

// TransformOperation converts an operation from the history archive ingestion system into a form suitable for BigQuery
func TransformOperation(operation xdr.Operation, operationIndex int32, transaction ingest.LedgerTransaction, ledgerSeq int32) (OperationOutput, error) {
	outputTransactionID := toid.New(ledgerSeq, int32(transaction.Index), 0).ToInt64()
	outputOperationID := toid.New(ledgerSeq, int32(transaction.Index), operationIndex+1).ToInt64() //operationIndex needs +1 increment to stay in sync with ingest package

	sourceAccount := getOperationSourceAccount(operation, transaction)
	outputSourceAccount, err := utils.GetAccountAddressFromMuxedAccount(sourceAccount)
	if err != nil {
		return OperationOutput{}, fmt.Errorf("for operation %d (ledger id=%d): %v", operationIndex, outputOperationID, err)
	}

	var outputSourceAccountMuxed null.String
	if sourceAccount.Type == xdr.CryptoKeyTypeKeyTypeMuxedEd25519 {
		muxedAddress, err := sourceAccount.GetAddress()
		if err != nil {
			return OperationOutput{}, err
		}
		outputSourceAccountMuxed = null.StringFrom(muxedAddress)
	}

	outputOperationType := int32(operation.Body.Type)
	if outputOperationType < 0 {
		return OperationOutput{}, fmt.Errorf("The operation type (%d) is negative for  operation %d (operation id=%d)", outputOperationType, operationIndex, outputOperationID)
	}

	outputDetails, err := extractOperationDetails(operation, transaction, operationIndex)
	if err != nil {
		return OperationOutput{}, err
	}

	transformedOperation := OperationOutput{
		SourceAccount:      outputSourceAccount,
		SourceAccountMuxed: outputSourceAccountMuxed.String,
		Type:               outputOperationType,
		ApplicationOrder:   operationIndex + 1, // Application order is 1-indexed
		TransactionID:      outputTransactionID,
		OperationID:        outputOperationID,
		OperationDetails:   outputDetails,
	}

	return transformedOperation, nil
}

func getOperationSourceAccount(operation xdr.Operation, transaction ingest.LedgerTransaction) xdr.MuxedAccount {
	sourceAccount := operation.SourceAccount
	if sourceAccount != nil {
		return *sourceAccount
	}

	return transaction.Envelope.SourceAccount()
}

func formatPrefix(p string) string {
	if p != "" {
		p += "_"
	}
	return p
}

func addAssetDetailsToOperationDetails(result map[string]interface{}, asset xdr.Asset, prefix string) error {
	var assetType, code, issuer string
	err := asset.Extract(&assetType, &code, &issuer)
	if err != nil {
		return err
	}

	prefix = formatPrefix(prefix)
	result[prefix+"asset_type"] = assetType

	if asset.Type == xdr.AssetTypeAssetTypeNative {
		return nil
	}

	result[prefix+"asset_code"] = code
	result[prefix+"asset_issuer"] = issuer

	return nil
}

func addAccountAndMuxedAccountDetails(result map[string]interface{}, a xdr.MuxedAccount, prefix string) error {
	account_id := a.ToAccountId()
	result[prefix] = account_id.Address()
	prefix = formatPrefix(prefix)
	if a.Type == xdr.CryptoKeyTypeKeyTypeMuxedEd25519 {
		muxedAccountAddress, err := a.GetAddress()
		if err != nil {
			return err
		}
		result[prefix+"muxed"] = muxedAccountAddress
		muxedAccountId, err := a.GetId()
		if err != nil {
			return err
		}
		result[prefix+"muxed_id"] = muxedAccountId
	}
	return nil
}

func addTrustLineFlagToDetails(result map[string]interface{}, f xdr.TrustLineFlags, prefix string) {
	var (
		n []int32
		s []string
	)

	if f.IsAuthorized() {
		n = append(n, int32(xdr.TrustLineFlagsAuthorizedFlag))
		s = append(s, "authorized")
	}

	if f.IsAuthorizedToMaintainLiabilitiesFlag() {
		n = append(n, int32(xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag))
		s = append(s, "authorized_to_maintain_liabilities")
	}

	if f.IsClawbackEnabledFlag() {
		n = append(n, int32(xdr.TrustLineFlagsTrustlineClawbackEnabledFlag))
		s = append(s, "clawback_enabled")
	}

	prefix = formatPrefix(prefix)
	result[prefix+"flags"] = n
	result[prefix+"flags_s"] = s
}

func addLedgerKeyToDetails(result map[string]interface{}, ledgerKey xdr.LedgerKey) error {
	switch ledgerKey.Type {
	case xdr.LedgerEntryTypeAccount:
		result["account_id"] = ledgerKey.Account.AccountId.Address()
	case xdr.LedgerEntryTypeClaimableBalance:
		marshalHex, err := xdr.MarshalHex(ledgerKey.ClaimableBalance.BalanceId)
		if err != nil {
			return errors.Wrapf(err, "in claimable balance")
		}
		result["claimable_balance_id"] = marshalHex
	case xdr.LedgerEntryTypeData:
		result["data_account_id"] = ledgerKey.Data.AccountId.Address()
		result["data_name"] = string(ledgerKey.Data.DataName)
	case xdr.LedgerEntryTypeOffer:
		result["offer_id"] = int64(ledgerKey.Offer.OfferId)
	case xdr.LedgerEntryTypeTrustline:
		result["trustline_account_id"] = ledgerKey.TrustLine.AccountId.Address()
		result["trustline_asset"] = ledgerKey.TrustLine.Asset.ToAsset().StringCanonical()
	}
	return nil
}

func transformPath(initialPath []xdr.Asset) []Path {
	if len(initialPath) == 0 {
		return nil
	}
	var path = make([]Path, 0)
	for _, pathAsset := range initialPath {
		var assetType, code, issuer string
		err := pathAsset.Extract(&assetType, &code, &issuer)
		if err != nil {
			return nil
		}

		path = append(path, Path{
			AssetType:   assetType,
			AssetIssuer: issuer,
			AssetCode:   code,
		})
	}
	return path
}

func findInitatingBeginSponsoringOp(operation xdr.Operation, operationIndex int32, transaction ingest.LedgerTransaction) *SponsorshipOutput {
	if !transaction.Result.Successful() {
		// Failed transactions may not have a compliant sandwich structure
		// we can rely on (e.g. invalid nesting or a being operation with the wrong sponsoree ID)
		// and thus we bail out since we could return incorrect information.
		return nil
	}
	sponsoree := getOperationSourceAccount(operation, transaction).ToAccountId()
	operations := transaction.Envelope.Operations()
	for i := int(operationIndex) - 1; i >= 0; i-- {
		if beginOp, ok := operations[i].Body.GetBeginSponsoringFutureReservesOp(); ok &&
			beginOp.SponsoredId.Address() == sponsoree.Address() {
			result := SponsorshipOutput{
				Operation:      operations[i],
				OperationIndex: uint32(i),
			}
			return &result
		}
	}
	return nil
}

func addOperationFlagToOperationDetails(result map[string]interface{}, flag uint32, prefix string) {
	intFlags := make([]int32, 0)
	stringFlags := make([]string, 0)

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

	if (int64(flag) & int64(xdr.AccountFlagsAuthClawbackEnabledFlag)) > 0 {
		intFlags = append(intFlags, int32(xdr.AccountFlagsAuthClawbackEnabledFlag))
		stringFlags = append(stringFlags, "auth_clawback_enabled")
	}

	prefix = formatPrefix(prefix)
	result[prefix+"flags"] = intFlags
	result[prefix+"flags_s"] = stringFlags
}

func extractOperationDetails(operation xdr.Operation, transaction ingest.LedgerTransaction, operationIndex int32) (map[string]interface{}, error) {
	details := map[string]interface{}{}
	sourceAccount := getOperationSourceAccount(operation, transaction)
	operationType := operation.Body.Type
	allOperationResults, ok := transaction.Result.OperationResults()
	if !ok {
		return details, fmt.Errorf("Could not access any results for this transaction")
	}

	currentOperationResult := allOperationResults[operationIndex]
	switch operationType {
	case xdr.OperationTypeCreateAccount:
		op, ok := operation.Body.GetCreateAccountOp()
		if !ok {
			return details, fmt.Errorf("Could not access CreateAccount info for this operation (index %d)", operationIndex)
		}

		if err := addAccountAndMuxedAccountDetails(details, sourceAccount, "funder"); err != nil {
			return details, err
		}
		details["account"] = op.Destination.Address()
		details["starting_balance"] = utils.ConvertStroopValueToReal(op.StartingBalance)

	case xdr.OperationTypePayment:
		op, ok := operation.Body.GetPaymentOp()
		if !ok {
			return details, fmt.Errorf("Could not access Payment info for this operation (index %d)", operationIndex)
		}

		if err := addAccountAndMuxedAccountDetails(details, sourceAccount, "from"); err != nil {
			return details, err
		}
		if err := addAccountAndMuxedAccountDetails(details, op.Destination, "to"); err != nil {
			return details, err
		}
		details["amount"] = utils.ConvertStroopValueToReal(op.Amount)
		if err := addAssetDetailsToOperationDetails(details, op.Asset, ""); err != nil {
			return details, err
		}

	case xdr.OperationTypePathPaymentStrictReceive:
		op, ok := operation.Body.GetPathPaymentStrictReceiveOp()
		if !ok {
			return details, fmt.Errorf("Could not access PathPaymentStrictReceive info for this operation (index %d)", operationIndex)
		}

		if err := addAccountAndMuxedAccountDetails(details, sourceAccount, "from"); err != nil {
			return details, err
		}
		if err := addAccountAndMuxedAccountDetails(details, op.Destination, "to"); err != nil {
			return details, err
		}
		details["amount"] = utils.ConvertStroopValueToReal(op.DestAmount)
		details["source_amount"] = amount.String(0)
		details["source_max"] = utils.ConvertStroopValueToReal(op.SendMax)
		if err := addAssetDetailsToOperationDetails(details, op.DestAsset, ""); err != nil {
			return details, err
		}
		if err := addAssetDetailsToOperationDetails(details, op.SendAsset, "source"); err != nil {
			return details, err
		}

		if transaction.Result.Successful() {
			resultBody, ok := currentOperationResult.GetTr()
			if !ok {
				return details, fmt.Errorf("Could not access result body for this operation (index %d)", operationIndex)
			}
			result, ok := resultBody.GetPathPaymentStrictReceiveResult()
			if !ok {
				return details, fmt.Errorf("Could not access PathPaymentStrictReceive result info for this operation (index %d)", operationIndex)
			}
			details["source_amount"] = utils.ConvertStroopValueToReal(result.SendAmount())
		}

		details["path"] = transformPath(op.Path)

	case xdr.OperationTypePathPaymentStrictSend:
		op, ok := operation.Body.GetPathPaymentStrictSendOp()
		if !ok {
			return details, fmt.Errorf("Could not access PathPaymentStrictSend info for this operation (index %d)", operationIndex)
		}

		if err := addAccountAndMuxedAccountDetails(details, sourceAccount, "from"); err != nil {
			return details, err
		}
		if err := addAccountAndMuxedAccountDetails(details, op.Destination, "to"); err != nil {
			return details, err
		}
		details["amount"] = amount.String(0)
		details["source_amount"] = utils.ConvertStroopValueToReal(op.SendAmount)
		details["destination_min"] = amount.String(op.DestMin)
		if err := addAssetDetailsToOperationDetails(details, op.DestAsset, ""); err != nil {
			return details, err
		}
		if err := addAssetDetailsToOperationDetails(details, op.SendAsset, "source"); err != nil {
			return details, err
		}

		if transaction.Result.Successful() {
			resultBody, ok := currentOperationResult.GetTr()
			if !ok {
				return details, fmt.Errorf("Could not access result body for this operation (index %d)", operationIndex)
			}
			result, ok := resultBody.GetPathPaymentStrictSendResult()
			if !ok {
				return details, fmt.Errorf("Could not access GetPathPaymentStrictSendResult result info for this operation (index %d)", operationIndex)
			}
			details["amount"] = utils.ConvertStroopValueToReal(result.DestAmount())
		}

		details["path"] = transformPath(op.Path)

	case xdr.OperationTypeManageBuyOffer:
		op, ok := operation.Body.GetManageBuyOfferOp()
		if !ok {
			return details, fmt.Errorf("Could not access ManageBuyOffer info for this operation (index %d)", operationIndex)
		}

		details["offer_id"] = int64(op.OfferId)
		details["amount"] = utils.ConvertStroopValueToReal(op.BuyAmount)
		parsedPrice, err := strconv.ParseFloat(op.Price.String(), 64)
		if err != nil {
			return details, err
		}

		details["price"] = parsedPrice
		details["price_r"] = Price{
			Numerator:   int32(op.Price.N),
			Denominator: int32(op.Price.D),
		}
		if err := addAssetDetailsToOperationDetails(details, op.Buying, "buying"); err != nil {
			return details, err
		}
		if err := addAssetDetailsToOperationDetails(details, op.Selling, "selling"); err != nil {
			return details, err
		}

	case xdr.OperationTypeManageSellOffer:
		op, ok := operation.Body.GetManageSellOfferOp()
		if !ok {
			return details, fmt.Errorf("Could not access ManageSellOffer info for this operation (index %d)", operationIndex)
		}

		details["offer_id"] = int64(op.OfferId)
		details["amount"] = utils.ConvertStroopValueToReal(op.Amount)
		parsedPrice, err := strconv.ParseFloat(op.Price.String(), 64)
		if err != nil {
			return details, err
		}

		details["price"] = parsedPrice
		details["price_r"] = Price{
			Numerator:   int32(op.Price.N),
			Denominator: int32(op.Price.D),
		}
		if err := addAssetDetailsToOperationDetails(details, op.Buying, "buying"); err != nil {
			return details, err
		}
		if err := addAssetDetailsToOperationDetails(details, op.Selling, "selling"); err != nil {
			return details, err
		}

	case xdr.OperationTypeCreatePassiveSellOffer:
		op, ok := operation.Body.GetCreatePassiveSellOfferOp()
		if !ok {
			return details, fmt.Errorf("Could not access CreatePassiveSellOffer info for this operation (index %d)", operationIndex)
		}

		details["amount"] = utils.ConvertStroopValueToReal(op.Amount)
		parsedPrice, err := strconv.ParseFloat(op.Price.String(), 64)
		if err != nil {
			return details, err
		}

		details["price"] = parsedPrice
		details["price_r"] = Price{
			Numerator:   int32(op.Price.N),
			Denominator: int32(op.Price.D),
		}
		if err := addAssetDetailsToOperationDetails(details, op.Buying, "buying"); err != nil {
			return details, err
		}
		if err := addAssetDetailsToOperationDetails(details, op.Selling, "selling"); err != nil {
			return details, err
		}

	case xdr.OperationTypeSetOptions:
		op, ok := operation.Body.GetSetOptionsOp()
		if !ok {
			return details, fmt.Errorf("Could not access GetSetOptions info for this operation (index %d)", operationIndex)
		}

		if op.InflationDest != nil {
			details["inflation_dest"] = op.InflationDest.Address()
		}

		if op.SetFlags != nil && *op.SetFlags > 0 {
			addOperationFlagToOperationDetails(details, uint32(*op.SetFlags), "set")
		}

		if op.ClearFlags != nil && *op.ClearFlags > 0 {
			addOperationFlagToOperationDetails(details, uint32(*op.ClearFlags), "clear")
		}

		if op.MasterWeight != nil {
			details["master_key_weight"] = uint32(*op.MasterWeight)
		}

		if op.LowThreshold != nil {
			details["low_threshold"] = uint32(*op.LowThreshold)
		}

		if op.MedThreshold != nil {
			details["med_threshold"] = uint32(*op.MedThreshold)
		}

		if op.HighThreshold != nil {
			details["high_threshold"] = uint32(*op.HighThreshold)
		}

		if op.HomeDomain != nil {
			details["home_domain"] = string(*op.HomeDomain)
		}

		if op.Signer != nil {
			details["signer_key"] = op.Signer.Key.Address()
			details["signer_weight"] = uint32(op.Signer.Weight)
		}

	case xdr.OperationTypeChangeTrust:
		op, ok := operation.Body.GetChangeTrustOp()
		if !ok {
			return details, fmt.Errorf("Could not access GetChangeTrust info for this operation (index %d)", operationIndex)
		}

		if err := addAssetDetailsToOperationDetails(details, op.Line.ToAsset(), ""); err != nil {
			return details, err
		}
		if err := addAccountAndMuxedAccountDetails(details, sourceAccount, "trustor"); err != nil {
			return details, err
		}
		details["trustee"] = details["asset_issuer"]
		details["limit"] = utils.ConvertStroopValueToReal(op.Limit)

	case xdr.OperationTypeAllowTrust:
		op, ok := operation.Body.GetAllowTrustOp()
		if !ok {
			return details, fmt.Errorf("Could not access AllowTrust info for this operation (index %d)", operationIndex)
		}

		if err := addAssetDetailsToOperationDetails(details, op.Asset.ToAsset(sourceAccount.ToAccountId()), ""); err != nil {
			return details, err
		}
		if err := addAccountAndMuxedAccountDetails(details, sourceAccount, "trustee"); err != nil {
			return details, err
		}
		details["trustor"] = op.Trustor.Address()
		shouldAuth := xdr.TrustLineFlags(op.Authorize).IsAuthorized()
		details["authorize"] = shouldAuth
		shouldAuthLiabilities := xdr.TrustLineFlags(op.Authorize).IsAuthorizedToMaintainLiabilitiesFlag()
		if shouldAuthLiabilities {
			details["authorize_to_maintain_liabilities"] = shouldAuthLiabilities
		}
		shouldClawbackEnabled := xdr.TrustLineFlags(op.Authorize).IsClawbackEnabledFlag()
		if shouldClawbackEnabled {
			details["clawback_enabled"] = shouldClawbackEnabled
		}

	case xdr.OperationTypeAccountMerge:
		destinationAccount, ok := operation.Body.GetDestination()
		if !ok {
			return details, fmt.Errorf("Could not access Destination info for this operation (index %d)", operationIndex)
		}

		if err := addAccountAndMuxedAccountDetails(details, sourceAccount, "account"); err != nil {
			return details, err
		}
		if err := addAccountAndMuxedAccountDetails(details, destinationAccount, "into"); err != nil {
			return details, err
		}

	case xdr.OperationTypeInflation:
		// Inflation operations don't have information that affects the details struct
	case xdr.OperationTypeManageData:
		op, ok := operation.Body.GetManageDataOp()
		if !ok {
			return details, fmt.Errorf("Could not access GetManageData info for this operation (index %d)", operationIndex)
		}

		details["name"] = string(op.DataName)
		if op.DataValue != nil {
			details["value"] = base64.StdEncoding.EncodeToString(*op.DataValue)
		} else {
			details["value"] = nil
		}

	case xdr.OperationTypeBumpSequence:
		op, ok := operation.Body.GetBumpSequenceOp()
		if !ok {
			return details, fmt.Errorf("Could not access BumpSequence info for this operation (index %d)", operationIndex)
		}
		details["bump_to"] = fmt.Sprintf("%d", op.BumpTo)

	case xdr.OperationTypeCreateClaimableBalance:
		op := operation.Body.MustCreateClaimableBalanceOp()
		details["asset"] = op.Asset.StringCanonical()
		details["amount"] = utils.ConvertStroopValueToReal(op.Amount)
		var claimants []Claimant
		for _, c := range op.Claimants {
			cv0 := c.MustV0()
			claimants = append(claimants, Claimant{
				Destination: cv0.Destination.Address(),
				Predicate:   cv0.Predicate,
			})
		}
		details["claimants"] = claimants

	case xdr.OperationTypeClaimClaimableBalance:
		op := operation.Body.MustClaimClaimableBalanceOp()
		balanceID, err := xdr.MarshalHex(op.BalanceId)
		if err != nil {
			return details, fmt.Errorf("Invalid balanceId in op: %d", operationIndex)
		}
		details["balance_id"] = balanceID
		if err := addAccountAndMuxedAccountDetails(details, sourceAccount, "claimant"); err != nil {
			return details, err
		}

	case xdr.OperationTypeBeginSponsoringFutureReserves:
		op := operation.Body.MustBeginSponsoringFutureReservesOp()
		details["sponsored_id"] = op.SponsoredId.Address()

	case xdr.OperationTypeEndSponsoringFutureReserves:
		beginSponsorOp := findInitatingBeginSponsoringOp(operation, operationIndex, transaction)
		if beginSponsorOp != nil {
			beginSponsorshipSource := getOperationSourceAccount(beginSponsorOp.Operation, transaction)
			if err := addAccountAndMuxedAccountDetails(details, beginSponsorshipSource, "begin_sponsor"); err != nil {
				return details, err
			}
		}

	case xdr.OperationTypeRevokeSponsorship:
		op := operation.Body.MustRevokeSponsorshipOp()
		switch op.Type {
		case xdr.RevokeSponsorshipTypeRevokeSponsorshipLedgerEntry:
			if err := addLedgerKeyToDetails(details, *op.LedgerKey); err != nil {
				return details, err
			}
		case xdr.RevokeSponsorshipTypeRevokeSponsorshipSigner:
			details["signer_account_id"] = op.Signer.AccountId.Address()
			details["signer_key"] = op.Signer.SignerKey.Address()
		}

	case xdr.OperationTypeClawback:
		op := operation.Body.MustClawbackOp()
		if err := addAssetDetailsToOperationDetails(details, op.Asset, ""); err != nil {
			return details, err
		}
		if err := addAccountAndMuxedAccountDetails(details, op.From, "from"); err != nil {
			return details, err
		}
		details["amount"] = utils.ConvertStroopValueToReal(op.Amount)

	case xdr.OperationTypeClawbackClaimableBalance:
		op := operation.Body.MustClawbackClaimableBalanceOp()
		balanceID, err := xdr.MarshalHex(op.BalanceId)
		if err != nil {
			return details, fmt.Errorf("Invalid balanceId in op: %d", operationIndex)
		}
		details["balance_id"] = balanceID

	case xdr.OperationTypeSetTrustLineFlags:
		op := operation.Body.MustSetTrustLineFlagsOp()
		details["trustor"] = op.Trustor.Address()
		if err := addAssetDetailsToOperationDetails(details, op.Asset, ""); err != nil {
			return details, err
		}
		if op.SetFlags > 0 {
			addTrustLineFlagToDetails(details, xdr.TrustLineFlags(op.SetFlags), "set")

		}
		if op.ClearFlags > 0 {
			addTrustLineFlagToDetails(details, xdr.TrustLineFlags(op.ClearFlags), "clear")
		}

	default:
		return details, fmt.Errorf("Unknown operation type: %s", operation.Body.Type.String())
	}

	return details, nil
}
