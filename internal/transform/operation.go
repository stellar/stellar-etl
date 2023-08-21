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
	"github.com/stellar/go/protocols/horizon/base"
	"github.com/stellar/go/xdr"

	"github.com/stellar/go/support/contractevents"
)

type liquidityPoolDelta struct {
	ReserveA        xdr.Int64
	ReserveB        xdr.Int64
	TotalPoolShares xdr.Int64
}

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

	outputOperationTypeString, err := mapOperationType(operation)
	if err != nil {
		return OperationOutput{}, err
	}

	transformedOperation := OperationOutput{
		SourceAccount:      outputSourceAccount,
		SourceAccountMuxed: outputSourceAccountMuxed.String,
		Type:               outputOperationType,
		TypeString:         outputOperationTypeString,
		TransactionID:      outputTransactionID,
		OperationID:        outputOperationID,
		OperationDetails:   outputDetails,
	}

	return transformedOperation, nil
}

func mapOperationType(operation xdr.Operation) (string, error) {
	var op_string_type string
	operationType := operation.Body.Type

	switch operationType {
	case xdr.OperationTypeCreateAccount:
		op_string_type = "create_account"
	case xdr.OperationTypePayment:
		op_string_type = "payment"
	case xdr.OperationTypePathPaymentStrictReceive:
		op_string_type = "path_payment_strict_receive"
	case xdr.OperationTypePathPaymentStrictSend:
		op_string_type = "path_payment_strict_send"
	case xdr.OperationTypeManageBuyOffer:
		op_string_type = "manage_buy_offer"
	case xdr.OperationTypeManageSellOffer:
		op_string_type = "manage_sell_offer"
	case xdr.OperationTypeCreatePassiveSellOffer:
		op_string_type = "create_passive_sell_offer"
	case xdr.OperationTypeSetOptions:
		op_string_type = "set_options"
	case xdr.OperationTypeChangeTrust:
		op_string_type = "change_trust"
	case xdr.OperationTypeAllowTrust:
		op_string_type = "allow_trust"
	case xdr.OperationTypeAccountMerge:
		op_string_type = "account_merge"
	case xdr.OperationTypeInflation:
		op_string_type = "inflation"
	case xdr.OperationTypeManageData:
		op_string_type = "manage_data"
	case xdr.OperationTypeBumpSequence:
		op_string_type = "bump_sequence"
	case xdr.OperationTypeCreateClaimableBalance:
		op_string_type = "create_claimable_balance"
	case xdr.OperationTypeClaimClaimableBalance:
		op_string_type = "claim_claimable_balance"
	case xdr.OperationTypeBeginSponsoringFutureReserves:
		op_string_type = "begin_sponsoring_future_reserves"
	case xdr.OperationTypeEndSponsoringFutureReserves:
		op_string_type = "end_sponsoring_future_reserves"
	case xdr.OperationTypeRevokeSponsorship:
		op_string_type = "revoke_sponsorship"
	case xdr.OperationTypeClawback:
		op_string_type = "clawback"
	case xdr.OperationTypeClawbackClaimableBalance:
		op_string_type = "clawback_claimable_balance"
	case xdr.OperationTypeSetTrustLineFlags:
		op_string_type = "set_trust_line_flags"
	case xdr.OperationTypeLiquidityPoolDeposit:
		op_string_type = "liquidity_pool_deposit"
	case xdr.OperationTypeLiquidityPoolWithdraw:
		op_string_type = "liquidity_pool_withdraw"
	case xdr.OperationTypeInvokeHostFunction:
		op_string_type = "invoke_host_function"
	case xdr.OperationTypeBumpFootprintExpiration:
		op_string_type = "bump_footprint_expiration"
	case xdr.OperationTypeRestoreFootprint:
		op_string_type = "restore_footprint"
	default:
		return op_string_type, fmt.Errorf("Unknown operation type: %s", operation.Body.Type.String())
	}
	return op_string_type, nil
}

func PoolIDToString(id xdr.PoolId) string {
	return xdr.Hash(id).HexString()
}

// operation xdr.Operation, operationIndex int32, transaction ingest.LedgerTransaction, ledgerSeq int32
func getLiquidityPoolAndProductDelta(operationIndex int32, transaction ingest.LedgerTransaction, lpID *xdr.PoolId) (*xdr.LiquidityPoolEntry, *liquidityPoolDelta, error) {
	changes, err := transaction.GetOperationChanges(uint32(operationIndex))
	if err != nil {
		return nil, nil, err
	}

	for _, c := range changes {
		if c.Type != xdr.LedgerEntryTypeLiquidityPool {
			continue
		}
		// The delta can be caused by a full removal or full creation of the liquidity pool
		var lp *xdr.LiquidityPoolEntry
		var preA, preB, preShares xdr.Int64
		if c.Pre != nil {
			if lpID != nil && c.Pre.Data.LiquidityPool.LiquidityPoolId != *lpID {
				// if we were looking for specific pool id, then check on it
				continue
			}
			lp = c.Pre.Data.LiquidityPool
			if c.Pre.Data.LiquidityPool.Body.Type != xdr.LiquidityPoolTypeLiquidityPoolConstantProduct {
				return nil, nil, fmt.Errorf("unexpected liquity pool body type %d", c.Pre.Data.LiquidityPool.Body.Type)
			}
			cpPre := c.Pre.Data.LiquidityPool.Body.ConstantProduct
			preA, preB, preShares = cpPre.ReserveA, cpPre.ReserveB, cpPre.TotalPoolShares
		}
		var postA, postB, postShares xdr.Int64
		if c.Post != nil {
			if lpID != nil && c.Post.Data.LiquidityPool.LiquidityPoolId != *lpID {
				// if we were looking for specific pool id, then check on it
				continue
			}
			lp = c.Post.Data.LiquidityPool
			if c.Post.Data.LiquidityPool.Body.Type != xdr.LiquidityPoolTypeLiquidityPoolConstantProduct {
				return nil, nil, fmt.Errorf("unexpected liquity pool body type %d", c.Post.Data.LiquidityPool.Body.Type)
			}
			cpPost := c.Post.Data.LiquidityPool.Body.ConstantProduct
			postA, postB, postShares = cpPost.ReserveA, cpPost.ReserveB, cpPost.TotalPoolShares
		}
		delta := &liquidityPoolDelta{
			ReserveA:        postA - preA,
			ReserveB:        postB - preB,
			TotalPoolShares: postShares - preShares,
		}
		return lp, delta, nil
	}

	return nil, nil, fmt.Errorf("Liquidity pool change not found")
}

func getOperationSourceAccount(operation xdr.Operation, transaction ingest.LedgerTransaction) xdr.MuxedAccount {
	sourceAccount := operation.SourceAccount
	if sourceAccount != nil {
		return *sourceAccount
	}

	return transaction.Envelope.SourceAccount()
}

func getSponsor(operation xdr.Operation, transaction ingest.LedgerTransaction, operationIndex int32) (*xdr.AccountId, error) {
	changes, err := transaction.GetOperationChanges(uint32(operationIndex))
	if err != nil {
		return nil, err
	}
	var signerKey string
	if setOps, ok := operation.Body.GetSetOptionsOp(); ok && setOps.Signer != nil {
		signerKey = setOps.Signer.Key.Address()
	}

	for _, c := range changes {
		// Check Signer changes
		if signerKey != "" {
			if sponsorAccount := getSignerSponsorInChange(signerKey, c); sponsorAccount != nil {
				return sponsorAccount, nil
			}
		}

		// Check Ledger key changes
		if c.Pre != nil || c.Post == nil {
			// We are only looking for entry creations denoting that a sponsor
			// is associated to the ledger entry of the operation.
			continue
		}
		if sponsorAccount := c.Post.SponsoringID(); sponsorAccount != nil {
			return sponsorAccount, nil
		}
	}

	return nil, nil
}

func getSignerSponsorInChange(signerKey string, change ingest.Change) xdr.SponsorshipDescriptor {
	if change.Type != xdr.LedgerEntryTypeAccount || change.Post == nil {
		return nil
	}

	preSigners := map[string]xdr.AccountId{}
	if change.Pre != nil {
		account := change.Pre.Data.MustAccount()
		preSigners = account.SponsorPerSigner()
	}

	account := change.Post.Data.MustAccount()
	postSigners := account.SponsorPerSigner()

	pre, preFound := preSigners[signerKey]
	post, postFound := postSigners[signerKey]

	if !postFound {
		return nil
	}

	if preFound {
		formerSponsor := pre.Address()
		newSponsor := post.Address()
		if formerSponsor == newSponsor {
			return nil
		}
	}

	return &post
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
		result[prefix+"asset_id"] = int64(-5706705804583548011)
		return nil
	}

	result[prefix+"asset_code"] = code
	result[prefix+"asset_issuer"] = issuer
	result[prefix+"asset_id"] = FarmHashAsset(code, issuer, assetType)

	return nil
}

func addLiquidityPoolAssetDetails(result map[string]interface{}, lpp xdr.LiquidityPoolParameters) error {
	result["asset_type"] = "liquidity_pool_shares"
	if lpp.Type != xdr.LiquidityPoolTypeLiquidityPoolConstantProduct {
		return fmt.Errorf("unknown liquidity pool type %d", lpp.Type)
	}
	cp := lpp.ConstantProduct
	poolID, err := xdr.NewPoolId(cp.AssetA, cp.AssetB, cp.Fee)
	if err != nil {
		return err
	}
	result["liquidity_pool_id"] = PoolIDToString(poolID)
	return nil
}

func addPriceDetails(result map[string]interface{}, price xdr.Price, prefix string) error {
	prefix = formatPrefix(prefix)
	parsedPrice, err := strconv.ParseFloat(price.String(), 64)
	if err != nil {
		return err
	}
	result[prefix+"price"] = parsedPrice
	result[prefix+"price_r"] = Price{
		Numerator:   int32(price.N),
		Denominator: int32(price.D),
	}
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
		if ledgerKey.TrustLine.Asset.Type == xdr.AssetTypeAssetTypePoolShare {
			result["trustline_liquidity_pool_id"] = PoolIDToString(*ledgerKey.TrustLine.Asset.LiquidityPoolId)
		} else {
			result["trustline_asset"] = ledgerKey.TrustLine.Asset.ToAsset().StringCanonical()
		}
	case xdr.LedgerEntryTypeLiquidityPool:
		result["liquidity_pool_id"] = PoolIDToString(ledgerKey.LiquidityPool.LiquidityPoolId)
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
			allOperationResults, ok := transaction.Result.OperationResults()
			if !ok {
				return details, fmt.Errorf("Could not access any results for this transaction")
			}
			currentOperationResult := allOperationResults[operationIndex]
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
			allOperationResults, ok := transaction.Result.OperationResults()
			if !ok {
				return details, fmt.Errorf("Could not access any results for this transaction")
			}
			currentOperationResult := allOperationResults[operationIndex]
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
		if err := addPriceDetails(details, op.Price, ""); err != nil {
			return details, err
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
		if err := addPriceDetails(details, op.Price, ""); err != nil {
			return details, err
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
		if err := addPriceDetails(details, op.Price, ""); err != nil {
			return details, err
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

		if op.Line.Type == xdr.AssetTypeAssetTypePoolShare {
			if err := addLiquidityPoolAssetDetails(details, *op.Line.LiquidityPool); err != nil {
				return details, err
			}
		} else {
			if err := addAssetDetailsToOperationDetails(details, op.Line.ToAsset(), ""); err != nil {
				return details, err
			}
			details["trustee"] = details["asset_issuer"]
		}

		if err := addAccountAndMuxedAccountDetails(details, sourceAccount, "trustor"); err != nil {
			return details, err
		}
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
		details["claimants"] = transformClaimants(op.Claimants)

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

	case xdr.OperationTypeLiquidityPoolDeposit:
		op := operation.Body.MustLiquidityPoolDepositOp()
		details["liquidity_pool_id"] = PoolIDToString(op.LiquidityPoolId)
		var (
			assetA, assetB         xdr.Asset
			depositedA, depositedB xdr.Int64
			sharesReceived         xdr.Int64
		)
		if transaction.Result.Successful() {
			// we will use the defaults (omitted asset and 0 amounts) if the transaction failed
			lp, delta, err := getLiquidityPoolAndProductDelta(operationIndex, transaction, &op.LiquidityPoolId)
			if err != nil {
				return nil, err
			}
			params := lp.Body.ConstantProduct.Params
			assetA, assetB = params.AssetA, params.AssetB
			depositedA, depositedB = delta.ReserveA, delta.ReserveB
			sharesReceived = delta.TotalPoolShares
		}

		// Process ReserveA Details
		if err := addAssetDetailsToOperationDetails(details, assetA, "reserve_a"); err != nil {
			return details, err
		}
		details["reserve_a_max_amount"] = utils.ConvertStroopValueToReal(op.MaxAmountA)
		depositA, err := strconv.ParseFloat(amount.String(depositedA), 64)
		if err != nil {
			return details, err
		}
		details["reserve_a_deposit_amount"] = depositA

		//Process ReserveB Details
		if err := addAssetDetailsToOperationDetails(details, assetB, "reserve_b"); err != nil {
			return details, err
		}
		details["reserve_b_max_amount"] = utils.ConvertStroopValueToReal(op.MaxAmountB)
		depositB, err := strconv.ParseFloat(amount.String(depositedB), 64)
		if err != nil {
			return details, err
		}
		details["reserve_b_deposit_amount"] = depositB

		if err := addPriceDetails(details, op.MinPrice, "min"); err != nil {
			return details, err
		}
		if err := addPriceDetails(details, op.MaxPrice, "max"); err != nil {
			return details, err
		}

		sharesToFloat, err := strconv.ParseFloat(amount.String(sharesReceived), 64)
		if err != nil {
			return details, err
		}
		details["shares_received"] = sharesToFloat

	case xdr.OperationTypeLiquidityPoolWithdraw:
		op := operation.Body.MustLiquidityPoolWithdrawOp()
		details["liquidity_pool_id"] = PoolIDToString(op.LiquidityPoolId)
		var (
			assetA, assetB       xdr.Asset
			receivedA, receivedB xdr.Int64
		)
		if transaction.Result.Successful() {
			// we will use the defaults (omitted asset and 0 amounts) if the transaction failed
			lp, delta, err := getLiquidityPoolAndProductDelta(operationIndex, transaction, &op.LiquidityPoolId)
			if err != nil {
				return nil, err
			}
			params := lp.Body.ConstantProduct.Params
			assetA, assetB = params.AssetA, params.AssetB
			receivedA, receivedB = -delta.ReserveA, -delta.ReserveB
		}
		// Process AssetA Details
		if err := addAssetDetailsToOperationDetails(details, assetA, "reserve_a"); err != nil {
			return details, err
		}
		details["reserve_a_min_amount"] = utils.ConvertStroopValueToReal(op.MinAmountA)
		details["reserve_a_withdraw_amount"] = utils.ConvertStroopValueToReal(receivedA)

		// Process AssetB Details
		if err := addAssetDetailsToOperationDetails(details, assetB, "reserve_b"); err != nil {
			return details, err
		}
		details["reserve_b_min_amount"] = utils.ConvertStroopValueToReal(op.MinAmountB)
		details["reserve_b_withdraw_amount"] = utils.ConvertStroopValueToReal(receivedB)

		details["shares"] = utils.ConvertStroopValueToReal(op.Amount)

	case xdr.OperationTypeInvokeHostFunction:
		op := operation.Body.MustInvokeHostFunctionOp()
		details["function"] = op.HostFunction.Type.String()

		switch op.HostFunction.Type {
		case xdr.HostFunctionTypeHostFunctionTypeInvokeContract:
			args := op.HostFunction.MustInvokeContract()
			details["type"] = "invoke_contract"
			params := make([]map[string]string, 0, len(args))

			transactionEnvelope := transaction.Envelope.MustV1()
			details["contract_id"] = contractIdFromTxEnvelope(transactionEnvelope)
			details["contract_code_hash"] = contractCodeHashFromTxEnvelope(transactionEnvelope)

			for _, param := range args {
				serializedParam := map[string]string{}
				serializedParam["value"] = "n/a"
				serializedParam["type"] = "n/a"

				if scValTypeName, ok := param.ArmForSwitch(int32(param.Type)); ok {
					serializedParam["type"] = scValTypeName
					if raw, err := param.MarshalBinary(); err == nil {
						serializedParam["value"] = base64.StdEncoding.EncodeToString(raw)
					}
				}
				params = append(params, serializedParam)
			}
			details["parameters"] = params

			if balanceChanges, err := parseAssetBalanceChangesFromContractEvents(transaction); err != nil {
				return nil, err
			} else {
				details["asset_balance_changes"] = balanceChanges
			}

		case xdr.HostFunctionTypeHostFunctionTypeCreateContract:
			args := op.HostFunction.MustCreateContract()
			details["type"] = "create_contract"

			transactionEnvelope := transaction.Envelope.MustV1()
			details["contract_id"] = contractIdFromTxEnvelope(transactionEnvelope)
			details["contract_code_hash"] = contractCodeHashFromTxEnvelope(transactionEnvelope)

			switch args.ContractIdPreimage.Type {
			case xdr.ContractIdPreimageTypeContractIdPreimageFromAddress:
				fromAddress := args.ContractIdPreimage.MustFromAddress()
				address, err := fromAddress.Address.String()
				if err != nil {
					panic(fmt.Errorf("error obtaining address for: %s", args.ContractIdPreimage.Type))
				}
				details["from"] = "address"
				details["address"] = address
				details["salt"] = fromAddress.Salt
			case xdr.ContractIdPreimageTypeContractIdPreimageFromAsset:
				details["from"] = "asset"
				details["asset"] = args.ContractIdPreimage.MustFromAsset().StringCanonical()
			default:
				panic(fmt.Errorf("unknown contract id type: %s", args.ContractIdPreimage.Type))
			}
		case xdr.HostFunctionTypeHostFunctionTypeUploadContractWasm:
			details["type"] = "upload_wasm"
			transactionEnvelope := transaction.Envelope.MustV1()
			details["contract_code_hash"] = contractCodeHashFromTxEnvelope(transactionEnvelope)
			details["contract_code_hash"] = contractCodeHashFromTxEnvelope(transactionEnvelope)
		default:
			panic(fmt.Errorf("unknown host function type: %s", op.HostFunction.Type))
		}
	case xdr.OperationTypeBumpFootprintExpiration:
		op := operation.Body.MustBumpFootprintExpirationOp()
		details["type"] = "bump_footprint_expiration"
		details["ledgers_to_expire"] = op.LedgersToExpire

		transactionEnvelope := transaction.Envelope.MustV1()
		details["contract_id"] = contractIdFromTxEnvelope(transactionEnvelope)
		details["contract_code_hash"] = contractCodeHashFromTxEnvelope(transactionEnvelope)
	case xdr.OperationTypeRestoreFootprint:
		details["type"] = "restore_footprint"

		transactionEnvelope := transaction.Envelope.MustV1()
		details["contract_id"] = contractIdFromTxEnvelope(transactionEnvelope)
		details["contract_code_hash"] = contractCodeHashFromTxEnvelope(transactionEnvelope)
	default:
		return details, fmt.Errorf("Unknown operation type: %s", operation.Body.Type.String())
	}

	sponsor, err := getSponsor(operation, transaction, operationIndex)
	if err != nil {
		return nil, err
	}
	if sponsor != nil {
		details["sponsor"] = sponsor.Address()
	}

	return details, nil
}

// transactionOperationWrapper represents the data for a single operation within a transaction
type transactionOperationWrapper struct {
	index          uint32
	transaction    ingest.LedgerTransaction
	operation      xdr.Operation
	ledgerSequence uint32
	network        string
}

// ID returns the ID for the operation.
func (operation *transactionOperationWrapper) ID() int64 {
	return toid.New(
		int32(operation.ledgerSequence),
		int32(operation.transaction.Index),
		int32(operation.index+1),
	).ToInt64()
}

// Order returns the operation order.
func (operation *transactionOperationWrapper) Order() uint32 {
	return operation.index + 1
}

// TransactionID returns the id for the transaction related with this operation.
func (operation *transactionOperationWrapper) TransactionID() int64 {
	return toid.New(int32(operation.ledgerSequence), int32(operation.transaction.Index), 0).ToInt64()
}

// SourceAccount returns the operation's source account.
func (operation *transactionOperationWrapper) SourceAccount() *xdr.MuxedAccount {
	sourceAccount := operation.operation.SourceAccount
	if sourceAccount != nil {
		return sourceAccount
	} else {
		ret := operation.transaction.Envelope.SourceAccount()
		return &ret
	}
}

// OperationType returns the operation type.
func (operation *transactionOperationWrapper) OperationType() xdr.OperationType {
	return operation.operation.Body.Type
}

func (operation *transactionOperationWrapper) getSignerSponsorInChange(signerKey string, change ingest.Change) xdr.SponsorshipDescriptor {
	if change.Type != xdr.LedgerEntryTypeAccount || change.Post == nil {
		return nil
	}

	preSigners := map[string]xdr.AccountId{}
	if change.Pre != nil {
		account := change.Pre.Data.MustAccount()
		preSigners = account.SponsorPerSigner()
	}

	account := change.Post.Data.MustAccount()
	postSigners := account.SponsorPerSigner()

	pre, preFound := preSigners[signerKey]
	post, postFound := postSigners[signerKey]

	if !postFound {
		return nil
	}

	if preFound {
		formerSponsor := pre.Address()
		newSponsor := post.Address()
		if formerSponsor == newSponsor {
			return nil
		}
	}

	return &post
}

func (operation *transactionOperationWrapper) getSponsor() (*xdr.AccountId, error) {
	changes, err := operation.transaction.GetOperationChanges(operation.index)
	if err != nil {
		return nil, err
	}
	var signerKey string
	if setOps, ok := operation.operation.Body.GetSetOptionsOp(); ok && setOps.Signer != nil {
		signerKey = setOps.Signer.Key.Address()
	}

	for _, c := range changes {
		// Check Signer changes
		if signerKey != "" {
			if sponsorAccount := operation.getSignerSponsorInChange(signerKey, c); sponsorAccount != nil {
				return sponsorAccount, nil
			}
		}

		// Check Ledger key changes
		if c.Pre != nil || c.Post == nil {
			// We are only looking for entry creations denoting that a sponsor
			// is associated to the ledger entry of the operation.
			continue
		}
		if sponsorAccount := c.Post.SponsoringID(); sponsorAccount != nil {
			return sponsorAccount, nil
		}
	}

	return nil, nil
}

var errLiquidityPoolChangeNotFound = errors.New("liquidity pool change not found")

func (operation *transactionOperationWrapper) getLiquidityPoolAndProductDelta(lpID *xdr.PoolId) (*xdr.LiquidityPoolEntry, *liquidityPoolDelta, error) {
	changes, err := operation.transaction.GetOperationChanges(operation.index)
	if err != nil {
		return nil, nil, err
	}

	for _, c := range changes {
		if c.Type != xdr.LedgerEntryTypeLiquidityPool {
			continue
		}
		// The delta can be caused by a full removal or full creation of the liquidity pool
		var lp *xdr.LiquidityPoolEntry
		var preA, preB, preShares xdr.Int64
		if c.Pre != nil {
			if lpID != nil && c.Pre.Data.LiquidityPool.LiquidityPoolId != *lpID {
				// if we were looking for specific pool id, then check on it
				continue
			}
			lp = c.Pre.Data.LiquidityPool
			if c.Pre.Data.LiquidityPool.Body.Type != xdr.LiquidityPoolTypeLiquidityPoolConstantProduct {
				return nil, nil, fmt.Errorf("unexpected liquity pool body type %d", c.Pre.Data.LiquidityPool.Body.Type)
			}
			cpPre := c.Pre.Data.LiquidityPool.Body.ConstantProduct
			preA, preB, preShares = cpPre.ReserveA, cpPre.ReserveB, cpPre.TotalPoolShares
		}
		var postA, postB, postShares xdr.Int64
		if c.Post != nil {
			if lpID != nil && c.Post.Data.LiquidityPool.LiquidityPoolId != *lpID {
				// if we were looking for specific pool id, then check on it
				continue
			}
			lp = c.Post.Data.LiquidityPool
			if c.Post.Data.LiquidityPool.Body.Type != xdr.LiquidityPoolTypeLiquidityPoolConstantProduct {
				return nil, nil, fmt.Errorf("unexpected liquity pool body type %d", c.Post.Data.LiquidityPool.Body.Type)
			}
			cpPost := c.Post.Data.LiquidityPool.Body.ConstantProduct
			postA, postB, postShares = cpPost.ReserveA, cpPost.ReserveB, cpPost.TotalPoolShares
		}
		delta := &liquidityPoolDelta{
			ReserveA:        postA - preA,
			ReserveB:        postB - preB,
			TotalPoolShares: postShares - preShares,
		}
		return lp, delta, nil
	}

	return nil, nil, errLiquidityPoolChangeNotFound
}

// OperationResult returns the operation's result record
func (operation *transactionOperationWrapper) OperationResult() *xdr.OperationResultTr {
	results, _ := operation.transaction.Result.OperationResults()
	tr := results[operation.index].MustTr()
	return &tr
}

func (operation *transactionOperationWrapper) findInitatingBeginSponsoringOp() *transactionOperationWrapper {
	if !operation.transaction.Result.Successful() {
		// Failed transactions may not have a compliant sandwich structure
		// we can rely on (e.g. invalid nesting or a being operation with the wrong sponsoree ID)
		// and thus we bail out since we could return incorrect information.
		return nil
	}
	sponsoree := operation.SourceAccount().ToAccountId()
	operations := operation.transaction.Envelope.Operations()
	for i := int(operation.index) - 1; i >= 0; i-- {
		if beginOp, ok := operations[i].Body.GetBeginSponsoringFutureReservesOp(); ok &&
			beginOp.SponsoredId.Address() == sponsoree.Address() {
			result := *operation
			result.index = uint32(i)
			result.operation = operations[i]
			return &result
		}
	}
	return nil
}

// Details returns the operation details as a map which can be stored as JSON.
func (operation *transactionOperationWrapper) Details() (map[string]interface{}, error) {
	details := map[string]interface{}{}
	source := operation.SourceAccount()
	switch operation.OperationType() {
	case xdr.OperationTypeCreateAccount:
		op := operation.operation.Body.MustCreateAccountOp()
		addAccountAndMuxedAccountDetails(details, *source, "funder")
		details["account"] = op.Destination.Address()
		details["starting_balance"] = amount.String(op.StartingBalance)
	case xdr.OperationTypePayment:
		op := operation.operation.Body.MustPaymentOp()
		addAccountAndMuxedAccountDetails(details, *source, "from")
		addAccountAndMuxedAccountDetails(details, op.Destination, "to")
		details["amount"] = amount.String(op.Amount)
		addAssetDetails(details, op.Asset, "")
	case xdr.OperationTypePathPaymentStrictReceive:
		op := operation.operation.Body.MustPathPaymentStrictReceiveOp()
		addAccountAndMuxedAccountDetails(details, *source, "from")
		addAccountAndMuxedAccountDetails(details, op.Destination, "to")

		details["amount"] = amount.String(op.DestAmount)
		details["source_amount"] = amount.String(0)
		details["source_max"] = amount.String(op.SendMax)
		addAssetDetails(details, op.DestAsset, "")
		addAssetDetails(details, op.SendAsset, "source_")

		if operation.transaction.Result.Successful() {
			result := operation.OperationResult().MustPathPaymentStrictReceiveResult()
			details["source_amount"] = amount.String(result.SendAmount())
		}

		var path = make([]map[string]interface{}, len(op.Path))
		for i := range op.Path {
			path[i] = make(map[string]interface{})
			addAssetDetails(path[i], op.Path[i], "")
		}
		details["path"] = path

	case xdr.OperationTypePathPaymentStrictSend:
		op := operation.operation.Body.MustPathPaymentStrictSendOp()
		addAccountAndMuxedAccountDetails(details, *source, "from")
		addAccountAndMuxedAccountDetails(details, op.Destination, "to")

		details["amount"] = amount.String(0)
		details["source_amount"] = amount.String(op.SendAmount)
		details["destination_min"] = amount.String(op.DestMin)
		addAssetDetails(details, op.DestAsset, "")
		addAssetDetails(details, op.SendAsset, "source_")

		if operation.transaction.Result.Successful() {
			result := operation.OperationResult().MustPathPaymentStrictSendResult()
			details["amount"] = amount.String(result.DestAmount())
		}

		var path = make([]map[string]interface{}, len(op.Path))
		for i := range op.Path {
			path[i] = make(map[string]interface{})
			addAssetDetails(path[i], op.Path[i], "")
		}
		details["path"] = path
	case xdr.OperationTypeManageBuyOffer:
		op := operation.operation.Body.MustManageBuyOfferOp()
		details["offer_id"] = op.OfferId
		details["amount"] = amount.String(op.BuyAmount)
		details["price"] = op.Price.String()
		details["price_r"] = map[string]interface{}{
			"n": op.Price.N,
			"d": op.Price.D,
		}
		addAssetDetails(details, op.Buying, "buying_")
		addAssetDetails(details, op.Selling, "selling_")
	case xdr.OperationTypeManageSellOffer:
		op := operation.operation.Body.MustManageSellOfferOp()
		details["offer_id"] = op.OfferId
		details["amount"] = amount.String(op.Amount)
		details["price"] = op.Price.String()
		details["price_r"] = map[string]interface{}{
			"n": op.Price.N,
			"d": op.Price.D,
		}
		addAssetDetails(details, op.Buying, "buying_")
		addAssetDetails(details, op.Selling, "selling_")
	case xdr.OperationTypeCreatePassiveSellOffer:
		op := operation.operation.Body.MustCreatePassiveSellOfferOp()
		details["amount"] = amount.String(op.Amount)
		details["price"] = op.Price.String()
		details["price_r"] = map[string]interface{}{
			"n": op.Price.N,
			"d": op.Price.D,
		}
		addAssetDetails(details, op.Buying, "buying_")
		addAssetDetails(details, op.Selling, "selling_")
	case xdr.OperationTypeSetOptions:
		op := operation.operation.Body.MustSetOptionsOp()

		if op.InflationDest != nil {
			details["inflation_dest"] = op.InflationDest.Address()
		}

		if op.SetFlags != nil && *op.SetFlags > 0 {
			addAuthFlagDetails(details, xdr.AccountFlags(*op.SetFlags), "set")
		}

		if op.ClearFlags != nil && *op.ClearFlags > 0 {
			addAuthFlagDetails(details, xdr.AccountFlags(*op.ClearFlags), "clear")
		}

		if op.MasterWeight != nil {
			details["master_key_weight"] = *op.MasterWeight
		}

		if op.LowThreshold != nil {
			details["low_threshold"] = *op.LowThreshold
		}

		if op.MedThreshold != nil {
			details["med_threshold"] = *op.MedThreshold
		}

		if op.HighThreshold != nil {
			details["high_threshold"] = *op.HighThreshold
		}

		if op.HomeDomain != nil {
			details["home_domain"] = *op.HomeDomain
		}

		if op.Signer != nil {
			details["signer_key"] = op.Signer.Key.Address()
			details["signer_weight"] = op.Signer.Weight
		}
	case xdr.OperationTypeChangeTrust:
		op := operation.operation.Body.MustChangeTrustOp()
		if op.Line.Type == xdr.AssetTypeAssetTypePoolShare {
			if err := addLiquidityPoolAssetDetails(details, *op.Line.LiquidityPool); err != nil {
				return nil, err
			}
		} else {
			addAssetDetails(details, op.Line.ToAsset(), "")
			details["trustee"] = details["asset_issuer"]
		}
		addAccountAndMuxedAccountDetails(details, *source, "trustor")
		details["limit"] = amount.String(op.Limit)
	case xdr.OperationTypeAllowTrust:
		op := operation.operation.Body.MustAllowTrustOp()
		addAssetDetails(details, op.Asset.ToAsset(source.ToAccountId()), "")
		addAccountAndMuxedAccountDetails(details, *source, "trustee")
		details["trustor"] = op.Trustor.Address()
		details["authorize"] = xdr.TrustLineFlags(op.Authorize).IsAuthorized()
		authLiabilities := xdr.TrustLineFlags(op.Authorize).IsAuthorizedToMaintainLiabilitiesFlag()
		if authLiabilities {
			details["authorize_to_maintain_liabilities"] = authLiabilities
		}
		clawbackEnabled := xdr.TrustLineFlags(op.Authorize).IsClawbackEnabledFlag()
		if clawbackEnabled {
			details["clawback_enabled"] = clawbackEnabled
		}
	case xdr.OperationTypeAccountMerge:
		addAccountAndMuxedAccountDetails(details, *source, "account")
		addAccountAndMuxedAccountDetails(details, operation.operation.Body.MustDestination(), "into")
	case xdr.OperationTypeInflation:
		// no inflation details, presently
	case xdr.OperationTypeManageData:
		op := operation.operation.Body.MustManageDataOp()
		details["name"] = string(op.DataName)
		if op.DataValue != nil {
			details["value"] = base64.StdEncoding.EncodeToString(*op.DataValue)
		} else {
			details["value"] = nil
		}
	case xdr.OperationTypeBumpSequence:
		op := operation.operation.Body.MustBumpSequenceOp()
		details["bump_to"] = fmt.Sprintf("%d", op.BumpTo)
	case xdr.OperationTypeCreateClaimableBalance:
		op := operation.operation.Body.MustCreateClaimableBalanceOp()
		details["asset"] = op.Asset.StringCanonical()
		details["amount"] = amount.String(op.Amount)
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
		op := operation.operation.Body.MustClaimClaimableBalanceOp()
		balanceID, err := xdr.MarshalHex(op.BalanceId)
		if err != nil {
			panic(fmt.Errorf("Invalid balanceId in op: %d", operation.index))
		}
		details["balance_id"] = balanceID
		addAccountAndMuxedAccountDetails(details, *source, "claimant")
	case xdr.OperationTypeBeginSponsoringFutureReserves:
		op := operation.operation.Body.MustBeginSponsoringFutureReservesOp()
		details["sponsored_id"] = op.SponsoredId.Address()
	case xdr.OperationTypeEndSponsoringFutureReserves:
		beginSponsorshipOp := operation.findInitatingBeginSponsoringOp()
		if beginSponsorshipOp != nil {
			beginSponsorshipSource := beginSponsorshipOp.SourceAccount()
			addAccountAndMuxedAccountDetails(details, *beginSponsorshipSource, "begin_sponsor")
		}
	case xdr.OperationTypeRevokeSponsorship:
		op := operation.operation.Body.MustRevokeSponsorshipOp()
		switch op.Type {
		case xdr.RevokeSponsorshipTypeRevokeSponsorshipLedgerEntry:
			if err := addLedgerKeyDetails(details, *op.LedgerKey); err != nil {
				return nil, err
			}
		case xdr.RevokeSponsorshipTypeRevokeSponsorshipSigner:
			details["signer_account_id"] = op.Signer.AccountId.Address()
			details["signer_key"] = op.Signer.SignerKey.Address()
		}
	case xdr.OperationTypeClawback:
		op := operation.operation.Body.MustClawbackOp()
		addAssetDetails(details, op.Asset, "")
		addAccountAndMuxedAccountDetails(details, op.From, "from")
		details["amount"] = amount.String(op.Amount)
	case xdr.OperationTypeClawbackClaimableBalance:
		op := operation.operation.Body.MustClawbackClaimableBalanceOp()
		balanceID, err := xdr.MarshalHex(op.BalanceId)
		if err != nil {
			panic(fmt.Errorf("Invalid balanceId in op: %d", operation.index))
		}
		details["balance_id"] = balanceID
	case xdr.OperationTypeSetTrustLineFlags:
		op := operation.operation.Body.MustSetTrustLineFlagsOp()
		details["trustor"] = op.Trustor.Address()
		addAssetDetails(details, op.Asset, "")
		if op.SetFlags > 0 {
			addTrustLineFlagDetails(details, xdr.TrustLineFlags(op.SetFlags), "set")
		}

		if op.ClearFlags > 0 {
			addTrustLineFlagDetails(details, xdr.TrustLineFlags(op.ClearFlags), "clear")
		}
	case xdr.OperationTypeLiquidityPoolDeposit:
		op := operation.operation.Body.MustLiquidityPoolDepositOp()
		details["liquidity_pool_id"] = PoolIDToString(op.LiquidityPoolId)
		var (
			assetA, assetB         string
			depositedA, depositedB xdr.Int64
			sharesReceived         xdr.Int64
		)
		if operation.transaction.Result.Successful() {
			// we will use the defaults (omitted asset and 0 amounts) if the transaction failed
			lp, delta, err := operation.getLiquidityPoolAndProductDelta(&op.LiquidityPoolId)
			if err != nil {
				return nil, err
			}
			params := lp.Body.ConstantProduct.Params
			assetA, assetB = params.AssetA.StringCanonical(), params.AssetB.StringCanonical()
			depositedA, depositedB = delta.ReserveA, delta.ReserveB
			sharesReceived = delta.TotalPoolShares
		}
		details["reserves_max"] = []base.AssetAmount{
			{Asset: assetA, Amount: amount.String(op.MaxAmountA)},
			{Asset: assetB, Amount: amount.String(op.MaxAmountB)},
		}
		details["min_price"] = op.MinPrice.String()
		details["min_price_r"] = map[string]interface{}{
			"n": op.MinPrice.N,
			"d": op.MinPrice.D,
		}
		details["max_price"] = op.MaxPrice.String()
		details["max_price_r"] = map[string]interface{}{
			"n": op.MaxPrice.N,
			"d": op.MaxPrice.D,
		}
		details["reserves_deposited"] = []base.AssetAmount{
			{Asset: assetA, Amount: amount.String(depositedA)},
			{Asset: assetB, Amount: amount.String(depositedB)},
		}
		details["shares_received"] = amount.String(sharesReceived)
	case xdr.OperationTypeLiquidityPoolWithdraw:
		op := operation.operation.Body.MustLiquidityPoolWithdrawOp()
		details["liquidity_pool_id"] = PoolIDToString(op.LiquidityPoolId)
		var (
			assetA, assetB       string
			receivedA, receivedB xdr.Int64
		)
		if operation.transaction.Result.Successful() {
			// we will use the defaults (omitted asset and 0 amounts) if the transaction failed
			lp, delta, err := operation.getLiquidityPoolAndProductDelta(&op.LiquidityPoolId)
			if err != nil {
				return nil, err
			}
			params := lp.Body.ConstantProduct.Params
			assetA, assetB = params.AssetA.StringCanonical(), params.AssetB.StringCanonical()
			receivedA, receivedB = -delta.ReserveA, -delta.ReserveB
		}
		details["reserves_min"] = []base.AssetAmount{
			{Asset: assetA, Amount: amount.String(op.MinAmountA)},
			{Asset: assetB, Amount: amount.String(op.MinAmountB)},
		}
		details["shares"] = amount.String(op.Amount)
		details["reserves_received"] = []base.AssetAmount{
			{Asset: assetA, Amount: amount.String(receivedA)},
			{Asset: assetB, Amount: amount.String(receivedB)},
		}
	case xdr.OperationTypeInvokeHostFunction:
		op := operation.operation.Body.MustInvokeHostFunctionOp()
		details["function"] = op.HostFunction.Type.String()

		switch op.HostFunction.Type {
		case xdr.HostFunctionTypeHostFunctionTypeInvokeContract:
			args := op.HostFunction.MustInvokeContract()
			details["type"] = "invoke_contract"
			params := make([]map[string]string, 0, len(args))

			transactionEnvelope := operation.transaction.Envelope.MustV1()
			details["contract_id"] = contractIdFromTxEnvelope(transactionEnvelope)
			details["contract_code_hash"] = contractCodeHashFromTxEnvelope(transactionEnvelope)

			for _, param := range args {
				serializedParam := map[string]string{}
				serializedParam["value"] = "n/a"
				serializedParam["type"] = "n/a"

				if scValTypeName, ok := param.ArmForSwitch(int32(param.Type)); ok {
					serializedParam["type"] = scValTypeName
					if raw, err := param.MarshalBinary(); err == nil {
						serializedParam["value"] = base64.StdEncoding.EncodeToString(raw)
					}
				}
				params = append(params, serializedParam)
			}
			details["parameters"] = params

			if balanceChanges, err := operation.parseAssetBalanceChangesFromContractEvents(); err != nil {
				return nil, err
			} else {
				details["asset_balance_changes"] = balanceChanges
			}

		case xdr.HostFunctionTypeHostFunctionTypeCreateContract:
			args := op.HostFunction.MustCreateContract()
			details["type"] = "create_contract"

			transactionEnvelope := operation.transaction.Envelope.MustV1()
			details["contract_id"] = contractIdFromTxEnvelope(transactionEnvelope)
			details["contract_code_hash"] = contractCodeHashFromTxEnvelope(transactionEnvelope)

			switch args.ContractIdPreimage.Type {
			case xdr.ContractIdPreimageTypeContractIdPreimageFromAddress:
				fromAddress := args.ContractIdPreimage.MustFromAddress()
				address, err := fromAddress.Address.String()
				if err != nil {
					panic(fmt.Errorf("error obtaining address for: %s", args.ContractIdPreimage.Type))
				}
				details["from"] = "address"
				details["address"] = address
				details["salt"] = fromAddress.Salt
			case xdr.ContractIdPreimageTypeContractIdPreimageFromAsset:
				details["from"] = "asset"
				details["asset"] = args.ContractIdPreimage.MustFromAsset().StringCanonical()
			default:
				panic(fmt.Errorf("unknown contract id type: %s", args.ContractIdPreimage.Type))
			}
		case xdr.HostFunctionTypeHostFunctionTypeUploadContractWasm:
			details["type"] = "upload_wasm"
			transactionEnvelope := operation.transaction.Envelope.MustV1()
			details["contract_code_hash"] = contractCodeHashFromTxEnvelope(transactionEnvelope)
		default:
			panic(fmt.Errorf("unknown host function type: %s", op.HostFunction.Type))
		}
	case xdr.OperationTypeBumpFootprintExpiration:
		op := operation.operation.Body.MustBumpFootprintExpirationOp()
		details["type"] = "bump_footprint_expiration"
		details["ledgers_to_expire"] = op.LedgersToExpire

		transactionEnvelope := operation.transaction.Envelope.MustV1()
		details["contract_id"] = contractIdFromTxEnvelope(transactionEnvelope)
		details["contract_code_hash"] = contractCodeHashFromTxEnvelope(transactionEnvelope)
	case xdr.OperationTypeRestoreFootprint:
		details["type"] = "restore_footprint"

		transactionEnvelope := operation.transaction.Envelope.MustV1()
		details["contract_id"] = contractIdFromTxEnvelope(transactionEnvelope)
		details["contract_code_hash"] = contractCodeHashFromTxEnvelope(transactionEnvelope)
	default:
		panic(fmt.Errorf("Unknown operation type: %s", operation.OperationType()))
	}

	sponsor, err := operation.getSponsor()
	if err != nil {
		return nil, err
	}
	if sponsor != nil {
		details["sponsor"] = sponsor.Address()
	}

	return details, nil
}

func contractIdFromTxEnvelope(transactionEnvelope xdr.TransactionV1Envelope) string {
	for _, ledgerKey := range transactionEnvelope.Tx.Ext.SorobanData.Resources.Footprint.ReadWrite {
		contractId := contractIdFromContractData(ledgerKey)
		if contractId != "" {
			return contractId
		}
	}

	for _, ledgerKey := range transactionEnvelope.Tx.Ext.SorobanData.Resources.Footprint.ReadOnly {
		contractId := contractIdFromContractData(ledgerKey)
		if contractId != "" {
			return contractId
		}
	}

	return ""
}

func contractIdFromContractData(ledgerKey xdr.LedgerKey) string {
	contractData, ok := ledgerKey.GetContractData()
	if !ok {
		return ""
	}
	contractIdHash, ok := contractData.Contract.GetContractId()
	if ok {
		return contractIdHash.HexString()
	}

	return ""
}

func contractCodeHashFromTxEnvelope(transactionEnvelope xdr.TransactionV1Envelope) string {
	for _, ledgerKey := range transactionEnvelope.Tx.Ext.SorobanData.Resources.Footprint.ReadOnly {
		contractCode := contractCodeFromContractData(ledgerKey)
		if contractCode != "" {
			return contractCode
		}
	}

	for _, ledgerKey := range transactionEnvelope.Tx.Ext.SorobanData.Resources.Footprint.ReadWrite {
		contractCode := contractCodeFromContractData(ledgerKey)
		if contractCode != "" {
			return contractCode
		}
	}

	return ""
}

func contractCodeFromContractData(ledgerKey xdr.LedgerKey) string {
	contractCode, ok := ledgerKey.GetContractCode()
	if !ok {
		return ""
	}
	contractCodeHash := contractCode.Hash
	return contractCodeHash.HexString()
}

func filterEvents(diagnosticEvents []xdr.DiagnosticEvent) []xdr.ContractEvent {
	var filtered []xdr.ContractEvent
	for _, diagnosticEvent := range diagnosticEvents {
		if !diagnosticEvent.InSuccessfulContractCall || diagnosticEvent.Event.Type != xdr.ContractEventTypeContract {
			continue
		}
		filtered = append(filtered, diagnosticEvent.Event)
	}
	return filtered
}

// Searches an operation for SAC events that are of a type which represent
// asset balances having changed.
//
// SAC events have a one-to-one association to SAC contract fn invocations.
// i.e. invoke the 'mint' function, will trigger one Mint Event to be emitted capturing the fn args.
//
// SAC events that involve asset balance changes follow some standard data formats.
// The 'amount' in the event is expressed as Int128Parts, which carries a sign, however it's expected
// that value will not be signed as it represents a absolute delta, the event type can provide the
// context of whether an amount was considered incremental or decremental, i.e. credit or debit to a balance.
func (operation *transactionOperationWrapper) parseAssetBalanceChangesFromContractEvents() ([]map[string]interface{}, error) {
	balanceChanges := []map[string]interface{}{}

	diagnosticEvents, err := operation.transaction.GetDiagnosticEvents()
	if err != nil {
		// this operation in this context must be an InvokeHostFunctionOp, therefore V3Meta should be present
		// as it's in same soroban model, so if any err, it's real,
		return nil, err
	}

	for _, contractEvent := range filterEvents(diagnosticEvents) {
		// Parse the xdr contract event to contractevents.StellarAssetContractEvent model

		// has some convenience like to/from attributes are expressed in strkey format for accounts(G...) and contracts(C...)
		if sacEvent, err := contractevents.NewStellarAssetContractEvent(&contractEvent, operation.network); err == nil {
			switch sacEvent.GetType() {
			case contractevents.EventTypeTransfer:
				transferEvt := sacEvent.(*contractevents.TransferEvent)
				balanceChanges = append(balanceChanges, createSACBalanceChangeEntry(transferEvt.From, transferEvt.To, transferEvt.Amount, transferEvt.Asset, "transfer"))
			case contractevents.EventTypeMint:
				mintEvt := sacEvent.(*contractevents.MintEvent)
				balanceChanges = append(balanceChanges, createSACBalanceChangeEntry("", mintEvt.To, mintEvt.Amount, mintEvt.Asset, "mint"))
			case contractevents.EventTypeClawback:
				clawbackEvt := sacEvent.(*contractevents.ClawbackEvent)
				balanceChanges = append(balanceChanges, createSACBalanceChangeEntry(clawbackEvt.From, "", clawbackEvt.Amount, clawbackEvt.Asset, "clawback"))
			case contractevents.EventTypeBurn:
				burnEvt := sacEvent.(*contractevents.BurnEvent)
				balanceChanges = append(balanceChanges, createSACBalanceChangeEntry(burnEvt.From, "", burnEvt.Amount, burnEvt.Asset, "burn"))
			}
		}
	}

	return balanceChanges, nil
}

func parseAssetBalanceChangesFromContractEvents(transaction ingest.LedgerTransaction) ([]map[string]interface{}, error) {
	balanceChanges := []map[string]interface{}{}

	diagnosticEvents, err := transaction.GetDiagnosticEvents()
	if err != nil {
		// this operation in this context must be an InvokeHostFunctionOp, therefore V3Meta should be present
		// as it's in same soroban model, so if any err, it's real,
		return nil, err
	}

	for _, contractEvent := range filterEvents(diagnosticEvents) {
		// Parse the xdr contract event to contractevents.StellarAssetContractEvent model

		// has some convenience like to/from attributes are expressed in strkey format for accounts(G...) and contracts(C...)
		if sacEvent, err := contractevents.NewStellarAssetContractEvent(&contractEvent, "Test SDF Future Network ; October 2022"); err == nil {
			switch sacEvent.GetType() {
			case contractevents.EventTypeTransfer:
				transferEvt := sacEvent.(*contractevents.TransferEvent)
				balanceChanges = append(balanceChanges, createSACBalanceChangeEntry(transferEvt.From, transferEvt.To, transferEvt.Amount, transferEvt.Asset, "transfer"))
			case contractevents.EventTypeMint:
				mintEvt := sacEvent.(*contractevents.MintEvent)
				balanceChanges = append(balanceChanges, createSACBalanceChangeEntry("", mintEvt.To, mintEvt.Amount, mintEvt.Asset, "mint"))
			case contractevents.EventTypeClawback:
				clawbackEvt := sacEvent.(*contractevents.ClawbackEvent)
				balanceChanges = append(balanceChanges, createSACBalanceChangeEntry(clawbackEvt.From, "", clawbackEvt.Amount, clawbackEvt.Asset, "clawback"))
			case contractevents.EventTypeBurn:
				burnEvt := sacEvent.(*contractevents.BurnEvent)
				balanceChanges = append(balanceChanges, createSACBalanceChangeEntry(burnEvt.From, "", burnEvt.Amount, burnEvt.Asset, "burn"))
			}
		}
	}

	return balanceChanges, nil
}

// fromAccount   - strkey format of contract or address
// toAccount     - strkey format of contract or address, or nillable
// amountChanged - absolute value that asset balance changed
// asset         - the fully qualified issuer:code for asset that had balance change
// changeType    - the type of source sac event that triggered this change
//
// return        - a balance changed record expressed as map of key/value's
func createSACBalanceChangeEntry(fromAccount string, toAccount string, amountChanged xdr.Int128Parts, asset xdr.Asset, changeType string) map[string]interface{} {
	balanceChange := map[string]interface{}{}

	if fromAccount != "" {
		balanceChange["from"] = fromAccount
	}
	if toAccount != "" {
		balanceChange["to"] = toAccount
	}

	balanceChange["type"] = changeType
	balanceChange["amount"] = amount.String128(amountChanged)
	addAssetDetails(balanceChange, asset, "")
	return balanceChange
}

// addAssetDetails sets the details for `a` on `result` using keys with `prefix`
func addAssetDetails(result map[string]interface{}, a xdr.Asset, prefix string) error {
	var (
		assetType string
		code      string
		issuer    string
	)
	err := a.Extract(&assetType, &code, &issuer)
	if err != nil {
		err = errors.Wrap(err, "xdr.Asset.Extract error")
		return err
	}
	result[prefix+"asset_type"] = assetType

	if a.Type == xdr.AssetTypeAssetTypeNative {
		return nil
	}

	result[prefix+"asset_code"] = code
	result[prefix+"asset_issuer"] = issuer
	return nil
}

// addAuthFlagDetails adds the account flag details for `f` on `result`.
func addAuthFlagDetails(result map[string]interface{}, f xdr.AccountFlags, prefix string) {
	var (
		n []int32
		s []string
	)

	if f.IsAuthRequired() {
		n = append(n, int32(xdr.AccountFlagsAuthRequiredFlag))
		s = append(s, "auth_required")
	}

	if f.IsAuthRevocable() {
		n = append(n, int32(xdr.AccountFlagsAuthRevocableFlag))
		s = append(s, "auth_revocable")
	}

	if f.IsAuthImmutable() {
		n = append(n, int32(xdr.AccountFlagsAuthImmutableFlag))
		s = append(s, "auth_immutable")
	}

	if f.IsAuthClawbackEnabled() {
		n = append(n, int32(xdr.AccountFlagsAuthClawbackEnabledFlag))
		s = append(s, "auth_clawback_enabled")
	}

	result[prefix+"_flags"] = n
	result[prefix+"_flags_s"] = s
}

// addTrustLineFlagDetails adds the trustline flag details for `f` on `result`.
func addTrustLineFlagDetails(result map[string]interface{}, f xdr.TrustLineFlags, prefix string) {
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
		s = append(s, "authorized_to_maintain_liabilites")
	}

	if f.IsClawbackEnabledFlag() {
		n = append(n, int32(xdr.TrustLineFlagsTrustlineClawbackEnabledFlag))
		s = append(s, "clawback_enabled")
	}

	result[prefix+"_flags"] = n
	result[prefix+"_flags_s"] = s
}

func addLedgerKeyDetails(result map[string]interface{}, ledgerKey xdr.LedgerKey) error {
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
		result["data_name"] = ledgerKey.Data.DataName
	case xdr.LedgerEntryTypeOffer:
		result["offer_id"] = fmt.Sprintf("%d", ledgerKey.Offer.OfferId)
	case xdr.LedgerEntryTypeTrustline:
		result["trustline_account_id"] = ledgerKey.TrustLine.AccountId.Address()
		if ledgerKey.TrustLine.Asset.Type == xdr.AssetTypeAssetTypePoolShare {
			result["trustline_liquidity_pool_id"] = PoolIDToString(*ledgerKey.TrustLine.Asset.LiquidityPoolId)
		} else {
			result["trustline_asset"] = ledgerKey.TrustLine.Asset.ToAsset().StringCanonical()
		}
	case xdr.LedgerEntryTypeLiquidityPool:
		result["liquidity_pool_id"] = PoolIDToString(ledgerKey.LiquidityPool.LiquidityPoolId)
	}
	return nil
}

func getLedgerKeyParticipants(ledgerKey xdr.LedgerKey) []xdr.AccountId {
	var result []xdr.AccountId
	switch ledgerKey.Type {
	case xdr.LedgerEntryTypeAccount:
		result = append(result, ledgerKey.Account.AccountId)
	case xdr.LedgerEntryTypeClaimableBalance:
		// nothing to do
	case xdr.LedgerEntryTypeData:
		result = append(result, ledgerKey.Data.AccountId)
	case xdr.LedgerEntryTypeOffer:
		result = append(result, ledgerKey.Offer.SellerId)
	case xdr.LedgerEntryTypeTrustline:
		result = append(result, ledgerKey.TrustLine.AccountId)
	}
	return result
}

// Participants returns the accounts taking part in the operation.
func (operation *transactionOperationWrapper) Participants() ([]xdr.AccountId, error) {
	participants := []xdr.AccountId{}
	participants = append(participants, operation.SourceAccount().ToAccountId())
	op := operation.operation

	switch operation.OperationType() {
	case xdr.OperationTypeCreateAccount:
		participants = append(participants, op.Body.MustCreateAccountOp().Destination)
	case xdr.OperationTypePayment:
		participants = append(participants, op.Body.MustPaymentOp().Destination.ToAccountId())
	case xdr.OperationTypePathPaymentStrictReceive:
		participants = append(participants, op.Body.MustPathPaymentStrictReceiveOp().Destination.ToAccountId())
	case xdr.OperationTypePathPaymentStrictSend:
		participants = append(participants, op.Body.MustPathPaymentStrictSendOp().Destination.ToAccountId())
	case xdr.OperationTypeManageBuyOffer:
		// the only direct participant is the source_account
	case xdr.OperationTypeManageSellOffer:
		// the only direct participant is the source_account
	case xdr.OperationTypeCreatePassiveSellOffer:
		// the only direct participant is the source_account
	case xdr.OperationTypeSetOptions:
		// the only direct participant is the source_account
	case xdr.OperationTypeChangeTrust:
		// the only direct participant is the source_account
	case xdr.OperationTypeAllowTrust:
		participants = append(participants, op.Body.MustAllowTrustOp().Trustor)
	case xdr.OperationTypeAccountMerge:
		participants = append(participants, op.Body.MustDestination().ToAccountId())
	case xdr.OperationTypeInflation:
		// the only direct participant is the source_account
	case xdr.OperationTypeManageData:
		// the only direct participant is the source_account
	case xdr.OperationTypeBumpSequence:
		// the only direct participant is the source_account
	case xdr.OperationTypeCreateClaimableBalance:
		for _, c := range op.Body.MustCreateClaimableBalanceOp().Claimants {
			participants = append(participants, c.MustV0().Destination)
		}
	case xdr.OperationTypeClaimClaimableBalance:
		// the only direct participant is the source_account
	case xdr.OperationTypeBeginSponsoringFutureReserves:
		participants = append(participants, op.Body.MustBeginSponsoringFutureReservesOp().SponsoredId)
	case xdr.OperationTypeEndSponsoringFutureReserves:
		beginSponsorshipOp := operation.findInitatingBeginSponsoringOp()
		if beginSponsorshipOp != nil {
			participants = append(participants, beginSponsorshipOp.SourceAccount().ToAccountId())
		}
	case xdr.OperationTypeRevokeSponsorship:
		op := operation.operation.Body.MustRevokeSponsorshipOp()
		switch op.Type {
		case xdr.RevokeSponsorshipTypeRevokeSponsorshipLedgerEntry:
			participants = append(participants, getLedgerKeyParticipants(*op.LedgerKey)...)
		case xdr.RevokeSponsorshipTypeRevokeSponsorshipSigner:
			participants = append(participants, op.Signer.AccountId)
			// We don't add signer as a participant because a signer can be arbitrary account.
			// This can spam successful operations history of any account.
		}
	case xdr.OperationTypeClawback:
		op := operation.operation.Body.MustClawbackOp()
		participants = append(participants, op.From.ToAccountId())
	case xdr.OperationTypeClawbackClaimableBalance:
		// the only direct participant is the source_account
	case xdr.OperationTypeSetTrustLineFlags:
		op := operation.operation.Body.MustSetTrustLineFlagsOp()
		participants = append(participants, op.Trustor)
	case xdr.OperationTypeLiquidityPoolDeposit:
		// the only direct participant is the source_account
	case xdr.OperationTypeLiquidityPoolWithdraw:
		// the only direct participant is the source_account
	case xdr.OperationTypeInvokeHostFunction:
		// the only direct participant is the source_account
	case xdr.OperationTypeBumpFootprintExpiration:
		// the only direct participant is the source_account
	case xdr.OperationTypeRestoreFootprint:
		// the only direct participant is the source_account
	default:
		return participants, fmt.Errorf("Unknown operation type: %s", op.Body.Type)
	}

	sponsor, err := operation.getSponsor()
	if err != nil {
		return nil, err
	}
	if sponsor != nil {
		participants = append(participants, *sponsor)
	}

	return dedupeParticipants(participants), nil
}

// dedupeParticipants remove any duplicate ids from `in`
func dedupeParticipants(in []xdr.AccountId) (out []xdr.AccountId) {
	set := map[string]xdr.AccountId{}
	for _, id := range in {
		set[id.Address()] = id
	}

	for _, id := range set {
		out = append(out, id)
	}
	return
}

// OperationsParticipants returns a map with all participants per operation
func operationsParticipants(transaction ingest.LedgerTransaction, sequence uint32) (map[int64][]xdr.AccountId, error) {
	participants := map[int64][]xdr.AccountId{}

	for opi, op := range transaction.Envelope.Operations() {
		operation := transactionOperationWrapper{
			index:          uint32(opi),
			transaction:    transaction,
			operation:      op,
			ledgerSequence: sequence,
		}

		p, err := operation.Participants()
		if err != nil {
			return participants, errors.Wrapf(err, "reading operation %v participants", operation.ID())
		}
		participants[operation.ID()] = p
	}

	return participants, nil
}
