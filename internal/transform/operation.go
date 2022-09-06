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
		return nil
	}

	result[prefix+"asset_code"] = code
	result[prefix+"asset_issuer"] = issuer

	return nil
}

func addLiquidityPoolAssetDetails(result map[string]interface{}, lpp xdr.LiquidityPoolParameters) error {
	result["asset_type"] = "liquidity_pool_shares"
	if lpp.Type != xdr.LiquidityPoolTypeLiquidityPoolConstantProduct {
		return fmt.Errorf("unkown liquidity pool type %d", lpp.Type)
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
