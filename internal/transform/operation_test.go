package transform

import (
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

func TestTransformOperation(t *testing.T) {
	type operationInput struct {
		operation        xdr.Operation
		index            int32
		transaction      ingest.LedgerTransaction
		ledgerClosedMeta xdr.LedgerCloseMeta
	}
	type transformTest struct {
		input      operationInput
		wantOutput OperationOutput
		wantErr    error
	}
	genericInput := operationInput{
		operation:   genericBumpOperation,
		index:       1,
		transaction: genericLedgerTransaction,
	}

	negativeOpTypeInput := genericInput
	negativeOpTypeEnvelope := genericBumpOperationEnvelope
	negativeOpTypeEnvelope.Tx.Operations[0].Body.Type = xdr.OperationType(-1)
	negativeOpTypeInput.operation.Body.Type = xdr.OperationType(-1)
	negativeOpTypeInput.transaction.Envelope.V1 = &negativeOpTypeEnvelope

	unknownOpTypeInput := genericInput
	unknownOpTypeEnvelope := genericBumpOperationEnvelope
	unknownOpTypeEnvelope.Tx.Operations[0].Body.Type = xdr.OperationType(99)
	unknownOpTypeInput.operation.Body.Type = xdr.OperationType(99)
	unknownOpTypeInput.transaction.Envelope.V1 = &unknownOpTypeEnvelope

	tests := []transformTest{
		{
			negativeOpTypeInput,
			OperationOutput{},
			fmt.Errorf("The operation type (-1) is negative for  operation 1 (operation id=4098)"),
		},
		{
			unknownOpTypeInput,
			OperationOutput{},
			fmt.Errorf("Unknown operation type: "),
		},
	}
	hardCodedInputTransaction, err := makeOperationTestInput()
	assert.NoError(t, err)
	hardCodedOutputArray := makeOperationTestOutputs()
	hardCodedInputLedgerCloseMeta := makeLedgerCloseMeta()

	for i, op := range hardCodedInputTransaction.Envelope.Operations() {
		tests = append(tests, transformTest{
			input:      operationInput{op, int32(i), hardCodedInputTransaction, hardCodedInputLedgerCloseMeta},
			wantOutput: hardCodedOutputArray[i],
			wantErr:    nil,
		})
	}

	for _, test := range tests {
		actualOutput, actualError := TransformOperation(test.input.operation, test.input.index, test.input.transaction, 0, test.input.ledgerClosedMeta)
		assert.Equal(t, test.wantErr, actualError)
		assert.Equal(t, test.wantOutput, actualOutput)
	}
}

func makeLedgerCloseMeta() (ledgerCloseMeta xdr.LedgerCloseMeta) {
	return xdr.LedgerCloseMeta{
		V: 1,
		V0: &xdr.LedgerCloseMetaV0{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					ScpValue: xdr.StellarValue{
						CloseTime: 0,
					},
				},
			},
		},
	}
}

// Creates a single transaction that contains one of every operation type
func makeOperationTestInput() (inputTransaction ingest.LedgerTransaction, err error) {
	inputTransaction = genericLedgerTransaction
	inputEnvelope := genericBumpOperationEnvelope

	inputEnvelope.Tx.SourceAccount = testAccount3
	hardCodedInflationDest := testAccount4ID

	hardCodedTrustAsset, err := usdtAsset.ToAssetCode("USDT")
	if err != nil {
		return
	}

	hardCodedClearFlags := xdr.Uint32(3)
	hardCodedSetFlags := xdr.Uint32(4)
	hardCodedMasterWeight := xdr.Uint32(3)
	hardCodedLowThresh := xdr.Uint32(1)
	hardCodedMedThresh := xdr.Uint32(3)
	hardCodedHighThresh := xdr.Uint32(5)
	hardCodedHomeDomain := xdr.String32("2019=DRA;n-test")
	hardCodedSignerKey, err := xdr.NewSignerKey(xdr.SignerKeyTypeSignerKeyTypeEd25519, xdr.Uint256([32]byte{}))
	if err != nil {
		return
	}

	hardCodedSigner := xdr.Signer{
		Key:    hardCodedSignerKey,
		Weight: xdr.Uint32(1),
	}

	hardCodedClaimableBalance := genericClaimableBalance
	hardCodedClaimant := testClaimant
	hardCodedDataValue := xdr.DataValue([]byte{0x76, 0x61, 0x6c, 0x75, 0x65})
	hardCodedSequenceNumber := xdr.SequenceNumber(100)
	inputOperations := []xdr.Operation{
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeCreateAccount,
				CreateAccountOp: &xdr.CreateAccountOp{
					StartingBalance: 25000000,
					Destination:     testAccount4ID,
				},
			},
		},
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypePayment,
				PaymentOp: &xdr.PaymentOp{
					Destination: testAccount4,
					Asset:       usdtAsset,
					Amount:      350000000,
				},
			},
		},
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypePayment,
				PaymentOp: &xdr.PaymentOp{
					Destination: testAccount4,
					Asset:       nativeAsset,
					Amount:      350000000,
				},
			},
		},
		xdr.Operation{
			SourceAccount: &testAccount3,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypePathPaymentStrictReceive,
				PathPaymentStrictReceiveOp: &xdr.PathPaymentStrictReceiveOp{
					SendAsset:   nativeAsset,
					SendMax:     8951495900,
					Destination: testAccount4,
					DestAsset:   nativeAsset,
					DestAmount:  8951495900,
					Path:        []xdr.Asset{usdtAsset},
				},
			},
		},
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeManageSellOffer,
				ManageSellOfferOp: &xdr.ManageSellOfferOp{
					Selling: usdtAsset,
					Buying:  nativeAsset,
					Amount:  765860000,
					Price: xdr.Price{
						N: 128523,
						D: 250000,
					},
					OfferId: 0,
				},
			},
		},
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeCreatePassiveSellOffer,
				CreatePassiveSellOfferOp: &xdr.CreatePassiveSellOfferOp{
					Selling: nativeAsset,
					Buying:  usdtAsset,
					Amount:  631595000,
					Price: xdr.Price{
						N: 99583200,
						D: 1257990000,
					},
				},
			},
		},
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeSetOptions,
				SetOptionsOp: &xdr.SetOptionsOp{
					InflationDest: &hardCodedInflationDest,
					ClearFlags:    &hardCodedClearFlags,
					SetFlags:      &hardCodedSetFlags,
					MasterWeight:  &hardCodedMasterWeight,
					LowThreshold:  &hardCodedLowThresh,
					MedThreshold:  &hardCodedMedThresh,
					HighThreshold: &hardCodedHighThresh,
					HomeDomain:    &hardCodedHomeDomain,
					Signer:        &hardCodedSigner,
				},
			},
		},
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeChangeTrust,
				ChangeTrustOp: &xdr.ChangeTrustOp{
					Line:  usdtChangeTrustAsset,
					Limit: xdr.Int64(500000000000000000),
				},
			},
		},
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeChangeTrust,
				ChangeTrustOp: &xdr.ChangeTrustOp{
					Line:  usdtLiquidityPoolShare,
					Limit: xdr.Int64(500000000000000000),
				},
			},
		},
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeAllowTrust,
				AllowTrustOp: &xdr.AllowTrustOp{
					Trustor:   testAccount4ID,
					Asset:     hardCodedTrustAsset,
					Authorize: xdr.Uint32(1),
				},
			},
		},
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type:        xdr.OperationTypeAccountMerge,
				Destination: &testAccount4,
			},
		},
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeInflation,
			},
		},
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeManageData,
				ManageDataOp: &xdr.ManageDataOp{
					DataName:  "test",
					DataValue: &hardCodedDataValue,
				},
			},
		},
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeBumpSequence,
				BumpSequenceOp: &xdr.BumpSequenceOp{
					BumpTo: hardCodedSequenceNumber,
				},
			},
		},
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeManageBuyOffer,
				ManageBuyOfferOp: &xdr.ManageBuyOfferOp{
					Selling:   usdtAsset,
					Buying:    nativeAsset,
					BuyAmount: 7654501001,
					Price: xdr.Price{
						N: 635863285,
						D: 1818402817,
					},
					OfferId: 100,
				},
			},
		},
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypePathPaymentStrictSend,
				PathPaymentStrictSendOp: &xdr.PathPaymentStrictSendOp{
					SendAsset:   nativeAsset,
					SendAmount:  1598182,
					Destination: testAccount4,
					DestAsset:   nativeAsset,
					DestMin:     4280460538,
					Path:        []xdr.Asset{usdtAsset},
				},
			},
		},
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeCreateClaimableBalance,
				CreateClaimableBalanceOp: &xdr.CreateClaimableBalanceOp{
					Asset:     usdtAsset,
					Amount:    1234567890000,
					Claimants: []xdr.Claimant{hardCodedClaimant},
				},
			},
		},
		xdr.Operation{
			SourceAccount: &testAccount3,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeClaimClaimableBalance,
				ClaimClaimableBalanceOp: &xdr.ClaimClaimableBalanceOp{
					BalanceId: hardCodedClaimableBalance,
				},
			},
		},
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeBeginSponsoringFutureReserves,
				BeginSponsoringFutureReservesOp: &xdr.BeginSponsoringFutureReservesOp{
					SponsoredId: testAccount4ID,
				},
			},
		},
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeRevokeSponsorship,
				RevokeSponsorshipOp: &xdr.RevokeSponsorshipOp{
					Type: xdr.RevokeSponsorshipTypeRevokeSponsorshipSigner,
					Signer: &xdr.RevokeSponsorshipOpSigner{
						AccountId: testAccount4ID,
						SignerKey: hardCodedSigner.Key,
					},
				},
			},
		},
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeRevokeSponsorship,
				RevokeSponsorshipOp: &xdr.RevokeSponsorshipOp{
					Type: xdr.RevokeSponsorshipTypeRevokeSponsorshipLedgerEntry,
					LedgerKey: &xdr.LedgerKey{
						Type: xdr.LedgerEntryTypeAccount,
						Account: &xdr.LedgerKeyAccount{
							AccountId: testAccount4ID,
						},
					},
				},
			},
		},
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeRevokeSponsorship,
				RevokeSponsorshipOp: &xdr.RevokeSponsorshipOp{
					Type: xdr.RevokeSponsorshipTypeRevokeSponsorshipLedgerEntry,
					LedgerKey: &xdr.LedgerKey{
						Type: xdr.LedgerEntryTypeClaimableBalance,
						ClaimableBalance: &xdr.LedgerKeyClaimableBalance{
							BalanceId: hardCodedClaimableBalance,
						},
					},
				},
			},
		},
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeRevokeSponsorship,
				RevokeSponsorshipOp: &xdr.RevokeSponsorshipOp{
					Type: xdr.RevokeSponsorshipTypeRevokeSponsorshipLedgerEntry,
					LedgerKey: &xdr.LedgerKey{
						Type: xdr.LedgerEntryTypeData,
						Data: &xdr.LedgerKeyData{
							AccountId: testAccount4ID,
							DataName:  "test",
						},
					},
				},
			},
		},
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeRevokeSponsorship,
				RevokeSponsorshipOp: &xdr.RevokeSponsorshipOp{
					Type: xdr.RevokeSponsorshipTypeRevokeSponsorshipLedgerEntry,
					LedgerKey: &xdr.LedgerKey{
						Type: xdr.LedgerEntryTypeOffer,
						Offer: &xdr.LedgerKeyOffer{
							SellerId: testAccount3ID,
							OfferId:  100,
						},
					},
				},
			},
		},
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeRevokeSponsorship,
				RevokeSponsorshipOp: &xdr.RevokeSponsorshipOp{
					Type: xdr.RevokeSponsorshipTypeRevokeSponsorshipLedgerEntry,
					LedgerKey: &xdr.LedgerKey{
						Type: xdr.LedgerEntryTypeTrustline,
						TrustLine: &xdr.LedgerKeyTrustLine{
							AccountId: testAccount3ID,
							Asset:     usdtTrustLineAsset,
						},
					},
				},
			},
		},
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeRevokeSponsorship,
				RevokeSponsorshipOp: &xdr.RevokeSponsorshipOp{
					Type: xdr.RevokeSponsorshipTypeRevokeSponsorshipLedgerEntry,
					LedgerKey: &xdr.LedgerKey{
						Type: xdr.LedgerEntryTypeLiquidityPool,
						LiquidityPool: &xdr.LedgerKeyLiquidityPool{
							LiquidityPoolId: xdr.PoolId{1, 2, 3, 4, 5, 6, 7, 8, 9},
						},
					},
				},
			},
		},
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeClawback,
				ClawbackOp: &xdr.ClawbackOp{
					Asset:  usdtAsset,
					From:   testAccount4,
					Amount: 1598182,
				},
			},
		},
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeClawbackClaimableBalance,
				ClawbackClaimableBalanceOp: &xdr.ClawbackClaimableBalanceOp{
					BalanceId: hardCodedClaimableBalance,
				},
			},
		},
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeSetTrustLineFlags,
				SetTrustLineFlagsOp: &xdr.SetTrustLineFlagsOp{
					Trustor:    testAccount4ID,
					Asset:      usdtAsset,
					SetFlags:   hardCodedSetFlags,
					ClearFlags: hardCodedClearFlags,
				},
			},
		},
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeLiquidityPoolDeposit,
				LiquidityPoolDepositOp: &xdr.LiquidityPoolDepositOp{
					LiquidityPoolId: xdr.PoolId{1, 2, 3, 4, 5, 6, 7, 8, 9},
					MaxAmountA:      1000,
					MaxAmountB:      100,
					MinPrice: xdr.Price{
						N: 1,
						D: 1000000,
					},
					MaxPrice: xdr.Price{
						N: 1000000,
						D: 1,
					},
				},
			},
		},
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeLiquidityPoolWithdraw,
				LiquidityPoolWithdrawOp: &xdr.LiquidityPoolWithdrawOp{
					LiquidityPoolId: xdr.PoolId{1, 2, 3, 4, 5, 6, 7, 8, 9},
					Amount:          4,
					MinAmountA:      1,
					MinAmountB:      1,
				},
			},
		},
	}
	inputEnvelope.Tx.Operations = inputOperations
	results := []xdr.OperationResult{
		xdr.OperationResult{},
		xdr.OperationResult{},
		xdr.OperationResult{},
		// There needs to be a true result for path payment receive and send
		xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypePathPaymentStrictReceive,
				PathPaymentStrictReceiveResult: &xdr.PathPaymentStrictReceiveResult{
					Code: xdr.PathPaymentStrictReceiveResultCodePathPaymentStrictReceiveSuccess,
					Success: &xdr.PathPaymentStrictReceiveResultSuccess{
						Last: xdr.SimplePaymentResult{Amount: 8946764349},
					},
				},
			},
		},
		xdr.OperationResult{},
		xdr.OperationResult{},
		xdr.OperationResult{},
		xdr.OperationResult{},
		xdr.OperationResult{},
		xdr.OperationResult{},
		xdr.OperationResult{},
		xdr.OperationResult{},
		xdr.OperationResult{},
		xdr.OperationResult{},
		xdr.OperationResult{},
		xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypePathPaymentStrictSend,
				PathPaymentStrictSendResult: &xdr.PathPaymentStrictSendResult{
					Code: xdr.PathPaymentStrictSendResultCodePathPaymentStrictSendSuccess,
					Success: &xdr.PathPaymentStrictSendResultSuccess{
						Last: xdr.SimplePaymentResult{Amount: 4334043858},
					},
				},
			},
		},
		xdr.OperationResult{},
		xdr.OperationResult{},
		xdr.OperationResult{},
		xdr.OperationResult{},
		xdr.OperationResult{},
		xdr.OperationResult{},
		xdr.OperationResult{},
		xdr.OperationResult{},
		xdr.OperationResult{},
		xdr.OperationResult{},
		xdr.OperationResult{},
		xdr.OperationResult{},
		xdr.OperationResult{},
		xdr.OperationResult{},
		xdr.OperationResult{},
	}
	inputTransaction.Result.Result.Result.Results = &results
	inputTransaction.Envelope.V1 = &inputEnvelope
	return
}

func makeOperationTestOutputs() (transformedOperations []OperationOutput) {
	hardCodedSourceAccountAddress := testAccount3Address
	hardCodedDestAccountAddress := testAccount4Address
	hardCodedLedgerClose := genericCloseTime.UTC()
	transformedOperations = []OperationOutput{
		OperationOutput{
			SourceAccount: hardCodedSourceAccountAddress,
			Type:          0,
			TypeString:    "create_account",
			TransactionID: 4096,
			OperationID:   4097,
			OperationDetails: map[string]interface{}{
				"account":          hardCodedDestAccountAddress,
				"funder":           hardCodedSourceAccountAddress,
				"starting_balance": 2.5,
			},
			ClosedAt: hardCodedLedgerClose,
		},
		OperationOutput{
			Type:          1,
			TypeString:    "payment",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4098,
			OperationDetails: map[string]interface{}{
				"from":         hardCodedSourceAccountAddress,
				"to":           hardCodedDestAccountAddress,
				"amount":       35.0,
				"asset_code":   "USDT",
				"asset_type":   "credit_alphanum4",
				"asset_issuer": hardCodedDestAccountAddress,
				"asset_id":     int64(-8205667356306085451),
			},
			ClosedAt: hardCodedLedgerClose,
		},
		OperationOutput{
			Type:          1,
			TypeString:    "payment",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4099,
			OperationDetails: map[string]interface{}{
				"from":       hardCodedSourceAccountAddress,
				"to":         hardCodedDestAccountAddress,
				"amount":     35.0,
				"asset_type": "native",
				"asset_id":   int64(-5706705804583548011),
			},
			ClosedAt: hardCodedLedgerClose,
		},
		OperationOutput{
			Type:          2,
			TypeString:    "path_payment_strict_receive",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4100,
			OperationDetails: map[string]interface{}{
				"from":              hardCodedSourceAccountAddress,
				"to":                hardCodedDestAccountAddress,
				"source_amount":     894.6764349,
				"source_max":        895.14959,
				"amount":            895.14959,
				"source_asset_type": "native",
				"source_asset_id":   int64(-5706705804583548011),
				"asset_type":        "native",
				"asset_id":          int64(-5706705804583548011),
				"path":              []Path{usdtAssetPath},
			},
			ClosedAt: hardCodedLedgerClose,
		},
		OperationOutput{
			Type:          3,
			TypeString:    "manage_sell_offer",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4101,
			OperationDetails: map[string]interface{}{
				"price":    0.514092,
				"amount":   76.586,
				"offer_id": int64(0.0),
				"price_r": Price{
					Numerator:   128523,
					Denominator: 250000,
				},
				"selling_asset_code":   "USDT",
				"selling_asset_type":   "credit_alphanum4",
				"selling_asset_issuer": hardCodedDestAccountAddress,
				"selling_asset_id":     int64(-8205667356306085451),
				"buying_asset_type":    "native",
				"buying_asset_id":      int64(-5706705804583548011),
			},
			ClosedAt: hardCodedLedgerClose,
		},
		OperationOutput{
			Type:          4,
			TypeString:    "create_passive_sell_offer",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4102,
			OperationDetails: map[string]interface{}{
				"amount": 63.1595,
				"price":  0.0791606,
				"price_r": Price{
					Numerator:   99583200,
					Denominator: 1257990000,
				},
				"buying_asset_code":   "USDT",
				"buying_asset_type":   "credit_alphanum4",
				"buying_asset_issuer": hardCodedDestAccountAddress,
				"buying_asset_id":     int64(-8205667356306085451),
				"selling_asset_type":  "native",
				"selling_asset_id":    int64(-5706705804583548011),
			},
			ClosedAt: hardCodedLedgerClose,
		},
		OperationOutput{
			Type:          5,
			TypeString:    "set_options",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4103,
			OperationDetails: map[string]interface{}{
				"inflation_dest":    hardCodedDestAccountAddress,
				"clear_flags":       []int32{1, 2},
				"clear_flags_s":     []string{"auth_required", "auth_revocable"},
				"set_flags":         []int32{4},
				"set_flags_s":       []string{"auth_immutable"},
				"master_key_weight": uint32(3),
				"low_threshold":     uint32(1),
				"med_threshold":     uint32(3),
				"high_threshold":    uint32(5),
				"home_domain":       "2019=DRA;n-test",
				"signer_key":        "GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF",
				"signer_weight":     uint32(1),
			},
			ClosedAt: hardCodedLedgerClose,
		},
		OperationOutput{
			Type:          6,
			TypeString:    "change_trust",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4104,
			OperationDetails: map[string]interface{}{
				"trustor":      hardCodedSourceAccountAddress,
				"trustee":      hardCodedDestAccountAddress,
				"limit":        50000000000.0,
				"asset_code":   "USSD",
				"asset_type":   "credit_alphanum4",
				"asset_issuer": hardCodedDestAccountAddress,
				"asset_id":     int64(6690054458235693884),
			},
			ClosedAt: hardCodedLedgerClose,
		},
		OperationOutput{
			Type:          6,
			TypeString:    "change_trust",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4105,
			OperationDetails: map[string]interface{}{
				"trustor":           hardCodedSourceAccountAddress,
				"limit":             50000000000.0,
				"asset_type":        "liquidity_pool_shares",
				"liquidity_pool_id": "185a6b384c651552ba09b32851b79f5f6ab61e80883d303f52bea1406a4923f0",
			},
			ClosedAt: hardCodedLedgerClose,
		},
		OperationOutput{
			Type:          7,
			TypeString:    "allow_trust",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4106,
			OperationDetails: map[string]interface{}{
				"trustee":      hardCodedSourceAccountAddress,
				"trustor":      hardCodedDestAccountAddress,
				"authorize":    true,
				"asset_code":   "USDT",
				"asset_type":   "credit_alphanum4",
				"asset_issuer": hardCodedSourceAccountAddress,
				"asset_id":     int64(8485542065083974675),
			},
			ClosedAt: hardCodedLedgerClose,
		},
		OperationOutput{
			Type:          8,
			TypeString:    "account_merge",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4107,
			OperationDetails: map[string]interface{}{
				"account": hardCodedSourceAccountAddress,
				"into":    hardCodedDestAccountAddress,
			},
			ClosedAt: hardCodedLedgerClose,
		},
		OperationOutput{
			Type:             9,
			TypeString:       "inflation",
			SourceAccount:    hardCodedSourceAccountAddress,
			TransactionID:    4096,
			OperationID:      4108,
			OperationDetails: map[string]interface{}{},
			ClosedAt:         hardCodedLedgerClose,
		},
		OperationOutput{
			Type:          10,
			TypeString:    "manage_data",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4109,
			OperationDetails: map[string]interface{}{
				"name":  "test",
				"value": base64.StdEncoding.EncodeToString([]byte{0x76, 0x61, 0x6c, 0x75, 0x65}),
			},
			ClosedAt: hardCodedLedgerClose,
		},
		OperationOutput{
			Type:          11,
			TypeString:    "bump_sequence",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4110,
			OperationDetails: map[string]interface{}{
				"bump_to": "100",
			},
			ClosedAt: hardCodedLedgerClose,
		},
		OperationOutput{
			Type:          12,
			TypeString:    "manage_buy_offer",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4111,
			OperationDetails: map[string]interface{}{
				"price":  0.3496823,
				"amount": 765.4501001,
				"price_r": Price{
					Numerator:   635863285,
					Denominator: 1818402817,
				},
				"selling_asset_code":   "USDT",
				"selling_asset_type":   "credit_alphanum4",
				"selling_asset_issuer": hardCodedDestAccountAddress,
				"selling_asset_id":     int64(-8205667356306085451),
				"buying_asset_type":    "native",
				"buying_asset_id":      int64(-5706705804583548011),
				"offer_id":             int64(100),
			},
			ClosedAt: hardCodedLedgerClose,
		},
		OperationOutput{
			Type:          13,
			TypeString:    "path_payment_strict_send",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4112,
			OperationDetails: map[string]interface{}{
				"from":              hardCodedSourceAccountAddress,
				"to":                hardCodedDestAccountAddress,
				"source_amount":     0.1598182,
				"destination_min":   "428.0460538",
				"amount":            433.4043858,
				"path":              []Path{usdtAssetPath},
				"source_asset_type": "native",
				"source_asset_id":   int64(-5706705804583548011),
				"asset_type":        "native",
				"asset_id":          int64(-5706705804583548011),
			},
			ClosedAt: hardCodedLedgerClose,
		},
		OperationOutput{
			Type:          14,
			TypeString:    "create_claimable_balance",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4113,
			OperationDetails: map[string]interface{}{
				"asset":     "USDT:GBVVRXLMNCJQW3IDDXC3X6XCH35B5Q7QXNMMFPENSOGUPQO7WO7HGZPA",
				"amount":    123456.789,
				"claimants": []Claimant{testClaimantDetails},
			},
			ClosedAt: hardCodedLedgerClose,
		},
		OperationOutput{
			Type:          15,
			TypeString:    "claim_claimable_balance",
			SourceAccount: testAccount3Address,
			TransactionID: 4096,
			OperationID:   4114,
			OperationDetails: map[string]interface{}{
				"claimant":   hardCodedSourceAccountAddress,
				"balance_id": "000000000102030405060708090000000000000000000000000000000000000000000000",
			},
			ClosedAt: hardCodedLedgerClose,
		},
		OperationOutput{
			Type:          16,
			TypeString:    "begin_sponsoring_future_reserves",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4115,
			OperationDetails: map[string]interface{}{
				"sponsored_id": hardCodedDestAccountAddress,
			},
			ClosedAt: hardCodedLedgerClose,
		},
		OperationOutput{
			Type:          18,
			TypeString:    "revoke_sponsorship",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4116,
			OperationDetails: map[string]interface{}{
				"signer_account_id": hardCodedDestAccountAddress,
				"signer_key":        "GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF",
			},
			ClosedAt: hardCodedLedgerClose,
		},
		OperationOutput{
			Type:          18,
			TypeString:    "revoke_sponsorship",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4117,
			OperationDetails: map[string]interface{}{
				"account_id": hardCodedDestAccountAddress,
			},
			ClosedAt: hardCodedLedgerClose,
		},
		OperationOutput{
			Type:          18,
			TypeString:    "revoke_sponsorship",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4118,
			OperationDetails: map[string]interface{}{
				"claimable_balance_id": "000000000102030405060708090000000000000000000000000000000000000000000000",
			},
			ClosedAt: hardCodedLedgerClose,
		},
		OperationOutput{
			Type:          18,
			TypeString:    "revoke_sponsorship",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4119,
			OperationDetails: map[string]interface{}{
				"data_account_id": hardCodedDestAccountAddress,
				"data_name":       "test",
			},
			ClosedAt: hardCodedLedgerClose,
		},
		OperationOutput{
			Type:          18,
			TypeString:    "revoke_sponsorship",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4120,
			OperationDetails: map[string]interface{}{
				"offer_id": int64(100),
			},
			ClosedAt: hardCodedLedgerClose,
		},
		OperationOutput{
			Type:          18,
			TypeString:    "revoke_sponsorship",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4121,
			OperationDetails: map[string]interface{}{
				"trustline_account_id": testAccount3Address,
				"trustline_asset":      "USTT:GBT4YAEGJQ5YSFUMNKX6BPBUOCPNAIOFAVZOF6MIME2CECBMEIUXFZZN",
			},
			ClosedAt: hardCodedLedgerClose,
		},
		OperationOutput{
			Type:          18,
			TypeString:    "revoke_sponsorship",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4122,
			OperationDetails: map[string]interface{}{
				"liquidity_pool_id": "0102030405060708090000000000000000000000000000000000000000000000",
			},
			ClosedAt: hardCodedLedgerClose,
		},
		OperationOutput{
			Type:          19,
			TypeString:    "clawback",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4123,
			OperationDetails: map[string]interface{}{
				"from":         hardCodedDestAccountAddress,
				"amount":       0.1598182,
				"asset_code":   "USDT",
				"asset_issuer": "GBVVRXLMNCJQW3IDDXC3X6XCH35B5Q7QXNMMFPENSOGUPQO7WO7HGZPA",
				"asset_type":   "credit_alphanum4",
				"asset_id":     int64(-8205667356306085451),
			},
			ClosedAt: hardCodedLedgerClose,
		},
		OperationOutput{
			Type:          20,
			TypeString:    "clawback_claimable_balance",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4124,
			OperationDetails: map[string]interface{}{
				"balance_id": "000000000102030405060708090000000000000000000000000000000000000000000000",
			},
			ClosedAt: hardCodedLedgerClose,
		},
		OperationOutput{
			Type:          21,
			TypeString:    "set_trust_line_flags",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4125,
			OperationDetails: map[string]interface{}{
				"asset_code":    "USDT",
				"asset_issuer":  "GBVVRXLMNCJQW3IDDXC3X6XCH35B5Q7QXNMMFPENSOGUPQO7WO7HGZPA",
				"asset_type":    "credit_alphanum4",
				"asset_id":      int64(-8205667356306085451),
				"trustor":       testAccount4Address,
				"clear_flags":   []int32{1, 2},
				"clear_flags_s": []string{"authorized", "authorized_to_maintain_liabilities"},
				"set_flags":     []int32{4},
				"set_flags_s":   []string{"clawback_enabled"},
			},
			ClosedAt: hardCodedLedgerClose,
		},
		OperationOutput{
			Type:          22,
			TypeString:    "liquidity_pool_deposit",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4126,
			OperationDetails: map[string]interface{}{
				"liquidity_pool_id":        "0102030405060708090000000000000000000000000000000000000000000000",
				"reserve_a_asset_type":     "native",
				"reserve_a_asset_id":       int64(-5706705804583548011),
				"reserve_a_max_amount":     0.0001,
				"reserve_a_deposit_amount": 0.0001,
				"reserve_b_asset_type":     "credit_alphanum4",
				"reserve_b_asset_code":     "USSD",
				"reserve_b_asset_issuer":   "GBVVRXLMNCJQW3IDDXC3X6XCH35B5Q7QXNMMFPENSOGUPQO7WO7HGZPA",
				"reserve_b_asset_id":       int64(6690054458235693884),
				"reserve_b_deposit_amount": 0.00001,
				"reserve_b_max_amount":     0.00001,
				"max_price":                1000000.0000000,
				"max_price_r": Price{
					Numerator:   1000000,
					Denominator: 1,
				},
				"min_price": 0.0000010,
				"min_price_r": Price{
					Numerator:   1,
					Denominator: 1000000,
				},
				"shares_received": 0.0000002,
			},
			ClosedAt: hardCodedLedgerClose,
		},
		OperationOutput{
			Type:          23,
			TypeString:    "liquidity_pool_withdraw",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4127,
			OperationDetails: map[string]interface{}{
				"liquidity_pool_id":         "0102030405060708090000000000000000000000000000000000000000000000",
				"reserve_a_asset_type":      "native",
				"reserve_a_asset_id":        int64(-5706705804583548011),
				"reserve_a_min_amount":      0.0000001,
				"reserve_a_withdraw_amount": -0.0001,
				"reserve_b_asset_type":      "credit_alphanum4",
				"reserve_b_asset_code":      "USSD",
				"reserve_b_asset_issuer":    "GBVVRXLMNCJQW3IDDXC3X6XCH35B5Q7QXNMMFPENSOGUPQO7WO7HGZPA",
				"reserve_b_asset_id":        int64(6690054458235693884),
				"reserve_b_withdraw_amount": -0.00001,
				"reserve_b_min_amount":      0.0000001,
				"shares":                    0.0000004,
			},
			ClosedAt: hardCodedLedgerClose,
		},
	}
	return
}
