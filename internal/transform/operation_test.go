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
			fmt.Errorf("the operation type (-1) is negative for  operation 1 (operation id=4098)"),
		},
		{
			unknownOpTypeInput,
			OperationOutput{},
			fmt.Errorf("unknown operation type: "),
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
		actualOutput, actualError := TransformOperation(test.input.operation, test.input.index, test.input.transaction, 0, test.input.ledgerClosedMeta, "")
		assert.Equal(t, test.wantErr, actualError)
		assert.Equal(t, test.wantOutput, actualOutput)
	}
}

func makeLedgerCloseMeta() (ledgerCloseMeta xdr.LedgerCloseMeta) {
	return xdr.LedgerCloseMeta{
		V: 0,
		V0: &xdr.LedgerCloseMetaV0{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					ScpValue: xdr.StellarValue{
						CloseTime: 0,
					},
					LedgerSeq: 0,
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

	//var wasm []byte
	//var contractHash xdr.Hash
	//var salt [32]byte
	//var assetCode [12]byte
	//var assetIssuer xdr.Uint256

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
		{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeCreateAccount,
				CreateAccountOp: &xdr.CreateAccountOp{
					StartingBalance: 25000000,
					Destination:     testAccount4ID,
				},
			},
		},
		{
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
		{
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
		{
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
		{
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
		{
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
		{
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
		{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeChangeTrust,
				ChangeTrustOp: &xdr.ChangeTrustOp{
					Line:  usdtChangeTrustAsset,
					Limit: xdr.Int64(500000000000000000),
				},
			},
		},
		{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeChangeTrust,
				ChangeTrustOp: &xdr.ChangeTrustOp{
					Line:  usdtLiquidityPoolShare,
					Limit: xdr.Int64(500000000000000000),
				},
			},
		},
		{
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
		{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type:        xdr.OperationTypeAccountMerge,
				Destination: &testAccount4,
			},
		},
		{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeInflation,
			},
		},
		{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeManageData,
				ManageDataOp: &xdr.ManageDataOp{
					DataName:  "test",
					DataValue: &hardCodedDataValue,
				},
			},
		},
		{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeBumpSequence,
				BumpSequenceOp: &xdr.BumpSequenceOp{
					BumpTo: hardCodedSequenceNumber,
				},
			},
		},
		{
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
		{
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
		{
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
		{
			SourceAccount: &testAccount3,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeClaimClaimableBalance,
				ClaimClaimableBalanceOp: &xdr.ClaimClaimableBalanceOp{
					BalanceId: hardCodedClaimableBalance,
				},
			},
		},
		{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeBeginSponsoringFutureReserves,
				BeginSponsoringFutureReservesOp: &xdr.BeginSponsoringFutureReservesOp{
					SponsoredId: testAccount4ID,
				},
			},
		},
		{
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
		{
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
		{
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
		{
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
		{
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
		{
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
		{
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
		{
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
		{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeClawbackClaimableBalance,
				ClawbackClaimableBalanceOp: &xdr.ClawbackClaimableBalanceOp{
					BalanceId: hardCodedClaimableBalance,
				},
			},
		},
		{
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
		{
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
		{
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
		//{
		//	SourceAccount: nil,
		//	Body: xdr.OperationBody{
		//		Type: xdr.OperationTypeInvokeHostFunction,
		//		InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{
		//			HostFunction: xdr.HostFunction{
		//				Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
		//				InvokeContract: &xdr.InvokeContractArgs{
		//					ContractAddress: xdr.ScAddress{
		//						Type:       xdr.ScAddressTypeScAddressTypeContract,
		//						ContractId: &contractHash,
		//					},
		//					FunctionName: "test",
		//					Args:         []xdr.ScVal{},
		//				},
		//			},
		//		},
		//	},
		//},
		//{
		//	SourceAccount: nil,
		//	Body: xdr.OperationBody{
		//		Type: xdr.OperationTypeInvokeHostFunction,
		//		InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{
		//			HostFunction: xdr.HostFunction{
		//				Type: xdr.HostFunctionTypeHostFunctionTypeCreateContract,
		//				CreateContract: &xdr.CreateContractArgs{
		//					ContractIdPreimage: xdr.ContractIdPreimage{
		//						Type: xdr.ContractIdPreimageTypeContractIdPreimageFromAddress,
		//						FromAddress: &xdr.ContractIdPreimageFromAddress{
		//							Address: xdr.ScAddress{
		//								Type:       xdr.ScAddressTypeScAddressTypeContract,
		//								ContractId: &contractHash,
		//							},
		//							Salt: salt,
		//						},
		//					},
		//					Executable: xdr.ContractExecutable{},
		//				},
		//			},
		//		},
		//	},
		//},
		//{
		//	SourceAccount: nil,
		//	Body: xdr.OperationBody{
		//		Type: xdr.OperationTypeInvokeHostFunction,
		//		InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{
		//			HostFunction: xdr.HostFunction{
		//				Type: xdr.HostFunctionTypeHostFunctionTypeCreateContract,
		//				CreateContract: &xdr.CreateContractArgs{
		//					ContractIdPreimage: xdr.ContractIdPreimage{
		//						Type: xdr.ContractIdPreimageTypeContractIdPreimageFromAsset,
		//						FromAsset: &xdr.Asset{
		//							Type: xdr.AssetTypeAssetTypeCreditAlphanum12,
		//							AlphaNum12: &xdr.AlphaNum12{
		//								AssetCode: assetCode,
		//								Issuer: xdr.AccountId{
		//									Type:    xdr.PublicKeyTypePublicKeyTypeEd25519,
		//									Ed25519: &assetIssuer,
		//								},
		//							},
		//						},
		//					},
		//					Executable: xdr.ContractExecutable{},
		//				},
		//			},
		//		},
		//	},
		//},
		//{
		//	SourceAccount: nil,
		//	Body: xdr.OperationBody{
		//		Type: xdr.OperationTypeInvokeHostFunction,
		//		InvokeHostFunctionOp: &xdr.InvokeHostFunctionOp{
		//			HostFunction: xdr.HostFunction{
		//				Type: xdr.HostFunctionTypeHostFunctionTypeUploadContractWasm,
		//				Wasm: &wasm,
		//			},
		//		},
		//	},
		//},
		//{
		//	SourceAccount: nil,
		//	Body: xdr.OperationBody{
		//		Type: xdr.OperationTypeBumpFootprintExpiration,
		//		BumpFootprintExpirationOp: &xdr.BumpFootprintExpirationOp{
		//			Ext: xdr.ExtensionPoint{
		//				V: 0,
		//			},
		//			LedgersToExpire: 1234,
		//		},
		//	},
		//},
		//{
		//	SourceAccount: nil,
		//	Body: xdr.OperationBody{
		//		Type: xdr.OperationTypeRestoreFootprint,
		//		RestoreFootprintOp: &xdr.RestoreFootprintOp{
		//			Ext: xdr.ExtensionPoint{
		//				V: 0,
		//			},
		//		},
		//	},
		//},
	}
	inputEnvelope.Tx.Operations = inputOperations
	results := []xdr.OperationResult{
		{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeCreateAccount,
				CreateAccountResult: &xdr.CreateAccountResult{
					Code: xdr.CreateAccountResultCodeCreateAccountSuccess,
				},
			},
		},
		{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypePayment,
				PaymentResult: &xdr.PaymentResult{
					Code: xdr.PaymentResultCodePaymentSuccess,
				},
			},
		},
		{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypePayment,
				PaymentResult: &xdr.PaymentResult{
					Code: xdr.PaymentResultCodePaymentSuccess,
				},
			},
		},
		// There needs to be a true result for path payment receive and send
		{
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
		{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeManageSellOffer,
				ManageSellOfferResult: &xdr.ManageSellOfferResult{
					Code: xdr.ManageSellOfferResultCodeManageSellOfferSuccess,
				},
			},
		},
		{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeManageSellOffer,
				ManageSellOfferResult: &xdr.ManageSellOfferResult{
					Code: xdr.ManageSellOfferResultCodeManageSellOfferSuccess,
				},
			},
		},
		{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeSetOptions,
				SetOptionsResult: &xdr.SetOptionsResult{
					Code: xdr.SetOptionsResultCodeSetOptionsSuccess,
				},
			},
		},
		{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeChangeTrust,
				ChangeTrustResult: &xdr.ChangeTrustResult{
					Code: xdr.ChangeTrustResultCodeChangeTrustSuccess,
				},
			},
		},
		{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeChangeTrust,
				ChangeTrustResult: &xdr.ChangeTrustResult{
					Code: xdr.ChangeTrustResultCodeChangeTrustSuccess,
				},
			},
		},
		{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeAllowTrust,
				AllowTrustResult: &xdr.AllowTrustResult{
					Code: xdr.AllowTrustResultCodeAllowTrustSuccess,
				},
			},
		},
		{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeAccountMerge,
				AccountMergeResult: &xdr.AccountMergeResult{
					Code: xdr.AccountMergeResultCodeAccountMergeSuccess,
				},
			},
		},
		{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeInflation,
				InflationResult: &xdr.InflationResult{
					Code: xdr.InflationResultCodeInflationSuccess,
				},
			},
		},
		{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeManageData,
				ManageDataResult: &xdr.ManageDataResult{
					Code: xdr.ManageDataResultCodeManageDataSuccess,
				},
			},
		},
		{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeBumpSequence,
				BumpSeqResult: &xdr.BumpSequenceResult{
					Code: xdr.BumpSequenceResultCodeBumpSequenceSuccess,
				},
			},
		},
		{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeManageBuyOffer,
				ManageBuyOfferResult: &xdr.ManageBuyOfferResult{
					Code: xdr.ManageBuyOfferResultCodeManageBuyOfferSuccess,
				},
			},
		},
		{
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
		{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeCreateClaimableBalance,
				CreateClaimableBalanceResult: &xdr.CreateClaimableBalanceResult{
					Code: xdr.CreateClaimableBalanceResultCodeCreateClaimableBalanceSuccess,
				},
			},
		},
		{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeClaimClaimableBalance,
				ClaimClaimableBalanceResult: &xdr.ClaimClaimableBalanceResult{
					Code: xdr.ClaimClaimableBalanceResultCodeClaimClaimableBalanceSuccess,
				},
			},
		},
		{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeBeginSponsoringFutureReserves,
				BeginSponsoringFutureReservesResult: &xdr.BeginSponsoringFutureReservesResult{
					Code: xdr.BeginSponsoringFutureReservesResultCodeBeginSponsoringFutureReservesSuccess,
				},
			},
		},
		{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeRevokeSponsorship,
				RevokeSponsorshipResult: &xdr.RevokeSponsorshipResult{
					Code: xdr.RevokeSponsorshipResultCodeRevokeSponsorshipSuccess,
				},
			},
		},
		{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeRevokeSponsorship,
				RevokeSponsorshipResult: &xdr.RevokeSponsorshipResult{
					Code: xdr.RevokeSponsorshipResultCodeRevokeSponsorshipSuccess,
				},
			},
		},
		{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeRevokeSponsorship,
				RevokeSponsorshipResult: &xdr.RevokeSponsorshipResult{
					Code: xdr.RevokeSponsorshipResultCodeRevokeSponsorshipSuccess,
				},
			},
		},
		{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeRevokeSponsorship,
				RevokeSponsorshipResult: &xdr.RevokeSponsorshipResult{
					Code: xdr.RevokeSponsorshipResultCodeRevokeSponsorshipSuccess,
				},
			},
		},
		{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeRevokeSponsorship,
				RevokeSponsorshipResult: &xdr.RevokeSponsorshipResult{
					Code: xdr.RevokeSponsorshipResultCodeRevokeSponsorshipSuccess,
				},
			},
		},
		{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeRevokeSponsorship,
				RevokeSponsorshipResult: &xdr.RevokeSponsorshipResult{
					Code: xdr.RevokeSponsorshipResultCodeRevokeSponsorshipSuccess,
				},
			},
		},
		{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeRevokeSponsorship,
				RevokeSponsorshipResult: &xdr.RevokeSponsorshipResult{
					Code: xdr.RevokeSponsorshipResultCodeRevokeSponsorshipSuccess,
				},
			},
		},
		{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeClawback,
				ClawbackResult: &xdr.ClawbackResult{
					Code: xdr.ClawbackResultCodeClawbackSuccess,
				},
			},
		},
		{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeClawbackClaimableBalance,
				ClawbackClaimableBalanceResult: &xdr.ClawbackClaimableBalanceResult{
					Code: xdr.ClawbackClaimableBalanceResultCodeClawbackClaimableBalanceSuccess,
				},
			},
		},
		{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeSetTrustLineFlags,
				SetTrustLineFlagsResult: &xdr.SetTrustLineFlagsResult{
					Code: xdr.SetTrustLineFlagsResultCodeSetTrustLineFlagsSuccess,
				},
			},
		},
		{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeLiquidityPoolDeposit,
				LiquidityPoolDepositResult: &xdr.LiquidityPoolDepositResult{
					Code: xdr.LiquidityPoolDepositResultCodeLiquidityPoolDepositSuccess,
				},
			},
		},
		{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeLiquidityPoolWithdraw,
				LiquidityPoolWithdrawResult: &xdr.LiquidityPoolWithdrawResult{
					Code: xdr.LiquidityPoolWithdrawResultCodeLiquidityPoolWithdrawSuccess,
				},
			},
		},
		//{},
		//{},
		//{},
		//{},
		//{},
		//{},
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
		{
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
			ClosedAt:            hardCodedLedgerClose,
			OperationResultCode: "OperationResultCodeOpInner",
			OperationTraceCode:  "CreateAccountResultCodeCreateAccountSuccess",
			LedgerSequence:      0,
			OperationDetailsJSON: map[string]interface{}{
				"account":          hardCodedDestAccountAddress,
				"funder":           hardCodedSourceAccountAddress,
				"starting_balance": 2.5,
			},
		},
		{
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
			ClosedAt:            hardCodedLedgerClose,
			OperationResultCode: "OperationResultCodeOpInner",
			OperationTraceCode:  "PaymentResultCodePaymentSuccess",
			LedgerSequence:      0,
			OperationDetailsJSON: map[string]interface{}{
				"from":         hardCodedSourceAccountAddress,
				"to":           hardCodedDestAccountAddress,
				"amount":       35.0,
				"asset_code":   "USDT",
				"asset_type":   "credit_alphanum4",
				"asset_issuer": hardCodedDestAccountAddress,
				"asset_id":     int64(-8205667356306085451),
			},
		},
		{
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
			ClosedAt:            hardCodedLedgerClose,
			OperationResultCode: "OperationResultCodeOpInner",
			OperationTraceCode:  "PaymentResultCodePaymentSuccess",
			LedgerSequence:      0,
			OperationDetailsJSON: map[string]interface{}{
				"from":       hardCodedSourceAccountAddress,
				"to":         hardCodedDestAccountAddress,
				"amount":     35.0,
				"asset_type": "native",
				"asset_id":   int64(-5706705804583548011),
			},
		},
		{
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
			ClosedAt:            hardCodedLedgerClose,
			OperationResultCode: "OperationResultCodeOpInner",
			OperationTraceCode:  "PathPaymentStrictReceiveResultCodePathPaymentStrictReceiveSuccess",
			LedgerSequence:      0,
			OperationDetailsJSON: map[string]interface{}{
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
		},
		{
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
			ClosedAt:            hardCodedLedgerClose,
			OperationResultCode: "OperationResultCodeOpInner",
			OperationTraceCode:  "ManageSellOfferResultCodeManageSellOfferSuccess",
			LedgerSequence:      0,
			OperationDetailsJSON: map[string]interface{}{
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
		},
		{
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
			ClosedAt:            hardCodedLedgerClose,
			OperationResultCode: "OperationResultCodeOpInner",
			OperationTraceCode:  "ManageSellOfferResultCodeManageSellOfferSuccess",
			LedgerSequence:      0,
			OperationDetailsJSON: map[string]interface{}{
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
		},
		{
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
			ClosedAt:            hardCodedLedgerClose,
			OperationResultCode: "OperationResultCodeOpInner",
			OperationTraceCode:  "SetOptionsResultCodeSetOptionsSuccess",
			LedgerSequence:      0,
			OperationDetailsJSON: map[string]interface{}{
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
		},
		{
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
			ClosedAt:            hardCodedLedgerClose,
			OperationResultCode: "OperationResultCodeOpInner",
			OperationTraceCode:  "ChangeTrustResultCodeChangeTrustSuccess",
			LedgerSequence:      0,
			OperationDetailsJSON: map[string]interface{}{
				"trustor":      hardCodedSourceAccountAddress,
				"trustee":      hardCodedDestAccountAddress,
				"limit":        50000000000.0,
				"asset_code":   "USSD",
				"asset_type":   "credit_alphanum4",
				"asset_issuer": hardCodedDestAccountAddress,
				"asset_id":     int64(6690054458235693884),
			},
		},
		{
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
			ClosedAt:            hardCodedLedgerClose,
			OperationResultCode: "OperationResultCodeOpInner",
			OperationTraceCode:  "ChangeTrustResultCodeChangeTrustSuccess",
			LedgerSequence:      0,
			OperationDetailsJSON: map[string]interface{}{
				"trustor":           hardCodedSourceAccountAddress,
				"limit":             50000000000.0,
				"asset_type":        "liquidity_pool_shares",
				"liquidity_pool_id": "185a6b384c651552ba09b32851b79f5f6ab61e80883d303f52bea1406a4923f0",
			},
		},
		{
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
			ClosedAt:            hardCodedLedgerClose,
			OperationResultCode: "OperationResultCodeOpInner",
			OperationTraceCode:  "AllowTrustResultCodeAllowTrustSuccess",
			LedgerSequence:      0,
			OperationDetailsJSON: map[string]interface{}{
				"trustee":      hardCodedSourceAccountAddress,
				"trustor":      hardCodedDestAccountAddress,
				"authorize":    true,
				"asset_code":   "USDT",
				"asset_type":   "credit_alphanum4",
				"asset_issuer": hardCodedSourceAccountAddress,
				"asset_id":     int64(8485542065083974675),
			},
		},
		{
			Type:          8,
			TypeString:    "account_merge",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4107,
			OperationDetails: map[string]interface{}{
				"account": hardCodedSourceAccountAddress,
				"into":    hardCodedDestAccountAddress,
			},
			ClosedAt:            hardCodedLedgerClose,
			OperationResultCode: "OperationResultCodeOpInner",
			OperationTraceCode:  "AccountMergeResultCodeAccountMergeSuccess",
			LedgerSequence:      0,
			OperationDetailsJSON: map[string]interface{}{
				"account": hardCodedSourceAccountAddress,
				"into":    hardCodedDestAccountAddress,
			},
		},
		{
			Type:                 9,
			TypeString:           "inflation",
			SourceAccount:        hardCodedSourceAccountAddress,
			TransactionID:        4096,
			OperationID:          4108,
			OperationDetails:     map[string]interface{}{},
			ClosedAt:             hardCodedLedgerClose,
			OperationResultCode:  "OperationResultCodeOpInner",
			OperationTraceCode:   "InflationResultCodeInflationSuccess",
			LedgerSequence:       0,
			OperationDetailsJSON: map[string]interface{}{},
		},
		{
			Type:          10,
			TypeString:    "manage_data",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4109,
			OperationDetails: map[string]interface{}{
				"name":  "test",
				"value": base64.StdEncoding.EncodeToString([]byte{0x76, 0x61, 0x6c, 0x75, 0x65}),
			},
			ClosedAt:            hardCodedLedgerClose,
			OperationResultCode: "OperationResultCodeOpInner",
			OperationTraceCode:  "ManageDataResultCodeManageDataSuccess",
			LedgerSequence:      0,
			OperationDetailsJSON: map[string]interface{}{
				"name":  "test",
				"value": base64.StdEncoding.EncodeToString([]byte{0x76, 0x61, 0x6c, 0x75, 0x65}),
			},
		},
		{
			Type:          11,
			TypeString:    "bump_sequence",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4110,
			OperationDetails: map[string]interface{}{
				"bump_to": "100",
			},
			ClosedAt:            hardCodedLedgerClose,
			OperationResultCode: "OperationResultCodeOpInner",
			OperationTraceCode:  "BumpSequenceResultCodeBumpSequenceSuccess",
			LedgerSequence:      0,
			OperationDetailsJSON: map[string]interface{}{
				"bump_to": "100",
			},
		},
		{
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
			ClosedAt:            hardCodedLedgerClose,
			OperationResultCode: "OperationResultCodeOpInner",
			OperationTraceCode:  "ManageBuyOfferResultCodeManageBuyOfferSuccess",
			LedgerSequence:      0,
			OperationDetailsJSON: map[string]interface{}{
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
		},
		{
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
			ClosedAt:            hardCodedLedgerClose,
			OperationResultCode: "OperationResultCodeOpInner",
			OperationTraceCode:  "PathPaymentStrictSendResultCodePathPaymentStrictSendSuccess",
			LedgerSequence:      0,
			OperationDetailsJSON: map[string]interface{}{
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
		},
		{
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
			ClosedAt:            hardCodedLedgerClose,
			OperationResultCode: "OperationResultCodeOpInner",
			OperationTraceCode:  "CreateClaimableBalanceResultCodeCreateClaimableBalanceSuccess",
			LedgerSequence:      0,
			OperationDetailsJSON: map[string]interface{}{
				"asset":     "USDT:GBVVRXLMNCJQW3IDDXC3X6XCH35B5Q7QXNMMFPENSOGUPQO7WO7HGZPA",
				"amount":    123456.789,
				"claimants": []Claimant{testClaimantDetails},
			},
		},
		{
			Type:          15,
			TypeString:    "claim_claimable_balance",
			SourceAccount: testAccount3Address,
			TransactionID: 4096,
			OperationID:   4114,
			OperationDetails: map[string]interface{}{
				"claimant":   hardCodedSourceAccountAddress,
				"balance_id": "000000000102030405060708090000000000000000000000000000000000000000000000",
			},
			ClosedAt:            hardCodedLedgerClose,
			OperationResultCode: "OperationResultCodeOpInner",
			OperationTraceCode:  "ClaimClaimableBalanceResultCodeClaimClaimableBalanceSuccess",
			LedgerSequence:      0,
			OperationDetailsJSON: map[string]interface{}{
				"claimant":   hardCodedSourceAccountAddress,
				"balance_id": "000000000102030405060708090000000000000000000000000000000000000000000000",
			},
		},
		{
			Type:          16,
			TypeString:    "begin_sponsoring_future_reserves",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4115,
			OperationDetails: map[string]interface{}{
				"sponsored_id": hardCodedDestAccountAddress,
			},
			ClosedAt:            hardCodedLedgerClose,
			OperationResultCode: "OperationResultCodeOpInner",
			OperationTraceCode:  "BeginSponsoringFutureReservesResultCodeBeginSponsoringFutureReservesSuccess",
			LedgerSequence:      0,
			OperationDetailsJSON: map[string]interface{}{
				"sponsored_id": hardCodedDestAccountAddress,
			},
		},
		{
			Type:          18,
			TypeString:    "revoke_sponsorship",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4116,
			OperationDetails: map[string]interface{}{
				"signer_account_id": hardCodedDestAccountAddress,
				"signer_key":        "GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF",
			},
			ClosedAt:            hardCodedLedgerClose,
			OperationResultCode: "OperationResultCodeOpInner",
			OperationTraceCode:  "RevokeSponsorshipResultCodeRevokeSponsorshipSuccess",
			LedgerSequence:      0,
			OperationDetailsJSON: map[string]interface{}{
				"signer_account_id": hardCodedDestAccountAddress,
				"signer_key":        "GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF",
			},
		},
		{
			Type:          18,
			TypeString:    "revoke_sponsorship",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4117,
			OperationDetails: map[string]interface{}{
				"account_id": hardCodedDestAccountAddress,
			},
			ClosedAt:            hardCodedLedgerClose,
			OperationResultCode: "OperationResultCodeOpInner",
			OperationTraceCode:  "RevokeSponsorshipResultCodeRevokeSponsorshipSuccess",
			LedgerSequence:      0,
			OperationDetailsJSON: map[string]interface{}{
				"account_id": hardCodedDestAccountAddress,
			},
		},
		{
			Type:          18,
			TypeString:    "revoke_sponsorship",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4118,
			OperationDetails: map[string]interface{}{
				"claimable_balance_id": "000000000102030405060708090000000000000000000000000000000000000000000000",
			},
			ClosedAt:            hardCodedLedgerClose,
			OperationResultCode: "OperationResultCodeOpInner",
			OperationTraceCode:  "RevokeSponsorshipResultCodeRevokeSponsorshipSuccess",
			LedgerSequence:      0,
			OperationDetailsJSON: map[string]interface{}{
				"claimable_balance_id": "000000000102030405060708090000000000000000000000000000000000000000000000",
			},
		},
		{
			Type:          18,
			TypeString:    "revoke_sponsorship",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4119,
			OperationDetails: map[string]interface{}{
				"data_account_id": hardCodedDestAccountAddress,
				"data_name":       "test",
			},
			ClosedAt:            hardCodedLedgerClose,
			OperationResultCode: "OperationResultCodeOpInner",
			OperationTraceCode:  "RevokeSponsorshipResultCodeRevokeSponsorshipSuccess",
			LedgerSequence:      0,
			OperationDetailsJSON: map[string]interface{}{
				"data_account_id": hardCodedDestAccountAddress,
				"data_name":       "test",
			},
		},
		{
			Type:          18,
			TypeString:    "revoke_sponsorship",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4120,
			OperationDetails: map[string]interface{}{
				"offer_id": int64(100),
			},
			ClosedAt:            hardCodedLedgerClose,
			OperationResultCode: "OperationResultCodeOpInner",
			OperationTraceCode:  "RevokeSponsorshipResultCodeRevokeSponsorshipSuccess",
			LedgerSequence:      0,
			OperationDetailsJSON: map[string]interface{}{
				"offer_id": int64(100),
			},
		},
		{
			Type:          18,
			TypeString:    "revoke_sponsorship",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4121,
			OperationDetails: map[string]interface{}{
				"trustline_account_id": testAccount3Address,
				"trustline_asset":      "USTT:GBT4YAEGJQ5YSFUMNKX6BPBUOCPNAIOFAVZOF6MIME2CECBMEIUXFZZN",
			},
			ClosedAt:            hardCodedLedgerClose,
			OperationResultCode: "OperationResultCodeOpInner",
			OperationTraceCode:  "RevokeSponsorshipResultCodeRevokeSponsorshipSuccess",
			LedgerSequence:      0,
			OperationDetailsJSON: map[string]interface{}{
				"trustline_account_id": testAccount3Address,
				"trustline_asset":      "USTT:GBT4YAEGJQ5YSFUMNKX6BPBUOCPNAIOFAVZOF6MIME2CECBMEIUXFZZN",
			},
		},
		{
			Type:          18,
			TypeString:    "revoke_sponsorship",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4122,
			OperationDetails: map[string]interface{}{
				"liquidity_pool_id": "0102030405060708090000000000000000000000000000000000000000000000",
			},
			ClosedAt:            hardCodedLedgerClose,
			OperationResultCode: "OperationResultCodeOpInner",
			OperationTraceCode:  "RevokeSponsorshipResultCodeRevokeSponsorshipSuccess",
			LedgerSequence:      0,
			OperationDetailsJSON: map[string]interface{}{
				"liquidity_pool_id": "0102030405060708090000000000000000000000000000000000000000000000",
			},
		},
		{
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
			ClosedAt:            hardCodedLedgerClose,
			OperationResultCode: "OperationResultCodeOpInner",
			OperationTraceCode:  "ClawbackResultCodeClawbackSuccess",
			LedgerSequence:      0,
			OperationDetailsJSON: map[string]interface{}{
				"from":         hardCodedDestAccountAddress,
				"amount":       0.1598182,
				"asset_code":   "USDT",
				"asset_issuer": "GBVVRXLMNCJQW3IDDXC3X6XCH35B5Q7QXNMMFPENSOGUPQO7WO7HGZPA",
				"asset_type":   "credit_alphanum4",
				"asset_id":     int64(-8205667356306085451),
			},
		},
		{
			Type:          20,
			TypeString:    "clawback_claimable_balance",
			SourceAccount: hardCodedSourceAccountAddress,
			TransactionID: 4096,
			OperationID:   4124,
			OperationDetails: map[string]interface{}{
				"balance_id": "000000000102030405060708090000000000000000000000000000000000000000000000",
			},
			ClosedAt:            hardCodedLedgerClose,
			OperationResultCode: "OperationResultCodeOpInner",
			OperationTraceCode:  "ClawbackClaimableBalanceResultCodeClawbackClaimableBalanceSuccess",
			LedgerSequence:      0,
			OperationDetailsJSON: map[string]interface{}{
				"balance_id": "000000000102030405060708090000000000000000000000000000000000000000000000",
			},
		},
		{
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
			ClosedAt:            hardCodedLedgerClose,
			OperationResultCode: "OperationResultCodeOpInner",
			OperationTraceCode:  "SetTrustLineFlagsResultCodeSetTrustLineFlagsSuccess",
			LedgerSequence:      0,
			OperationDetailsJSON: map[string]interface{}{
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
		},
		{
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
			ClosedAt:            hardCodedLedgerClose,
			OperationResultCode: "OperationResultCodeOpInner",
			OperationTraceCode:  "LiquidityPoolDepositResultCodeLiquidityPoolDepositSuccess",
			LedgerSequence:      0,
			OperationDetailsJSON: map[string]interface{}{
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
		},
		{
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
			ClosedAt:            hardCodedLedgerClose,
			OperationResultCode: "OperationResultCodeOpInner",
			OperationTraceCode:  "LiquidityPoolWithdrawResultCodeLiquidityPoolWithdrawSuccess",
			LedgerSequence:      0,
			OperationDetailsJSON: map[string]interface{}{
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
		},
		//OperationOutput{
		//	Type:          24,
		//	TypeString:    "invoke_host_function",
		//	SourceAccount: hardCodedSourceAccountAddress,
		//	TransactionID: 4096,
		//	OperationID:   4128,
		//	OperationDetails: map[string]interface{}{
		//		"function":              "HostFunctionTypeHostFunctionTypeInvokeContract",
		//		"type":                  "invoke_contract",
		//		"contract_id":           "",
		//		"contract_code_hash":    "",
		//		"asset_balance_changes": []map[string]interface{}{},
		//	},
		//	ClosedAt: hardCodedLedgerClose,
		//},
		//OperationOutput{
		//	Type:          24,
		//	TypeString:    "invoke_host_function",
		//	SourceAccount: hardCodedSourceAccountAddress,
		//	TransactionID: 4096,
		//	OperationID:   4129,
		//	OperationDetails: map[string]interface{}{
		//		"function":           "HostFunctionTypeHostFunctionTypeCreateContract",
		//		"type":               "create_contract",
		//		"contract_id":        "",
		//		"contract_code_hash": "",
		//		"from":               "address",
		//		"address":            "",
		//	},
		//	ClosedAt: hardCodedLedgerClose,
		//},
		//OperationOutput{
		//	Type:          24,
		//	TypeString:    "invoke_host_function",
		//	SourceAccount: hardCodedSourceAccountAddress,
		//	TransactionID: 4096,
		//	OperationID:   4130,
		//	OperationDetails: map[string]interface{}{
		//		"function":           "HostFunctionTypeHostFunctionTypeCreateContract",
		//		"type":               "create_contract",
		//		"contract_id":        "",
		//		"contract_code_hash": "",
		//		"from":               "asset",
		//		"asset":              "",
		//	},
		//	ClosedAt: hardCodedLedgerClose,
		//},
		//OperationOutput{
		//	Type:          24,
		//	TypeString:    "invoke_host_function",
		//	SourceAccount: hardCodedSourceAccountAddress,
		//	TransactionID: 4096,
		//	OperationID:   4131,
		//	OperationDetails: map[string]interface{}{
		//		"function":           "HostFunctionTypeHostFunctionTypeUploadContractWasm",
		//		"type":               "upload_wasm",
		//		"contract_code_hash": "",
		//	},
		//	ClosedAt: hardCodedLedgerClose,
		//},
		//OperationOutput{
		//	Type:          25,
		//	TypeString:    "bump_footprint_expiration",
		//	SourceAccount: hardCodedSourceAccountAddress,
		//	TransactionID: 4096,
		//	OperationID:   4132,
		//	OperationDetails: map[string]interface{}{
		//		"type":               "bump_footprint_expiration",
		//		"ledgers_to_expire":  1234,
		//		"contract_id":        "",
		//		"contract_code_hash": "",
		//	},
		//	ClosedAt: hardCodedLedgerClose,
		//},
		//OperationOutput{
		//	Type:          26,
		//	TypeString:    "restore_footprint",
		//	SourceAccount: hardCodedSourceAccountAddress,
		//	TransactionID: 4096,
		//	OperationID:   4133,
		//	OperationDetails: map[string]interface{}{
		//		"type":               "restore_footprint",
		//		"contract_id":        "",
		//		"contract_code_hash": "",
		//	},
		//	ClosedAt: hardCodedLedgerClose,
		//},
	}
	return
}
