package transform

import (
	"encoding/base64"
	"fmt"
	"io"
	"testing"

	ingestio "github.com/stellar/go/exp/ingest/io"
	"github.com/stellar/go/exp/ingest/ledgerbackend"
	"github.com/stellar/go/network"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/internal/utils"
	"github.com/stretchr/testify/assert"
)

func createTestArchiveBackend() *ledgerbackend.HistoryArchiveBackend {
	archiveStellarURL := "http://history.stellar.org/prd/core-live/core_live_001"
	backend, err := ledgerbackend.NewHistoryArchiveBackendFromURL(archiveStellarURL)
	utils.PanicOnError(err)
	return backend
}
func TestLive(t *testing.T) {
	/*
		21585418 for account creation; 21585418 for payment; 22490180 for path receive
		22490168 for manage sell (2nd one); 22490175 for create passive; 29353599 for set options
		22490190 for change trust; 30659243 for manage buy (4th one); 30540921 for path send
	*/
	sequenceNumber := uint32(30540921)
	backend := createTestArchiveBackend()
	txReader, err := ingestio.NewLedgerTransactionReader(backend, network.PublicNetworkPassphrase, sequenceNumber)
	utils.PanicOnError(err)
	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		utils.PanicOnError(err)
		envelope := tx.Envelope
		for index, op := range envelope.Operations() {
			if op.Body.Type == xdr.OperationTypePathPaymentStrictSend && tx.Result.Successful() {
				fmt.Println(op)
			}
			convertedOp, err := TransformOperation(op, int32(index), tx)
			utils.PanicOnError(err)
			fmt.Println(convertedOp.Type)
		}
	}
}

func TestTransformOperation(t *testing.T) {
	type operationInput struct {
		operation   xdr.Operation
		index       int32
		transaction ingestio.LedgerTransaction
	}
	type transformTest struct {
		input      operationInput
		wantOutput OperationOutput
		wantErr    error
	}
	genericInput := operationInput{genericOperation, 1, genericLedgerTransaction}

	negativeOpTypeInput := genericInput
	negativeOpTypeEnvelope := genericEnvelope
	negativeOpTypeEnvelope.Tx.Operations[0].Body.Type = xdr.OperationType(-1)
	negativeOpTypeInput.operation.Body.Type = xdr.OperationType(-1)
	negativeOpTypeInput.transaction.Envelope.V1 = &negativeOpTypeEnvelope

	unknownOpTypeInput := genericInput
	unknownOpTypeEnvelope := genericEnvelope
	unknownOpTypeEnvelope.Tx.Operations[0].Body.Type = xdr.OperationType(20)
	unknownOpTypeInput.operation.Body.Type = xdr.OperationType(20)
	unknownOpTypeInput.transaction.Envelope.V1 = &unknownOpTypeEnvelope

	tests := []transformTest{
		{
			negativeOpTypeInput,
			OperationOutput{},
			fmt.Errorf("The operation type (-1) is negative for  operation 1"),
		},
		{
			unknownOpTypeInput,
			OperationOutput{},
			fmt.Errorf("Unknown operation type: "),
		},
	}
	hardCodedInputTransaction, err := prepareHardcodedOperationTestInput()
	assert.NoError(t, err)
	hardCodedOutputArray := prepareHardcodedOperationTestOutputs()

	for i, op := range hardCodedInputTransaction.Envelope.Operations() {
		tests = append(tests, transformTest{
			input:      operationInput{op, int32(i), hardCodedInputTransaction},
			wantOutput: hardCodedOutputArray[i],
			wantErr:    nil,
		})
	}

	for _, test := range tests {
		actualOutput, actualError := TransformOperation(test.input.operation, test.input.index, test.input.transaction)
		assert.Equal(t, test.wantErr, actualError)
		assert.Equal(t, test.wantOutput, actualOutput)
	}
}

//creates a single transaction that contains one of every operation type
func prepareHardcodedOperationTestInput() (inputTransaction ingestio.LedgerTransaction, err error) {
	inputTransaction = genericLedgerTransaction
	inputEnvelope := genericEnvelope
	hardCodedSourceAccount, err := xdr.NewMuxedAccount(xdr.CryptoKeyTypeKeyTypeEd25519, xdr.Uint256([32]byte{0x67, 0xcc, 0x0, 0x86, 0x4c, 0x3b, 0x89, 0x16, 0x8c, 0x6a, 0xaf, 0xe0, 0xbc, 0x34, 0x70, 0x9e, 0xd0, 0x21, 0xc5, 0x5, 0x72, 0xe2, 0xf9, 0x88, 0x61, 0x34, 0x22, 0x8, 0x2c, 0x22, 0x29, 0x72}))
	if err != nil {
		return
	}

	hardCodedDestAccount, err := xdr.NewMuxedAccount(xdr.CryptoKeyTypeKeyTypeEd25519, xdr.Uint256([32]byte{0x6b, 0x58, 0xdd, 0x6c, 0x68, 0x93, 0xb, 0x6d, 0x3, 0x1d, 0xc5, 0xbb, 0xfa, 0xe2, 0x3e, 0xfa, 0x1e, 0xc3, 0xf0, 0xbb, 0x58, 0xc2, 0xbc, 0x8d, 0x93, 0x8d, 0x47, 0xc1, 0xdf, 0xb3, 0xbe, 0x73}))
	if err != nil {
		return
	}

	inputEnvelope.Tx.SourceAccount = hardCodedSourceAccount
	hardCodedAsset := xdr.Asset{
		Type: xdr.AssetTypeAssetTypeCreditAlphanum4,
		AlphaNum4: &xdr.AssetAlphaNum4{
			AssetCode: xdr.AssetCode4([4]byte{0x55, 0x53, 0x44, 0x54}),
			Issuer:    hardCodedDestAccount.ToAccountId(),
		},
	}
	hardCodedNativeAsset := xdr.MustNewNativeAsset()
	hardCodedInflationDest := hardCodedDestAccount.ToAccountId()

	hardCodedTrustAsset, err := hardCodedAsset.ToAllowTrustOpAsset("USDT")
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

	hardCodedDataValue := xdr.DataValue([]byte{0x76, 0x61, 0x6c, 0x75, 0x65})
	hardCodedSequenceNumber := xdr.SequenceNumber(100)
	inputOperations := []xdr.Operation{
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeCreateAccount,
				CreateAccountOp: &xdr.CreateAccountOp{
					StartingBalance: 25000000,
					Destination:     hardCodedDestAccount.ToAccountId(),
				},
			},
		},
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypePayment,
				PaymentOp: &xdr.PaymentOp{
					Destination: hardCodedDestAccount,
					Asset:       hardCodedAsset,
					Amount:      350000000,
				},
			},
		},
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypePayment,
				PaymentOp: &xdr.PaymentOp{
					Destination: hardCodedDestAccount,
					Asset:       hardCodedNativeAsset,
					Amount:      350000000,
				},
			},
		},
		xdr.Operation{
			SourceAccount: &hardCodedSourceAccount,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypePathPaymentStrictReceive,
				PathPaymentStrictReceiveOp: &xdr.PathPaymentStrictReceiveOp{
					SendAsset:   hardCodedNativeAsset,
					SendMax:     8951495900,
					Destination: hardCodedDestAccount,
					DestAsset:   hardCodedNativeAsset,
					DestAmount:  8951495900,
					Path:        []xdr.Asset{hardCodedAsset},
				},
			},
		},
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeManageSellOffer,
				ManageSellOfferOp: &xdr.ManageSellOfferOp{
					Selling: hardCodedAsset,
					Buying:  hardCodedNativeAsset,
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
					Selling: hardCodedNativeAsset,
					Buying:  hardCodedAsset,
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
					Line:  hardCodedAsset,
					Limit: xdr.Int64(500000000000000000),
				},
			},
		},
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeAllowTrust,
				AllowTrustOp: &xdr.AllowTrustOp{
					Trustor:   hardCodedSourceAccount.ToAccountId(),
					Asset:     hardCodedTrustAsset,
					Authorize: xdr.Uint32(0),
				},
			},
		},
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type:        xdr.OperationTypeAccountMerge,
				Destination: &hardCodedDestAccount,
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
					Selling:   hardCodedAsset,
					Buying:    hardCodedNativeAsset,
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
					SendAsset:   hardCodedNativeAsset,
					SendAmount:  1598182,
					Destination: hardCodedDestAccount,
					DestAsset:   hardCodedNativeAsset,
					DestMin:     4280460538,
					Path:        []xdr.Asset{hardCodedAsset},
				},
			},
		},
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypePathPaymentStrictSend,
				PathPaymentStrictSendOp: &xdr.PathPaymentStrictSendOp{
					SendAsset:   hardCodedNativeAsset,
					SendAmount:  1598182,
					Destination: hardCodedDestAccount,
					DestAsset:   hardCodedNativeAsset,
					DestMin:     4280460538,
					Path:        nil,
				},
			},
		},
	}
	inputEnvelope.Tx.Operations = inputOperations
	results := []xdr.OperationResult{
		xdr.OperationResult{},
		xdr.OperationResult{},
		xdr.OperationResult{},
		//need a true result for path payment receive
		xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypePathPaymentStrictReceive,
				PathPaymentStrictReceiveResult: &xdr.PathPaymentStrictReceiveResult{
					Code: xdr.PathPaymentStrictReceiveResultCodePathPaymentStrictReceiveSuccess,
					Success: &xdr.PathPaymentStrictReceiveResultSuccess{
						Last: xdr.SimplePaymentResult{Amount: 8951495900},
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
		xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypePathPaymentStrictSend,
				PathPaymentStrictSendResult: &xdr.PathPaymentStrictSendResult{
					Code: xdr.PathPaymentStrictSendResultCodePathPaymentStrictSendSuccess,
					Success: &xdr.PathPaymentStrictSendResultSuccess{
						Last: xdr.SimplePaymentResult{Amount: 4280460538},
					},
				},
			},
		},
	}
	inputTransaction.Result.Result.Result.Results = &results
	inputTransaction.Envelope.V1 = &inputEnvelope
	return
}

func prepareHardcodedOperationTestOutputs() (transformedOperations []OperationOutput) {
	hardCodedSourceAccountAddress := "GBT4YAEGJQ5YSFUMNKX6BPBUOCPNAIOFAVZOF6MIME2CECBMEIUXFZZN"
	hardCodedDestAccountAddress := "GBVVRXLMNCJQW3IDDXC3X6XCH35B5Q7QXNMMFPENSOGUPQO7WO7HGZPA"
	hardCodedAssetOutput := AssetOutput{
		AssetType:   "credit_alphanum4",
		AssetCode:   "USDT",
		AssetIssuer: hardCodedDestAccountAddress,
	}
	transformedOperations = []OperationOutput{
		OperationOutput{
			SourceAccount:    hardCodedSourceAccountAddress,
			Type:             0,
			ApplicationOrder: 1,
			OperationDetails: Details{
				Account:         hardCodedDestAccountAddress,
				Funder:          hardCodedSourceAccountAddress,
				StartingBalance: 2.5,
			},
		},
		OperationOutput{
			Type:             1,
			ApplicationOrder: 2,
			SourceAccount:    hardCodedSourceAccountAddress,

			OperationDetails: Details{
				From:        hardCodedSourceAccountAddress,
				To:          hardCodedDestAccountAddress,
				Amount:      35,
				AssetCode:   "USDT",
				AssetType:   "credit_alphanum4",
				AssetIssuer: hardCodedDestAccountAddress,
			},
		},
		OperationOutput{
			Type:             1,
			ApplicationOrder: 3,
			SourceAccount:    hardCodedSourceAccountAddress,

			OperationDetails: Details{
				From:      hardCodedSourceAccountAddress,
				To:        hardCodedDestAccountAddress,
				Amount:    35,
				AssetType: "native",
			},
		},
		OperationOutput{
			Type:             2,
			ApplicationOrder: 4,
			SourceAccount:    hardCodedSourceAccountAddress,

			OperationDetails: Details{
				From:         hardCodedSourceAccountAddress,
				To:           hardCodedDestAccountAddress,
				SourceAmount: 894.6764349,
				SourceMax:    895.14959,
				Amount:       895.14959,
				Path:         []AssetOutput{hardCodedAssetOutput},
			},
		},
		OperationOutput{
			Type:             3,
			ApplicationOrder: 5,
			SourceAccount:    hardCodedSourceAccountAddress,
			OperationDetails: Details{
				Price:  0.514092,
				Amount: 76.586,
				PriceR: Price{
					Numerator:   128523,
					Denominator: 250000,
				},
				SellingAssetCode:   "USDT",
				SellingAssetType:   "credit_alphanum4",
				SellingAssetIssuer: hardCodedDestAccountAddress,
				BuyingAssetType:    "native",
			},
		},
		OperationOutput{
			Type:             4,
			ApplicationOrder: 6,
			SourceAccount:    hardCodedSourceAccountAddress,
			OperationDetails: Details{
				Amount: 63.1595,
				Price:  0.0791606,
				PriceR: Price{
					Numerator:   99583200,
					Denominator: 1257990000,
				},
				BuyingAssetCode:   "USDT",
				BuyingAssetType:   "credit_alphanum4",
				BuyingAssetIssuer: hardCodedDestAccountAddress,
				SellingAssetType:  "native",
			},
		},
		OperationOutput{
			Type:             5,
			ApplicationOrder: 7,
			SourceAccount:    hardCodedSourceAccountAddress,
			OperationDetails: Details{
				InflationDest:   hardCodedDestAccountAddress,
				ClearFlags:      []int32{1, 2},
				SetFlags:        []int32{4},
				MasterKeyWeight: 3,
				LowThreshold:    1,
				MedThreshold:    3,
				HighThreshold:   5,
				HomeDomain:      "2019=DRA;n-test",
				SignerKey:       "A",
				SignerWeight:    1,
			},
		},
		OperationOutput{
			Type:             6,
			ApplicationOrder: 8,
			SourceAccount:    hardCodedSourceAccountAddress,
			OperationDetails: Details{
				Trustor:     hardCodedSourceAccountAddress,
				Trustee:     hardCodedDestAccountAddress,
				Limit:       50000000000,
				AssetCode:   "USDT",
				AssetType:   "credit_alphanum4",
				AssetIssuer: hardCodedDestAccountAddress,
			},
		},
		OperationOutput{
			Type:             7,
			ApplicationOrder: 9,
			SourceAccount:    hardCodedSourceAccountAddress,
			OperationDetails: Details{
				Trustee:     hardCodedSourceAccountAddress,
				Trustor:     hardCodedDestAccountAddress,
				Authorize:   false,
				AssetCode:   "USDT",
				AssetType:   "credit_alphanum4",
				AssetIssuer: hardCodedDestAccountAddress,
			},
		},
		OperationOutput{
			Type:             8,
			ApplicationOrder: 10,
			SourceAccount:    hardCodedSourceAccountAddress,
			OperationDetails: Details{
				Account: hardCodedSourceAccountAddress,
				Into:    hardCodedDestAccountAddress,
			},
		},
		OperationOutput{
			Type:             9,
			ApplicationOrder: 11,
			SourceAccount:    hardCodedSourceAccountAddress,
			OperationDetails: Details{},
		},
		OperationOutput{
			Type:             10,
			ApplicationOrder: 12,
			SourceAccount:    hardCodedSourceAccountAddress,
			OperationDetails: Details{
				Name:  "test",
				Value: base64.StdEncoding.EncodeToString([]byte{0x76, 0x61, 0x6c, 0x75, 0x65}),
			},
		},
		OperationOutput{
			Type:             11,
			ApplicationOrder: 13,
			SourceAccount:    hardCodedSourceAccountAddress,
			OperationDetails: Details{
				BumpTo: "100",
			},
		},
		OperationOutput{
			Type:             12,
			ApplicationOrder: 14,
			SourceAccount:    hardCodedSourceAccountAddress,
			OperationDetails: Details{
				Price:  0.34968230309,
				Amount: 765.4501001,
				PriceR: Price{
					Numerator:   635863285,
					Denominator: 1818402817,
				},
				SellingAssetCode:   "USDT",
				SellingAssetType:   "credit_alphanum4",
				SellingAssetIssuer: hardCodedDestAccountAddress,
				BuyingAssetType:    "native",
				OfferID:            100,
			},
		},
		OperationOutput{
			Type:             13,
			ApplicationOrder: 15,
			SourceAccount:    hardCodedSourceAccountAddress,
			OperationDetails: Details{
				From:            hardCodedSourceAccountAddress,
				To:              hardCodedDestAccountAddress,
				SourceAmount:    0.1598182,
				DestinationMin:  "428.0460538",
				Amount:          433.4043858,
				Path:            []AssetOutput{hardCodedAssetOutput},
				SourceAssetType: "native",
				AssetType:       "native",
			},
		},
		OperationOutput{
			Type:             13,
			ApplicationOrder: 16,
			SourceAccount:    hardCodedSourceAccountAddress,
			OperationDetails: Details{
				From:            hardCodedSourceAccountAddress,
				To:              hardCodedDestAccountAddress,
				SourceAmount:    0.1598182,
				DestinationMin:  "428.0460538",
				Amount:          428.0460538,
				Path:            nil,
				SourceAssetType: "native",
				AssetType:       "native",
			},
		},
	}
	return
}
