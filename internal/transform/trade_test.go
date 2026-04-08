package transform

import (
	"fmt"
	"testing"
	"time"

	"github.com/guregu/null"
	"github.com/stretchr/testify/assert"

	"github.com/stellar/stellar-etl/v2/internal/utils"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

func TestTransformTrade(t *testing.T) {
	type tradeInput struct {
		index       int32
		transaction ingest.LedgerTransaction
		closeTime   time.Time
	}
	type transformTest struct {
		input      tradeInput
		wantOutput []TradeOutput
		wantErr    error
	}

	hardCodedInputTransaction := makeTradeTestInput()
	hardCodedOutputArray := makeTradeTestOutput()

	genericInput := tradeInput{
		index:       0,
		transaction: genericLedgerTransaction,
		closeTime:   genericCloseTime,
	}

	wrongTypeInput := genericInput
	wrongTypeInput.transaction = ingest.LedgerTransaction{
		Index: 1,
		Envelope: xdr.TransactionEnvelope{
			Type: xdr.EnvelopeTypeEnvelopeTypeTx,
			V1: &xdr.TransactionV1Envelope{
				Tx: xdr.Transaction{
					SourceAccount: genericSourceAccount,
					Memo:          xdr.Memo{},
					Operations: []xdr.Operation{
						genericBumpOperation,
					},
				},
			},
		},
		Result: utils.CreateSampleResultMeta(true, 1).Result,
	}

	resultOutOfRangeInput := genericInput
	resultOutOfRangeEnvelope := genericManageBuyOfferEnvelope
	resultOutOfRangeInput.transaction.Envelope.V1 = &resultOutOfRangeEnvelope
	resultOutOfRangeInput.transaction.Result = wrapOperationsResultsSlice([]xdr.OperationResult{}, true)

	failedTxInput := genericInput
	failedTxInput.transaction.Result = wrapOperationsResultsSlice([]xdr.OperationResult{}, false)

	noTrInput := genericInput
	noTrEnvelope := genericManageBuyOfferEnvelope
	noTrInput.transaction.Envelope.V1 = &noTrEnvelope
	noTrInput.transaction.Result = wrapOperationsResultsSlice([]xdr.OperationResult{
		{Tr: nil},
	}, true)

	failedResultInput := genericInput
	failedResultEnvelope := genericManageBuyOfferEnvelope
	failedResultInput.transaction.Envelope.V1 = &failedResultEnvelope
	failedResultInput.transaction.Result = wrapOperationsResultsSlice([]xdr.OperationResult{
		{
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeManageBuyOffer,
				ManageBuyOfferResult: &xdr.ManageBuyOfferResult{
					Code: xdr.ManageBuyOfferResultCodeManageBuyOfferMalformed,
				},
			}},
	}, true)

	negBaseAmountInput := genericInput
	negBaseAmountEnvelope := genericManageBuyOfferEnvelope
	negBaseAmountInput.transaction.Envelope.V1 = &negBaseAmountEnvelope
	negBaseAmountInput.transaction.Result = wrapOperationsResultsSlice([]xdr.OperationResult{
		{
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeManageBuyOffer,
				ManageBuyOfferResult: &xdr.ManageBuyOfferResult{
					Code: xdr.ManageBuyOfferResultCodeManageBuyOfferSuccess,
					Success: &xdr.ManageOfferSuccessResult{
						OffersClaimed: []xdr.ClaimAtom{
							{
								Type: xdr.ClaimAtomTypeClaimAtomTypeOrderBook,
								OrderBook: &xdr.ClaimOfferAtom{
									SellerId:   genericAccountID,
									AmountSold: -1,
								},
							},
						},
					},
				},
			}},
	}, true)

	negCounterAmountInput := genericInput
	negCounterAmountEnvelope := genericManageBuyOfferEnvelope
	negCounterAmountInput.transaction.Envelope.V1 = &negCounterAmountEnvelope
	negCounterAmountInput.transaction.Result = wrapOperationsResultsSlice([]xdr.OperationResult{
		{
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeManageBuyOffer,
				ManageBuyOfferResult: &xdr.ManageBuyOfferResult{
					Code: xdr.ManageBuyOfferResultCodeManageBuyOfferSuccess,
					Success: &xdr.ManageOfferSuccessResult{
						OffersClaimed: []xdr.ClaimAtom{
							{
								Type: xdr.ClaimAtomTypeClaimAtomTypeOrderBook,
								OrderBook: &xdr.ClaimOfferAtom{
									SellerId:     genericAccountID,
									AmountBought: -2,
								},
							},
						},
					},
				},
			}},
	}, true)

	tests := []transformTest{
		{
			wrongTypeInput,
			[]TradeOutput{}, fmt.Errorf("operation of type OperationTypeBumpSequence at index 0 does not result in trades"),
		},
		{
			resultOutOfRangeInput,
			[]TradeOutput{}, fmt.Errorf("operation index of 0 is out of bounds in result slice (len = 0)"),
		},
		{
			failedTxInput,
			[]TradeOutput{}, fmt.Errorf("transaction failed; no trades"),
		},
		{
			noTrInput,
			[]TradeOutput{}, fmt.Errorf("could not get result Tr for operation at index 0"),
		},
		{
			failedResultInput,
			[]TradeOutput{}, fmt.Errorf("could not get ManageOfferSuccess for operation at index 0"),
		},
		{
			negBaseAmountInput,
			[]TradeOutput{}, fmt.Errorf("amount sold is negative (-1) for operation at index 0"),
		},
		{
			negCounterAmountInput,
			[]TradeOutput{}, fmt.Errorf("amount bought is negative (-2) for operation at index 0"),
		},
	}

	for i := range hardCodedInputTransaction.Envelope.Operations() {
		tests = append(tests, transformTest{
			input:      tradeInput{index: int32(i), transaction: hardCodedInputTransaction, closeTime: genericCloseTime},
			wantOutput: hardCodedOutputArray[i],
			wantErr:    nil,
		})
	}

	for _, test := range tests {
		actualOutput, actualError := TransformTrade(test.input.index, 100, test.input.transaction, test.input.closeTime)
		assert.Equal(t, test.wantErr, actualError)
		assert.Equal(t, test.wantOutput, actualOutput)
	}
}

func wrapOperationsResultsSlice(results []xdr.OperationResult, successful bool) xdr.TransactionResultPair {
	resultCode := xdr.TransactionResultCodeTxFailed
	if successful {
		resultCode = xdr.TransactionResultCodeTxSuccess
	}
	return xdr.TransactionResultPair{
		Result: xdr.TransactionResult{
			Result: xdr.TransactionResultResult{
				Code:    resultCode,
				Results: &results,
			},
		},
	}
}

func makeTradeTestInput() (inputTransaction ingest.LedgerTransaction) {
	inputTransaction = genericLedgerTransaction
	inputEnvelope := genericBumpOperationEnvelope

	inputEnvelope.Tx.SourceAccount = testAccount3
	offerOne := xdr.ClaimAtom{
		Type: xdr.ClaimAtomTypeClaimAtomTypeOrderBook,
		OrderBook: &xdr.ClaimOfferAtom{
			SellerId:     testAccount1ID,
			OfferId:      97684906,
			AssetSold:    ethAsset,
			AssetBought:  usdtAsset,
			AmountSold:   13300347,
			AmountBought: 12634,
		},
	}
	offerTwo := xdr.ClaimAtom{
		Type: xdr.ClaimAtomTypeClaimAtomTypeOrderBook,
		OrderBook: &xdr.ClaimOfferAtom{
			SellerId:     testAccount3ID,
			OfferId:      86106895,
			AssetSold:    usdtAsset,
			AssetBought:  nativeAsset,
			AmountSold:   500,
			AmountBought: 20,
		},
	}
	lPOne := xdr.ClaimAtom{
		Type: xdr.ClaimAtomTypeClaimAtomTypeLiquidityPool,
		LiquidityPool: &xdr.ClaimLiquidityAtom{
			LiquidityPoolId: xdr.PoolId{4, 5, 6},
			AssetSold:       xdr.MustNewCreditAsset("WER", testAccount4Address),
			AmountSold:      123,
			AssetBought:     xdr.MustNewCreditAsset("NIJ", testAccount1Address),
			AmountBought:    456,
		},
	}

	lPTwo := xdr.ClaimAtom{
		Type: xdr.ClaimAtomTypeClaimAtomTypeLiquidityPool,
		LiquidityPool: &xdr.ClaimLiquidityAtom{
			LiquidityPoolId: xdr.PoolId{1, 2, 3, 4, 5, 6},
			AssetSold:       xdr.MustNewCreditAsset("HAH", testAccount1Address),
			AmountSold:      1,
			AssetBought:     xdr.MustNewCreditAsset("WHO", testAccount4Address),
			AmountBought:    1,
		},
	}

	inputOperations := []xdr.Operation{
		{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type:              xdr.OperationTypeManageSellOffer,
				ManageSellOfferOp: &xdr.ManageSellOfferOp{},
			},
		},
		{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type:             xdr.OperationTypeManageBuyOffer,
				ManageBuyOfferOp: &xdr.ManageBuyOfferOp{},
			},
		},
		{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypePathPaymentStrictSend,
				PathPaymentStrictSendOp: &xdr.PathPaymentStrictSendOp{
					Destination: testAccount1,
				},
			},
		},
		{
			SourceAccount: &testAccount3,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypePathPaymentStrictReceive,
				PathPaymentStrictReceiveOp: &xdr.PathPaymentStrictReceiveOp{
					Destination: testAccount1,
				},
			},
		},
		{
			SourceAccount: &testAccount3,
			Body: xdr.OperationBody{
				Type:                    xdr.OperationTypePathPaymentStrictSend,
				PathPaymentStrictSendOp: &xdr.PathPaymentStrictSendOp{},
			},
		},
		{
			SourceAccount: &testAccount3,
			Body: xdr.OperationBody{
				Type:                       xdr.OperationTypePathPaymentStrictReceive,
				PathPaymentStrictReceiveOp: &xdr.PathPaymentStrictReceiveOp{},
			},
		},
		{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type:                     xdr.OperationTypeCreatePassiveSellOffer,
				CreatePassiveSellOfferOp: &xdr.CreatePassiveSellOfferOp{},
			},
		},
	}
	inputEnvelope.Tx.Operations = inputOperations
	results := []xdr.OperationResult{
		{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeManageSellOffer,
				ManageSellOfferResult: &xdr.ManageSellOfferResult{
					Code: xdr.ManageSellOfferResultCodeManageSellOfferSuccess,
					Success: &xdr.ManageOfferSuccessResult{
						OffersClaimed: []xdr.ClaimAtom{
							offerOne,
						},
					},
				},
			},
		},

		{
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeManageBuyOffer,
				ManageBuyOfferResult: &xdr.ManageBuyOfferResult{
					Code: xdr.ManageBuyOfferResultCodeManageBuyOfferSuccess,
					Success: &xdr.ManageOfferSuccessResult{
						OffersClaimed: []xdr.ClaimAtom{
							offerTwo,
						},
					},
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
						Offers: []xdr.ClaimAtom{
							offerOne, offerTwo,
						},
					},
				},
			},
		},
		{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypePathPaymentStrictReceive,
				PathPaymentStrictReceiveResult: &xdr.PathPaymentStrictReceiveResult{
					Code: xdr.PathPaymentStrictReceiveResultCodePathPaymentStrictReceiveSuccess,
					Success: &xdr.PathPaymentStrictReceiveResultSuccess{
						Offers: []xdr.ClaimAtom{
							offerTwo, offerOne,
						},
					},
				},
			},
		},
		{
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypePathPaymentStrictSend,
				PathPaymentStrictSendResult: &xdr.PathPaymentStrictSendResult{
					Code: xdr.PathPaymentStrictSendResultCodePathPaymentStrictSendSuccess,
					Success: &xdr.PathPaymentStrictSendResultSuccess{
						Offers: []xdr.ClaimAtom{
							lPOne,
						},
					},
				},
			},
		},
		{
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypePathPaymentStrictReceive,
				PathPaymentStrictReceiveResult: &xdr.PathPaymentStrictReceiveResult{
					Code: xdr.PathPaymentStrictReceiveResultCodePathPaymentStrictReceiveSuccess,
					Success: &xdr.PathPaymentStrictReceiveResultSuccess{
						Offers: []xdr.ClaimAtom{
							lPTwo,
						},
					},
				},
			},
		},
		{
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeCreatePassiveSellOffer,
				CreatePassiveSellOfferResult: &xdr.ManageSellOfferResult{
					Code: xdr.ManageSellOfferResultCodeManageSellOfferSuccess,
					Success: &xdr.ManageOfferSuccessResult{
						OffersClaimed: []xdr.ClaimAtom{},
					},
				},
			},
		},
	}

	unsafeMeta := xdr.TransactionMetaV1{
		Operations: []xdr.OperationMeta{
			{
				Changes: xdr.LedgerEntryChanges{
					xdr.LedgerEntryChange{
						Type: xdr.LedgerEntryChangeTypeLedgerEntryState,
						State: &xdr.LedgerEntry{
							Data: xdr.LedgerEntryData{
								Type: xdr.LedgerEntryTypeOffer,
								Offer: &xdr.OfferEntry{
									SellerId: testAccount1ID,
									OfferId:  97684906,
									Price: xdr.Price{
										N: 12634,
										D: 13300347,
									},
								},
							},
						},
					},
					xdr.LedgerEntryChange{
						Type: xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
						Updated: &xdr.LedgerEntry{
							Data: xdr.LedgerEntryData{
								Type: xdr.LedgerEntryTypeOffer,
								Offer: &xdr.OfferEntry{
									SellerId: testAccount1ID,
									OfferId:  97684906,
									Price: xdr.Price{
										N: 2,
										D: 4,
									},
								},
							},
						},
					},
				},
			},
			{
				Changes: xdr.LedgerEntryChanges{
					xdr.LedgerEntryChange{
						Type: xdr.LedgerEntryChangeTypeLedgerEntryState,
						State: &xdr.LedgerEntry{
							Data: xdr.LedgerEntryData{
								Type: xdr.LedgerEntryTypeOffer,
								Offer: &xdr.OfferEntry{
									SellerId: testAccount3ID,
									OfferId:  86106895,
									Price: xdr.Price{
										N: 25,
										D: 1,
									},
								},
							},
						},
					},
					xdr.LedgerEntryChange{
						Type: xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
						Updated: &xdr.LedgerEntry{
							Data: xdr.LedgerEntryData{
								Type: xdr.LedgerEntryTypeOffer,
								Offer: &xdr.OfferEntry{
									SellerId: testAccount3ID,
									OfferId:  86106895,
									Price: xdr.Price{
										N: 1111,
										D: 12,
									},
								},
							},
						},
					},
				},
			},
			{
				Changes: xdr.LedgerEntryChanges{
					xdr.LedgerEntryChange{
						Type: xdr.LedgerEntryChangeTypeLedgerEntryState,
						State: &xdr.LedgerEntry{
							Data: xdr.LedgerEntryData{
								Type: xdr.LedgerEntryTypeOffer,
								Offer: &xdr.OfferEntry{
									SellerId: testAccount1ID,
									OfferId:  97684906,
									Price: xdr.Price{
										N: 12634,
										D: 13300347,
									},
								},
							},
						},
					},
					xdr.LedgerEntryChange{
						Type: xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
						Updated: &xdr.LedgerEntry{
							Data: xdr.LedgerEntryData{
								Type: xdr.LedgerEntryTypeOffer,
								Offer: &xdr.OfferEntry{
									SellerId: testAccount1ID,
									OfferId:  97684906,
									Price: xdr.Price{
										N: 1111,
										D: 12,
									},
								},
							},
						},
					},
					xdr.LedgerEntryChange{
						Type: xdr.LedgerEntryChangeTypeLedgerEntryState,
						State: &xdr.LedgerEntry{
							Data: xdr.LedgerEntryData{
								Type: xdr.LedgerEntryTypeOffer,
								Offer: &xdr.OfferEntry{
									SellerId: testAccount3ID,
									OfferId:  86106895,
									Price: xdr.Price{
										N: 20,
										D: 500,
									},
								},
							},
						},
					},
					xdr.LedgerEntryChange{
						Type: xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
						Updated: &xdr.LedgerEntry{
							Data: xdr.LedgerEntryData{
								Type: xdr.LedgerEntryTypeOffer,
								Offer: &xdr.OfferEntry{
									SellerId: testAccount3ID,
									OfferId:  86106895,
									Price: xdr.Price{
										N: 1111,
										D: 12,
									},
								},
							},
						},
					},
				},
			},
			{
				Changes: xdr.LedgerEntryChanges{
					xdr.LedgerEntryChange{
						Type: xdr.LedgerEntryChangeTypeLedgerEntryState,
						State: &xdr.LedgerEntry{
							Data: xdr.LedgerEntryData{
								Type: xdr.LedgerEntryTypeOffer,
								Offer: &xdr.OfferEntry{
									SellerId: testAccount3ID,
									OfferId:  86106895,
									Price: xdr.Price{
										N: 20,
										D: 500,
									},
								},
							},
						},
					},
					xdr.LedgerEntryChange{
						Type: xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
						Updated: &xdr.LedgerEntry{
							Data: xdr.LedgerEntryData{
								Type: xdr.LedgerEntryTypeOffer,
								Offer: &xdr.OfferEntry{
									SellerId: testAccount1ID,
									OfferId:  97684906,
									Price: xdr.Price{
										N: 12634,
										D: 13300347,
									},
								},
							},
						},
					},
					xdr.LedgerEntryChange{
						Type: xdr.LedgerEntryChangeTypeLedgerEntryState,
						State: &xdr.LedgerEntry{
							Data: xdr.LedgerEntryData{
								Type: xdr.LedgerEntryTypeOffer,
								Offer: &xdr.OfferEntry{
									SellerId: testAccount1ID,
									OfferId:  97684906,
									Price: xdr.Price{
										N: 12634,
										D: 13300347,
									},
								},
							},
						},
					},
					xdr.LedgerEntryChange{
						Type: xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
						Updated: &xdr.LedgerEntry{
							Data: xdr.LedgerEntryData{
								Type: xdr.LedgerEntryTypeOffer,
								Offer: &xdr.OfferEntry{
									SellerId: testAccount1ID,
									Price: xdr.Price{
										N: 12634,
										D: 1330,
									},
								},
							},
						},
					},
				},
			},
			{
				Changes: xdr.LedgerEntryChanges{
					xdr.LedgerEntryChange{
						Type: xdr.LedgerEntryChangeTypeLedgerEntryState,
						State: &xdr.LedgerEntry{
							Data: xdr.LedgerEntryData{
								Type: xdr.LedgerEntryTypeLiquidityPool,
								LiquidityPool: &xdr.LiquidityPoolEntry{
									LiquidityPoolId: xdr.PoolId{4, 5, 6},
									Body: xdr.LiquidityPoolEntryBody{
										Type: xdr.LiquidityPoolTypeLiquidityPoolConstantProduct,
										ConstantProduct: &xdr.LiquidityPoolEntryConstantProduct{
											Params: xdr.LiquidityPoolConstantProductParameters{
												AssetA: xdr.MustNewCreditAsset("NIJ", testAccount1Address),
												AssetB: xdr.MustNewCreditAsset("WER", testAccount4Address),
												Fee:    xdr.LiquidityPoolFeeV18,
											},
											ReserveA:                 400,
											ReserveB:                 800,
											TotalPoolShares:          40,
											PoolSharesTrustLineCount: 50,
										},
									},
								},
							},
						},
					},
					xdr.LedgerEntryChange{
						Type: xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
						Updated: &xdr.LedgerEntry{
							Data: xdr.LedgerEntryData{
								Type: xdr.LedgerEntryTypeLiquidityPool,
								LiquidityPool: &xdr.LiquidityPoolEntry{
									LiquidityPoolId: xdr.PoolId{4, 5, 6},
									Body: xdr.LiquidityPoolEntryBody{
										Type: xdr.LiquidityPoolTypeLiquidityPoolConstantProduct,
										ConstantProduct: &xdr.LiquidityPoolEntryConstantProduct{
											Params: xdr.LiquidityPoolConstantProductParameters{
												AssetA: xdr.MustNewCreditAsset("NIJ", testAccount1Address),
												AssetB: xdr.MustNewCreditAsset("WER", testAccount4Address),
												Fee:    xdr.LiquidityPoolFeeV18,
											},
											ReserveA:                 500,
											ReserveB:                 750,
											TotalPoolShares:          40,
											PoolSharesTrustLineCount: 50,
										},
									},
								},
							},
						},
					},
				},
			},
			{
				Changes: xdr.LedgerEntryChanges{
					xdr.LedgerEntryChange{
						Type: xdr.LedgerEntryChangeTypeLedgerEntryState,
						State: &xdr.LedgerEntry{
							Data: xdr.LedgerEntryData{
								Type: xdr.LedgerEntryTypeLiquidityPool,
								LiquidityPool: &xdr.LiquidityPoolEntry{
									LiquidityPoolId: xdr.PoolId{1, 2, 3, 4, 5, 6},
									Body: xdr.LiquidityPoolEntryBody{
										Type: xdr.LiquidityPoolTypeLiquidityPoolConstantProduct,
										ConstantProduct: &xdr.LiquidityPoolEntryConstantProduct{
											Params: xdr.LiquidityPoolConstantProductParameters{
												AssetA: xdr.MustNewCreditAsset("HAH", testAccount4Address),
												AssetB: xdr.MustNewCreditAsset("WHO", testAccount1Address),
												Fee:    xdr.LiquidityPoolFeeV18,
											},
											ReserveA:                 100000,
											ReserveB:                 10000,
											TotalPoolShares:          40,
											PoolSharesTrustLineCount: 50,
										},
									},
								},
							},
						},
					},
					xdr.LedgerEntryChange{
						Type: xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
						Updated: &xdr.LedgerEntry{
							Data: xdr.LedgerEntryData{
								Type: xdr.LedgerEntryTypeLiquidityPool,
								LiquidityPool: &xdr.LiquidityPoolEntry{
									LiquidityPoolId: xdr.PoolId{4, 5, 6},
									Body: xdr.LiquidityPoolEntryBody{
										Type: xdr.LiquidityPoolTypeLiquidityPoolConstantProduct,
										ConstantProduct: &xdr.LiquidityPoolEntryConstantProduct{
											Params: xdr.LiquidityPoolConstantProductParameters{
												AssetA: xdr.MustNewCreditAsset("HAH", testAccount4Address),
												AssetB: xdr.MustNewCreditAsset("WHO", testAccount1Address),
												Fee:    xdr.LiquidityPoolFeeV18,
											},
											ReserveA:                 999999,
											ReserveB:                 10001,
											TotalPoolShares:          40,
											PoolSharesTrustLineCount: 50,
										},
									},
								},
							},
						},
					},
				},
			},
			{},
		}}

	inputTransaction.Result.Result.Result.Results = &results
	inputTransaction.Envelope.V1 = &inputEnvelope
	inputTransaction.UnsafeMeta.V1 = &unsafeMeta
	return
}

func makeTradeTestOutput() [][]TradeOutput {
	offerOneOutput := TradeOutput{
		Order:                 0,
		LedgerClosedAt:        genericCloseTime,
		SellingAccountAddress: testAccount1Address,
		SellingAssetCode:      "ETH",
		SellingAssetIssuer:    testAccount3Address,
		SellingAssetType:      "credit_alphanum4",
		SellingAssetID:        4476940172956910889,
		SellingAmount:         13300347 * 0.0000001,
		BuyingAccountAddress:  testAccount3Address,
		BuyingAssetCode:       "USDT",
		BuyingAssetIssuer:     testAccount4Address,
		BuyingAssetType:       "credit_alphanum4",
		BuyingAssetID:         -8205667356306085451,
		BuyingAmount:          12634 * 0.0000001,
		PriceN:                12634,
		PriceD:                13300347,
		SellingOfferID:        null.IntFrom(97684906),
		BuyingOfferID:         null.IntFrom(4611686018427388005),
		HistoryOperationID:    101,
		TradeType:             1,
	}
	offerTwoOutput := TradeOutput{
		Order:                 0,
		LedgerClosedAt:        genericCloseTime,
		SellingAccountAddress: testAccount3Address,
		SellingAssetCode:      "USDT",
		SellingAssetIssuer:    testAccount4Address,
		SellingAssetType:      "credit_alphanum4",
		SellingAssetID:        -8205667356306085451,
		SellingAmount:         500 * 0.0000001,
		BuyingAccountAddress:  testAccount3Address,
		BuyingAssetCode:       "",
		BuyingAssetIssuer:     "",
		BuyingAssetType:       "native",
		BuyingAssetID:         -5706705804583548011,
		BuyingAmount:          20 * 0.0000001,
		PriceN:                25,
		PriceD:                1,
		SellingOfferID:        null.IntFrom(86106895),
		BuyingOfferID:         null.IntFrom(4611686018427388005),
		HistoryOperationID:    101,
		TradeType:             1,
	}

	lPOneOutput := TradeOutput{
		Order:                        0,
		LedgerClosedAt:               genericCloseTime,
		SellingAssetCode:             "WER",
		SellingAssetIssuer:           testAccount4Address,
		SellingAssetType:             "credit_alphanum4",
		SellingAssetID:               -7615773297180926952,
		SellingAmount:                123 * 0.0000001,
		BuyingAccountAddress:         testAccount3Address,
		BuyingAssetCode:              "NIJ",
		BuyingAssetIssuer:            testAccount1Address,
		BuyingAssetType:              "credit_alphanum4",
		BuyingAssetID:                -8061435944444096568,
		BuyingAmount:                 456 * 0.0000001,
		PriceN:                       456,
		PriceD:                       123,
		BuyingOfferID:                null.IntFrom(4611686018427388005),
		SellingLiquidityPoolID:       null.StringFrom("0405060000000000000000000000000000000000000000000000000000000000"),
		LiquidityPoolFee:             null.IntFrom(30),
		HistoryOperationID:           101,
		TradeType:                    2,
		RoundingSlippage:             null.IntFrom(0),
		SellerIsExact:                null.BoolFrom(false),
		SellingLiquidityPoolIDStrkey: null.StringFrom("LACAKBQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGOE"),
	}

	lPTwoOutput := TradeOutput{
		Order:                        0,
		LedgerClosedAt:               genericCloseTime,
		SellingAssetCode:             "HAH",
		SellingAssetIssuer:           testAccount1Address,
		SellingAssetType:             "credit_alphanum4",
		SellingAssetID:               -6231594281606355691,
		SellingAmount:                1 * 0.0000001,
		BuyingAccountAddress:         testAccount3Address,
		BuyingAssetCode:              "WHO",
		BuyingAssetIssuer:            testAccount4Address,
		BuyingAssetType:              "credit_alphanum4",
		BuyingAssetID:                -680582465233747022,
		BuyingAmount:                 1 * 0.0000001,
		PriceN:                       1,
		PriceD:                       1,
		BuyingOfferID:                null.IntFrom(4611686018427388005),
		SellingLiquidityPoolID:       null.StringFrom("0102030405060000000000000000000000000000000000000000000000000000"),
		LiquidityPoolFee:             null.IntFrom(30),
		HistoryOperationID:           101,
		TradeType:                    2,
		RoundingSlippage:             null.IntFrom(9223372036854775807),
		SellerIsExact:                null.BoolFrom(true),
		SellingLiquidityPoolIDStrkey: null.StringFrom("LAAQEAYEAUDAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABUTF"),
	}

	onePriceIsAmount := offerOneOutput
	onePriceIsAmount.PriceN = 12634
	onePriceIsAmount.PriceD = 13300347
	onePriceIsAmount.SellerIsExact = null.BoolFrom(false)

	offerOneOutputSecondPlace := onePriceIsAmount
	offerOneOutputSecondPlace.Order = 1
	offerOneOutputSecondPlace.SellerIsExact = null.BoolFrom(true)

	twoPriceIsAmount := offerTwoOutput
	twoPriceIsAmount.PriceN = int64(twoPriceIsAmount.BuyingAmount * 10000000)
	twoPriceIsAmount.PriceD = int64(twoPriceIsAmount.SellingAmount * 10000000)
	twoPriceIsAmount.SellerIsExact = null.BoolFrom(true)

	offerTwoOutputSecondPlace := twoPriceIsAmount
	offerTwoOutputSecondPlace.Order = 1
	offerTwoOutputSecondPlace.SellerIsExact = null.BoolFrom(false)

	output := [][]TradeOutput{
		{offerOneOutput},
		{offerTwoOutput},
		{onePriceIsAmount, offerTwoOutputSecondPlace},
		{twoPriceIsAmount, offerOneOutputSecondPlace},
		{lPOneOutput},
		{lPTwoOutput},
		{},
	}
	return output
}
