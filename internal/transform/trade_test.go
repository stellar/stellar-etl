package transform

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/guregu/null"
	"github.com/stretchr/testify/assert"

	"github.com/stellar/stellar-etl/internal/utils"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
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
		xdr.OperationResult{Tr: nil},
	}, true)

	failedResultInput := genericInput
	failedResultEnvelope := genericManageBuyOfferEnvelope
	failedResultInput.transaction.Envelope.V1 = &failedResultEnvelope
	failedResultInput.transaction.Result = wrapOperationsResultsSlice([]xdr.OperationResult{
		xdr.OperationResult{
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
		xdr.OperationResult{
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeManageBuyOffer,
				ManageBuyOfferResult: &xdr.ManageBuyOfferResult{
					Code: xdr.ManageBuyOfferResultCodeManageBuyOfferSuccess,
					Success: &xdr.ManageOfferSuccessResult{
						OffersClaimed: []xdr.ClaimAtom{
							xdr.ClaimAtom{
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
		xdr.OperationResult{
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeManageBuyOffer,
				ManageBuyOfferResult: &xdr.ManageBuyOfferResult{
					Code: xdr.ManageBuyOfferResultCodeManageBuyOfferSuccess,
					Success: &xdr.ManageOfferSuccessResult{
						OffersClaimed: []xdr.ClaimAtom{
							xdr.ClaimAtom{
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
			[]TradeOutput{}, fmt.Errorf("Operation of type OperationTypeBumpSequence at index 0 does not result in trades"),
		},
		{
			resultOutOfRangeInput,
			[]TradeOutput{}, fmt.Errorf("Operation index of 0 is out of bounds in result slice (len = 0)"),
		},
		{
			failedTxInput,
			[]TradeOutput{}, fmt.Errorf("Transaction failed; no trades"),
		},
		{
			noTrInput,
			[]TradeOutput{}, fmt.Errorf("Could not get result Tr for operation at index 0"),
		},
		{
			failedResultInput,
			[]TradeOutput{}, fmt.Errorf("Could not get ManageOfferSuccess for operation at index 0"),
		},
		{
			negBaseAmountInput,
			[]TradeOutput{}, fmt.Errorf("Amount sold is negative (-1) for operation at index 0"),
		},
		{
			negCounterAmountInput,
			[]TradeOutput{}, fmt.Errorf("Amount bought is negative (-2) for operation at index 0"),
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
	inputOperations := []xdr.Operation{

		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type:              xdr.OperationTypeManageSellOffer,
				ManageSellOfferOp: &xdr.ManageSellOfferOp{},
			},
		},

		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type:             xdr.OperationTypeManageBuyOffer,
				ManageBuyOfferOp: &xdr.ManageBuyOfferOp{},
			},
		},
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypePathPaymentStrictSend,
				PathPaymentStrictSendOp: &xdr.PathPaymentStrictSendOp{
					Destination: testAccount1,
				},
			},
		},
		xdr.Operation{
			SourceAccount: &testAccount3,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypePathPaymentStrictReceive,
				PathPaymentStrictReceiveOp: &xdr.PathPaymentStrictReceiveOp{
					Destination: testAccount1,
				},
			},
		},
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type:                     xdr.OperationTypeCreatePassiveSellOffer,
				CreatePassiveSellOfferOp: &xdr.CreatePassiveSellOfferOp{},
			},
		},
	}
	inputEnvelope.Tx.Operations = inputOperations
	results := []xdr.OperationResult{
		xdr.OperationResult{
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

		xdr.OperationResult{
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
		xdr.OperationResult{
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
		xdr.OperationResult{
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
		xdr.OperationResult{
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
			xdr.OperationMeta{
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
			xdr.OperationMeta{
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
			xdr.OperationMeta{
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
			xdr.OperationMeta{
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
		SellingAmount:         13300347 * 0.0000001,
		BuyingAccountAddress:  testAccount3Address,
		BuyingAssetCode:       "USDT",
		BuyingAssetIssuer:     testAccount4Address,
		BuyingAssetType:       "credit_alphanum4",
		BuyingAmount:          12634 * 0.0000001,
		PriceN:                12634,
		PriceD:                13300347,
		SellingOfferID:        null.IntFrom(97684906),
		BuyingOfferID:         null.IntFrom(4611686018427388004),
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
		SellingAmount:         500 * 0.0000001,
		BuyingAccountAddress:  testAccount3Address,
		BuyingAssetCode:       "",
		BuyingAssetIssuer:     "",
		BuyingAssetType:       "native",
		BuyingAmount:          20 * 0.0000001,
		PriceN:                25,
		PriceD:                1,
		SellingOfferID:        null.IntFrom(86106895),
		BuyingOfferID:         null.IntFrom(4611686018427388004),
		HistoryOperationID:    101,
		TradeType:             1,
	}

	onePriceIsAmount := offerOneOutput
	onePriceIsAmount.PriceN = 12634
	onePriceIsAmount.PriceD = 13300347
	onePriceIsAmount.SellerIsExact = null.Bool{
		NullBool: sql.NullBool{
			Bool:  false,
			Valid: true,
		},
	}

	offerOneOutputSecondPlace := onePriceIsAmount
	offerOneOutputSecondPlace.Order = 1
	offerOneOutputSecondPlace.SellerIsExact = null.Bool{
		NullBool: sql.NullBool{
			Bool:  true,
			Valid: true,
		},
	}

	twoPriceIsAmount := offerTwoOutput
	twoPriceIsAmount.PriceN = int64(twoPriceIsAmount.BuyingAmount * 10000000)
	twoPriceIsAmount.PriceD = int64(twoPriceIsAmount.SellingAmount * 10000000)
	twoPriceIsAmount.SellerIsExact = null.Bool{
		NullBool: sql.NullBool{
			Bool:  true,
			Valid: true,
		},
	}

	offerTwoOutputSecondPlace := twoPriceIsAmount
	offerTwoOutputSecondPlace.Order = 1
	offerTwoOutputSecondPlace.SellerIsExact = null.Bool{
		NullBool: sql.NullBool{
			Bool:  false,
			Valid: true,
		},
	}

	output := [][]TradeOutput{
		[]TradeOutput{offerOneOutput},
		[]TradeOutput{offerTwoOutput},
		[]TradeOutput{onePriceIsAmount, offerTwoOutputSecondPlace},
		[]TradeOutput{twoPriceIsAmount, offerOneOutputSecondPlace},
		[]TradeOutput{},
	}
	return output
}
