package transform

import (
	"fmt"
	"testing"
	"time"

	ingestio "github.com/stellar/go/exp/ingest/io"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/internal/utils"
	"github.com/stretchr/testify/assert"
)

func TestTransformTrade(t *testing.T) {
	type tradeInput struct {
		index       int32
		transaction ingestio.LedgerTransaction
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
	wrongTypeInput.transaction = ingestio.LedgerTransaction{
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
						OffersClaimed: []xdr.ClaimOfferAtom{
							xdr.ClaimOfferAtom{
								SellerId:   genericAccountID,
								AmountSold: -1,
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
						OffersClaimed: []xdr.ClaimOfferAtom{
							xdr.ClaimOfferAtom{
								SellerId:     genericAccountID,
								AmountBought: -2,
							},
						},
					},
				},
			}},
	}, true)

	negOfferIDInput := genericInput
	negOfferIDEnvelope := genericManageBuyOfferEnvelope
	negOfferIDInput.transaction.Envelope.V1 = &negOfferIDEnvelope
	negOfferIDInput.transaction.Result = wrapOperationsResultsSlice([]xdr.OperationResult{
		xdr.OperationResult{
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeManageBuyOffer,
				ManageBuyOfferResult: &xdr.ManageBuyOfferResult{
					Code: xdr.ManageBuyOfferResultCodeManageBuyOfferSuccess,
					Success: &xdr.ManageOfferSuccessResult{
						OffersClaimed: []xdr.ClaimOfferAtom{
							xdr.ClaimOfferAtom{
								SellerId: genericAccountID,
								OfferId:  -3,
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
		{
			negOfferIDInput,
			[]TradeOutput{}, fmt.Errorf("Offer ID is negative (-3) for operation at index 0"),
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
		actualOutput, actualError := TransformTrade(test.input.index, test.input.transaction, test.input.closeTime)
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

func makeTradeTestInput() (inputTransaction ingestio.LedgerTransaction) {
	inputTransaction = genericLedgerTransaction
	inputEnvelope := genericBumpOperationEnvelope

	inputEnvelope.Tx.SourceAccount = testAccount3
	offerOne := xdr.ClaimOfferAtom{
		SellerId:     testAccount1ID,
		OfferId:      97684906,
		AssetSold:    ethAsset,
		AssetBought:  usdtAsset,
		AmountSold:   13300347,
		AmountBought: 12634,
	}
	offerTwo := xdr.ClaimOfferAtom{
		SellerId:     testAccount3ID,
		OfferId:      86106895,
		AssetSold:    usdtAsset,
		AssetBought:  nativeAsset,
		AmountSold:   17339680,
		AmountBought: 57798933,
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
						OffersClaimed: []xdr.ClaimOfferAtom{
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
						OffersClaimed: []xdr.ClaimOfferAtom{
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
						Offers: []xdr.ClaimOfferAtom{
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
						Offers: []xdr.ClaimOfferAtom{
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
						OffersClaimed: []xdr.ClaimOfferAtom{},
					},
				},
			},
		},
	}
	inputTransaction.Result.Result.Result.Results = &results
	inputTransaction.Envelope.V1 = &inputEnvelope
	return
}

func makeTradeTestOutput() [][]TradeOutput {
	offerOneOutput := TradeOutput{
		Order:                 0,
		LedgerClosedAt:        genericCloseTime,
		OfferID:               97684906,
		BaseAccountAddress:    testAccount1Address,
		BaseAssetCode:         "ETH",
		BaseAssetIssuer:       testAccount3Address,
		BaseAssetType:         "credit_alphanum4",
		BaseAmount:            13300347,
		CounterAccountAddress: testAccount3Address,
		CounterAssetCode:      "USDT",
		CounterAssetIssuer:    testAccount4Address,
		CounterAssetType:      "credit_alphanum4",
		CounterAmount:         12634,
		BaseIsSeller:          true,
		PriceN:                12634,
		PriceD:                13300347,
	}
	offerTwoOutput := TradeOutput{
		Order:                 0,
		LedgerClosedAt:        genericCloseTime,
		OfferID:               86106895,
		BaseAccountAddress:    testAccount3Address,
		BaseAssetCode:         "USDT",
		BaseAssetIssuer:       testAccount4Address,
		BaseAssetType:         "credit_alphanum4",
		BaseAmount:            17339680,
		CounterAccountAddress: testAccount3Address,
		CounterAssetCode:      "",
		CounterAssetIssuer:    "",
		CounterAssetType:      "native",
		CounterAmount:         57798933,
		BaseIsSeller:          true,
		PriceN:                57798933,
		PriceD:                17339680,
	}

	onePriceIsAmount := offerOneOutput
	onePriceIsAmount.PriceN = onePriceIsAmount.CounterAmount
	onePriceIsAmount.PriceD = onePriceIsAmount.BaseAmount

	offerOneOutputSecondPlace := onePriceIsAmount
	offerOneOutputSecondPlace.Order = 1

	twoPriceIsAmount := offerTwoOutput
	twoPriceIsAmount.PriceN = twoPriceIsAmount.CounterAmount
	twoPriceIsAmount.PriceD = twoPriceIsAmount.BaseAmount

	offerTwoOutputSecondPlace := twoPriceIsAmount
	offerTwoOutputSecondPlace.Order = 1

	output := [][]TradeOutput{
		[]TradeOutput{offerOneOutput},
		[]TradeOutput{offerTwoOutput},
		[]TradeOutput{onePriceIsAmount, offerTwoOutputSecondPlace},
		[]TradeOutput{twoPriceIsAmount, offerOneOutputSecondPlace},
		[]TradeOutput{},
	}
	output[0][0].HistoryOperationBase64 = "AAAAAAAAAAMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=="
	output[1][0].HistoryOperationBase64 = "AAAAAAAAAAwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=="
	output[2][0].HistoryOperationBase64 = "AAAAAAAAAA0AAAAAAAAAAAAAAAAAAAAAiOGmtKVxUo+qnybiDmy8P+c8roC0RmMMW+8BUq9wfXgAAAAAAAAAAAAAAAAAAAAA"
	output[2][1].HistoryOperationBase64 = "AAAAAAAAAA0AAAAAAAAAAAAAAAAAAAAAiOGmtKVxUo+qnybiDmy8P+c8roC0RmMMW+8BUq9wfXgAAAAAAAAAAAAAAAAAAAAA"
	output[3][0].HistoryOperationBase64 = "AAAAAQAAAABnzACGTDuJFoxqr+C8NHCe0CHFBXLi+YhhNCIILCIpcgAAAAIAAAAAAAAAAAAAAAAAAAAAiOGmtKVxUo+qnybiDmy8P+c8roC0RmMMW+8BUq9wfXgAAAAAAAAAAAAAAAAAAAAA"
	output[3][1].HistoryOperationBase64 = "AAAAAQAAAABnzACGTDuJFoxqr+C8NHCe0CHFBXLi+YhhNCIILCIpcgAAAAIAAAAAAAAAAAAAAAAAAAAAiOGmtKVxUo+qnybiDmy8P+c8roC0RmMMW+8BUq9wfXgAAAAAAAAAAAAAAAAAAAAA"
	return output
}
