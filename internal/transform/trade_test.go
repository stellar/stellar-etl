package transform

import (
	"fmt"
	"io"
	"testing"
	"time"

	ingestio "github.com/stellar/go/exp/ingest/io"
	"github.com/stellar/go/exp/ingest/ledgerbackend"
	"github.com/stellar/go/network"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
)

type OfferIDType uint64

const (
	CoreOfferIDType OfferIDType = 0
	TOIDType        OfferIDType = 1

	mask uint64 = 0xC000000000000000
)

func encodeOfferId(id uint64, typ OfferIDType) int64 {
	// First ensure the bits we're going to change are 0s
	if id&mask != 0 {
		panic("Value too big to encode")
	}
	return int64(id | uint64(typ)<<62)
}
func TestLive(t *testing.T) {
	//30715263 has deleted stuff
	sequence := uint32(30522175) //30522111, 30522175, 30715263, 30715199, 24229503, 24560308
	//30522175 has prob with sell order
	archiveURL := "http://history.stellar.org/prd/core-live/core_live_001"
	backend, err := ledgerbackend.NewHistoryArchiveBackendFromURL(archiveURL)
	assert.NoError(t, err)
	txReader, err := ingestio.NewLedgerTransactionReader(backend, network.PublicNetworkPassphrase, sequence)
	assert.NoError(t, err)
	tradeOps := map[xdr.OperationType]bool{
		//xdr.OperationTypeManageBuyOffer: true,
		xdr.OperationTypeManageSellOffer: true,
		/*xdr.OperationTypeCreatePassiveSellOffer:   true,
		xdr.OperationTypePathPaymentStrictSend:    true,
		xdr.OperationTypePathPaymentStrictReceive: true,*/
	}
	for { // buy / sell - sell offer
		// sell / buy - managebuyoffer
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		assert.NoError(t, err)
		envelope := tx.Envelope
		for opIndex, op := range envelope.Operations() {
			if tradeOps[op.Body.Type] {
				converted, err := TransformTrade(int32(opIndex), tx, genericCloseTime)
				if err == nil && len(converted) > 0 {
					fmt.Println(converted)
				}
			}
		}
	}

	txReader.Close()
}

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

	hardCodedInputTransaction := prepareHardcodedTradeTestInput()
	hardCodedOutputArray := prepareHardcodedTradeTestOutput()

	genericInput := tradeInput{
		index:       0,
		transaction: genericLedgerTransaction,
		closeTime:   genericCloseTime,
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
			//the generic input has a bump sequence operation, which does not result in trades
			tradeInput{
				index:       0,
				transaction: genericLedgerTransaction,
				closeTime:   genericCloseTime,
			},
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
			[]TradeOutput{}, fmt.Errorf("Could not get OperationTypeManageBuyOfferSuccess for operation at index 0"),
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

func prepareHardcodedTradeTestInput() (inputTransaction ingestio.LedgerTransaction) {
	inputTransaction = genericLedgerTransaction
	inputEnvelope := genericBumpOperationEnvelope

	inputEnvelope.Tx.SourceAccount = hardCodedAccountThree
	offerOne := xdr.ClaimOfferAtom{
		SellerId:     hardCodedAccountOneID,
		OfferId:      97684906,
		AssetSold:    hardCodedETHAsset,
		AssetBought:  hardCodedUSDTAsset,
		AmountSold:   13300347,
		AmountBought: 12634,
	}
	offerTwo := xdr.ClaimOfferAtom{
		SellerId:     hardCodedAccountThreeID,
		OfferId:      86106895,
		AssetSold:    hardCodedUSDTAsset,
		AssetBought:  hardCodedNativeAsset,
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
				Type:                    xdr.OperationTypePathPaymentStrictSend,
				PathPaymentStrictSendOp: &xdr.PathPaymentStrictSendOp{},
			},
		},
		xdr.Operation{
			SourceAccount: &hardCodedAccountThree,
			Body: xdr.OperationBody{
				Type:                       xdr.OperationTypePathPaymentStrictReceive,
				PathPaymentStrictReceiveOp: &xdr.PathPaymentStrictReceiveOp{},
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

func prepareHardcodedTradeTestOutput() [][]TradeOutput {
	offerOneOutput := TradeOutput{
		Order:                 0,
		LedgerClosedAt:        genericCloseTime,
		OfferID:               97684906,
		BaseAccountAddress:    hardCodedAccountOneAddress,
		BaseAssetCode:         "ETH",
		BaseAssetIssuer:       hardCodedAccountThreeAddress,
		BaseAssetType:         "credit_alphanum4",
		BaseAmount:            13300347,
		BaseOfferID:           0,
		CounterAccountAddress: hardCodedAccountThreeAddress,
		CounterAssetCode:      "USDT",
		CounterAssetIssuer:    hardCodedAccountFourAddress,
		CounterAssetType:      "credit_alphanum4",
		CounterAmount:         12634,
		BaseIsSeller:          true,
		CounterOfferID:        0,
		PriceN:                12634,
		PriceD:                13300347,
	}
	offerTwoOutput := TradeOutput{
		Order:                 0,
		LedgerClosedAt:        genericCloseTime,
		OfferID:               86106895,
		BaseAccountAddress:    hardCodedAccountThreeAddress,
		BaseAssetCode:         "USDT",
		BaseAssetIssuer:       hardCodedAccountFourAddress,
		BaseAssetType:         "credit_alphanum4",
		BaseAmount:            17339680,
		BaseOfferID:           0,
		CounterAccountAddress: hardCodedAccountThreeAddress,
		CounterAssetCode:      "",
		CounterAssetIssuer:    "",
		CounterAssetType:      "native",
		CounterAmount:         57798933,
		BaseIsSeller:          true,
		CounterOfferID:        0,
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

	return [][]TradeOutput{
		[]TradeOutput{offerOneOutput},
		[]TradeOutput{offerTwoOutput},
		[]TradeOutput{onePriceIsAmount, offerTwoOutputSecondPlace},
		[]TradeOutput{twoPriceIsAmount, offerOneOutputSecondPlace},
		[]TradeOutput{},
	}
}
