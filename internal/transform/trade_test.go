package transform

import (
	"fmt"
	"testing"
	"time"

	ingestio "github.com/stellar/go/exp/ingest/io"
	"github.com/stellar/go/xdr"
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

	//hardCodedInputTransaction := prepareHardcodedTradeTestInput()
	//hardCodedOutputArray := prepareHardcodedTradeTestOutput()

	genericInput := tradeInput{
		index:       0,
		transaction: genericLedgerTransaction,
		closeTime:   genericCloseTime,
	}

	resultOutOfRangeInput := genericInput
	resultOutOfRangeEnvelope := genericManageBuyOfferEnvelope
	resultOutOfRangeInput.transaction.Envelope.V1 = &resultOutOfRangeEnvelope
	resultOutOfRangeInput.transaction.Result = xdr.TransactionResultPair{
		Result: xdr.TransactionResult{
			Result: xdr.TransactionResultResult{
				Code:    xdr.TransactionResultCodeTxSuccess,
				Results: &[]xdr.OperationResult{},
			},
		},
	}

	failedTxInput := genericInput
	failedTxInput.transaction.Result = xdr.TransactionResultPair{
		Result: xdr.TransactionResult{
			Result: xdr.TransactionResultResult{
				Code:    xdr.TransactionResultCodeTxFailed,
				Results: &[]xdr.OperationResult{},
			},
		},
	}
	//check no tr, no result, no success
	noTrInput := genericInput
	noTrEnvelope := genericManageBuyOfferEnvelope
	noTrInput.transaction.Envelope.V1 = &noTrEnvelope
	noTrInput.transaction.Result = xdr.TransactionResultPair{
		Result: xdr.TransactionResult{
			Result: xdr.TransactionResultResult{
				Code: xdr.TransactionResultCodeTxSuccess,
				Results: &[]xdr.OperationResult{
					{
						Tr: nil,
					},
				},
			},
		},
	}

	failedResultInput := genericInput
	failedResultEnvelope := genericManageBuyOfferEnvelope
	failedResultInput.transaction.Envelope.V1 = &failedResultEnvelope
	failedResultInput.transaction.Result = xdr.TransactionResultPair{
		Result: xdr.TransactionResult{
			Result: xdr.TransactionResultResult{
				Code: xdr.TransactionResultCodeTxSuccess,
				Results: &[]xdr.OperationResult{
					{
						Tr: &xdr.OperationResultTr{
							Type: xdr.OperationTypeManageBuyOffer,
							ManageBuyOfferResult: &xdr.ManageBuyOfferResult{
								Code: xdr.ManageBuyOfferResultCodeManageBuyOfferMalformed,
							},
						},
					},
				},
			},
		},
	}
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
	}

	/*for i := range hardCodedInputTransaction.Envelope.Operations() {
		tests = append(tests, transformTest{
			input:      tradeInput{index: int32(i), transaction: hardCodedInputTransaction, closeTime: genericCloseTime},
			wantOutput: hardCodedOutputArray[i],
			wantErr:    nil,
		})
	}*/

	for _, test := range tests {
		actualOutput, actualError := TransformTrade(test.input.index, test.input.transaction, test.input.closeTime)
		assert.Equal(t, test.wantErr, actualError)
		assert.Equal(t, test.wantOutput, actualOutput)
	}
}

func prepareHardcodedTradeTestInput() ingestio.LedgerTransaction {
	return ingestio.LedgerTransaction{}
}

func prepareHardcodedTradeTestOutput() [][]TradeOutput {
	return [][]TradeOutput{}
}
