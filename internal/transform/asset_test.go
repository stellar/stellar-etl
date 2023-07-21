package transform

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

func TestTransformAsset(t *testing.T) {

	type assetInput struct {
		operation xdr.Operation
		index     int32
		txnIndex  int32
		// transaction xdr.TransactionEnvelope
	}

	type transformTest struct {
		input      assetInput
		wantOutput AssetOutput
		wantErr    error
	}

	nonPaymentInput := assetInput{
		operation: genericBumpOperation,
		txnIndex:  0,
		index:     0,
	}

	tests := []transformTest{
		{
			input:      nonPaymentInput,
			wantOutput: AssetOutput{},
			wantErr:    fmt.Errorf("operation of type 11 cannot issue an asset (id 0)"),
		},
	}

	hardCodedInputTransaction, err := makeAssetTestInput()
	assert.NoError(t, err)
	hardCodedOutputArray := makeAssetTestOutput()

	for i, op := range hardCodedInputTransaction.Envelope.Operations() {
		tests = append(tests, transformTest{
			input: assetInput{
				operation: op,
				index:     int32(i),
				txnIndex:  int32(i)},
			wantOutput: hardCodedOutputArray[i],
			wantErr:    nil,
		})
	}

	for _, test := range tests {
		actualOutput, actualError := TransformAsset(test.input.operation, test.input.index, test.input.txnIndex, 0)
		assert.Equal(t, test.wantErr, actualError)
		assert.Equal(t, test.wantOutput, actualOutput)
	}
}

func makeAssetTestInput() (inputTransaction ingest.LedgerTransaction, err error) {
	inputTransaction = genericLedgerTransaction
	inputEnvelope := genericBumpOperationEnvelope

	inputEnvelope.Tx.SourceAccount = testAccount1

	inputOperations := []xdr.Operation{
		xdr.Operation{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypePayment,
				PaymentOp: &xdr.PaymentOp{
					Destination: testAccount2,
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
					Destination: testAccount3,
					Asset:       nativeAsset,
					Amount:      350000000,
				},
			},
		},
	}

	inputEnvelope.Tx.Operations = inputOperations
	inputTransaction.Envelope.V1 = &inputEnvelope
	return
}

func makeAssetTestOutput() (transformedAssets []AssetOutput) {
	transformedAssets = []AssetOutput{
		AssetOutput{
			AssetCode:   "USDT",
			AssetIssuer: "GBVVRXLMNCJQW3IDDXC3X6XCH35B5Q7QXNMMFPENSOGUPQO7WO7HGZPA",
			AssetType:   "credit_alphanum4",
			AssetID:     1229977787683536144,
			ID:          -8205667356306085451,
		},
		AssetOutput{
			AssetCode:   "",
			AssetIssuer: "",
			AssetType:   "native",
			AssetID:     12638146518625398189,
			ID:          -5706705804583548011,
		},
	}
	return
}
