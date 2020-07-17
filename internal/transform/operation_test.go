package transform

import (
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
	sequenceNumber := uint32(22490180) //21585418 for account creation; 21585418 for payment; 22490180 for path receive
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
			if op.Body.Type == xdr.OperationTypePathPaymentStrictReceive && tx.Result.Successful() {
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
			AssetCode: xdr.AssetCode4([4]byte{}),
			Issuer:    hardCodedDestAccount.ToAccountId(),
		},
	}
	hardCodedNativeAsset := xdr.MustNewNativeAsset()
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
			},
		},
		xdr.Operation{
			SourceAccount: &hardCodedSourceAccount,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeManageSellOffer,
			},
		},
		xdr.Operation{
			SourceAccount: &hardCodedSourceAccount,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypeCreatePassiveSellOffer,
			},
		},
	}
	inputEnvelope.Tx.Operations = inputOperations
	inputTransaction.Envelope.V1 = &inputEnvelope
	return
}

func prepareHardcodedOperationTestOutputs() (transformedOperations []OperationOutput) {
	transformedOperations = []OperationOutput{
		OperationOutput{
			SourceAccount: "GBT4YAEGJQ5YSFUMNKX6BPBUOCPNAIOFAVZOF6MIME2CECBMEIUXFZZN",
			Type:          0,
			OperationDetails: Details{
				Account:         "GBVVRXLMNCJQW3IDDXC3X6XCH35B5Q7QXNMMFPENSOGUPQO7WO7HGZPA",
				Funder:          "GBT4YAEGJQ5YSFUMNKX6BPBUOCPNAIOFAVZOF6MIME2CECBMEIUXFZZN",
				StartingBalance: 25000000,
			},
		},
		OperationOutput{
			Type:             1,
			OperationDetails: Details{},
		},
		OperationOutput{
			Type:             1,
			OperationDetails: Details{},
		},
		OperationOutput{
			Type:             2,
			OperationDetails: Details{},
		},
		OperationOutput{
			Type:             3,
			OperationDetails: Details{},
		},
		OperationOutput{
			Type:             4,
			OperationDetails: Details{},
		},
	}
	return
}
