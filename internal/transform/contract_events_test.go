package transform

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/guregu/null"
	"github.com/stretchr/testify/assert"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

func TestTransformContractEvent(t *testing.T) {
	type inputStruct struct {
		transaction   ingest.LedgerTransaction
		historyHeader xdr.LedgerHeaderHistoryEntry
	}
	type transformTest struct {
		input      inputStruct
		wantOutput []ContractEventOutput
		wantErr    error
	}

	hardCodedTransaction, hardCodedLedgerHeader, err := makeContractEventTestInput()
	assert.NoError(t, err)
	hardCodedOutput, err := makeContractEventTestOutput()
	assert.NoError(t, err)

	tests := []transformTest{}

	for i := range hardCodedTransaction {
		tests = append(tests, transformTest{
			input:      inputStruct{hardCodedTransaction[i], hardCodedLedgerHeader[i]},
			wantOutput: hardCodedOutput[i],
			wantErr:    nil,
		})
	}

	for _, test := range tests {
		actualOutput, actualError := TransformContractEvent(test.input.transaction, test.input.historyHeader)
		assert.Equal(t, test.wantErr, actualError)
		assert.Equal(t, test.wantOutput, actualOutput)
	}
}

func makeContractEventTestOutput() (output [][]ContractEventOutput, err error) {

	var topics, topicsDecoded []interface{}
	topics = append(topics, "AAAAAAAAAAE=")
	topicsDecoded = append(topicsDecoded, json.RawMessage("{\"bool\":true}"))

	var data, dataDecoded interface{}
	data = "AAAAAAAAAAE="
	dataDecoded = json.RawMessage("{\"bool\":true}")

	output = [][]ContractEventOutput{{
		ContractEventOutput{
			TransactionHash:          "a87fef5eeb260269c380f2de456aad72b59bb315aaac777860456e09dac0bafb",
			TransactionID:            131090201534533632,
			Successful:               false,
			LedgerSequence:           30521816,
			ClosedAt:                 time.Date(2020, time.July, 9, 5, 28, 42, 0, time.UTC),
			InSuccessfulContractCall: true,
			ContractId:               "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4",
			Type:                     2,
			TypeString:               "ContractEventTypeDiagnostic",
			Topics:                   topics,
			TopicsDecoded:            topicsDecoded,
			Data:                     data,
			DataDecoded:              dataDecoded,
			ContractEventXDR:         "AAAAAQAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAEAAAAAAAAAAQAAAAAAAAAB",
			OperationID:              null.IntFrom(131090201534533633),
		},
		ContractEventOutput{
			TransactionHash:          "a87fef5eeb260269c380f2de456aad72b59bb315aaac777860456e09dac0bafb",
			TransactionID:            131090201534533632,
			Successful:               false,
			LedgerSequence:           30521816,
			ClosedAt:                 time.Date(2020, time.July, 9, 5, 28, 42, 0, time.UTC),
			InSuccessfulContractCall: true,
			ContractId:               "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4",
			Type:                     2,
			TypeString:               "ContractEventTypeDiagnostic",
			Topics:                   topics,
			TopicsDecoded:            topicsDecoded,
			Data:                     data,
			DataDecoded:              dataDecoded,
			ContractEventXDR:         "AAAAAQAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAEAAAAAAAAAAQAAAAAAAAAB",
		},
	},
		{
			ContractEventOutput{
				TransactionHash:          "a87fef5eeb260269c380f2de456aad72b59bb315aaac777860456e09dac0bafb",
				TransactionID:            131090201534537728,
				Successful:               false,
				LedgerSequence:           30521816,
				ClosedAt:                 time.Date(2020, time.July, 9, 5, 28, 42, 0, time.UTC),
				InSuccessfulContractCall: true,
				ContractId:               "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4",
				Type:                     1,
				TypeString:               "ContractEventTypeContract",
				Topics:                   topics,
				TopicsDecoded:            topicsDecoded,
				Data:                     data,
				DataDecoded:              dataDecoded,
				ContractEventXDR:         "AAAAAQAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAQAAAAAAAAAB",
				OperationID:              null.Int{},
			},
			ContractEventOutput{
				TransactionHash:          "a87fef5eeb260269c380f2de456aad72b59bb315aaac777860456e09dac0bafb",
				TransactionID:            131090201534537728,
				Successful:               false,
				LedgerSequence:           30521816,
				ClosedAt:                 time.Date(2020, time.July, 9, 5, 28, 42, 0, time.UTC),
				InSuccessfulContractCall: true,
				ContractId:               "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4",
				Type:                     1,
				TypeString:               "ContractEventTypeContract",
				Topics:                   topics,
				TopicsDecoded:            topicsDecoded,
				Data:                     data,
				DataDecoded:              dataDecoded,
				ContractEventXDR:         "AAAAAQAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAAQAAAAAAAAAB",
				OperationID:              null.IntFrom(131090201534537729),
			},
			ContractEventOutput{
				TransactionHash:          "a87fef5eeb260269c380f2de456aad72b59bb315aaac777860456e09dac0bafb",
				TransactionID:            131090201534537728,
				Successful:               false,
				LedgerSequence:           30521816,
				ClosedAt:                 time.Date(2020, time.July, 9, 5, 28, 42, 0, time.UTC),
				InSuccessfulContractCall: true,
				ContractId:               "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4",
				Type:                     2,
				TypeString:               "ContractEventTypeDiagnostic",
				Topics:                   topics,
				TopicsDecoded:            topicsDecoded,
				Data:                     data,
				DataDecoded:              dataDecoded,
				ContractEventXDR:         "AAAAAQAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAEAAAAAAAAAAQAAAAAAAAAB",
				OperationID:              null.Int{},
			},
		},
	}
	return
}

func makeContractEventTestInput() (transaction []ingest.LedgerTransaction, historyHeader []xdr.LedgerHeaderHistoryEntry, err error) {
	hardCodedMemoText := "HL5aCgozQHIW7sSc5XdcfmR"
	hardCodedTransactionHash := xdr.Hash([32]byte{0xa8, 0x7f, 0xef, 0x5e, 0xeb, 0x26, 0x2, 0x69, 0xc3, 0x80, 0xf2, 0xde, 0x45, 0x6a, 0xad, 0x72, 0xb5, 0x9b, 0xb3, 0x15, 0xaa, 0xac, 0x77, 0x78, 0x60, 0x45, 0x6e, 0x9, 0xda, 0xc0, 0xba, 0xfb})
	hardCodedBool := true
	hardCodedTxMetaV3 := xdr.TransactionMetaV3{
		SorobanMeta: &xdr.SorobanTransactionMeta{
			Events: []xdr.ContractEvent{
				{
					Ext: xdr.ExtensionPoint{
						V: 0,
					},
					ContractId: &xdr.ContractId{},
					Type:       xdr.ContractEventTypeDiagnostic,
					Body: xdr.ContractEventBody{
						V: 0,
						V0: &xdr.ContractEventV0{
							Topics: []xdr.ScVal{
								{
									Type: xdr.ScValTypeScvBool,
									B:    &hardCodedBool,
								},
							},
							Data: xdr.ScVal{
								Type: xdr.ScValTypeScvBool,
								B:    &hardCodedBool,
							},
						},
					},
				},
			},
			DiagnosticEvents: []xdr.DiagnosticEvent{
				{
					InSuccessfulContractCall: true,
					Event: xdr.ContractEvent{
						Ext: xdr.ExtensionPoint{
							V: 0,
						},
						ContractId: &xdr.ContractId{},
						Type:       xdr.ContractEventTypeDiagnostic,
						Body: xdr.ContractEventBody{
							V: 0,
							V0: &xdr.ContractEventV0{
								Topics: []xdr.ScVal{
									{
										Type: xdr.ScValTypeScvBool,
										B:    &hardCodedBool,
									},
								},
								Data: xdr.ScVal{
									Type: xdr.ScValTypeScvBool,
									B:    &hardCodedBool,
								},
							},
						},
					},
				},
			},
		},
	}

	hardCodedTxMetaV4 := xdr.TransactionMetaV4{
		Ext:             xdr.ExtensionPoint{},
		TxChangesBefore: xdr.LedgerEntryChanges{},
		Operations: []xdr.OperationMetaV2{
			{
				Events: []xdr.ContractEvent{
					{
						Ext: xdr.ExtensionPoint{
							V: 0,
						},
						ContractId: &xdr.ContractId{},
						Type:       xdr.ContractEventTypeContract,
						Body: xdr.ContractEventBody{
							V: 0,
							V0: &xdr.ContractEventV0{
								Topics: []xdr.ScVal{
									{
										Type: xdr.ScValTypeScvBool,
										B:    &hardCodedBool,
									},
								},
								Data: xdr.ScVal{
									Type: xdr.ScValTypeScvBool,
									B:    &hardCodedBool,
								},
							},
						},
					},
				},
			},
		},
		TxChangesAfter: xdr.LedgerEntryChanges{},
		SorobanMeta: &xdr.SorobanTransactionMetaV2{
			Ext: xdr.SorobanTransactionMetaExt{
				V: 1,
				V1: &xdr.SorobanTransactionMetaExtV1{
					Ext:                                  xdr.ExtensionPoint{},
					TotalNonRefundableResourceFeeCharged: 0,
					TotalRefundableResourceFeeCharged:    0,
					RentFeeCharged:                       0,
				},
			},
			ReturnValue: &xdr.ScVal{},
		},
		Events: []xdr.TransactionEvent{
			{
				Stage: xdr.TransactionEventStageTransactionEventStageBeforeAllTxs,
				Event: xdr.ContractEvent{
					Ext: xdr.ExtensionPoint{
						V: 0,
					},
					ContractId: &xdr.ContractId{},
					Type:       xdr.ContractEventTypeContract,
					Body: xdr.ContractEventBody{
						V: 0,
						V0: &xdr.ContractEventV0{
							Topics: []xdr.ScVal{
								{
									Type: xdr.ScValTypeScvBool,
									B:    &hardCodedBool,
								},
							},
							Data: xdr.ScVal{
								Type: xdr.ScValTypeScvBool,
								B:    &hardCodedBool,
							},
						},
					},
				},
			},
		},
		DiagnosticEvents: []xdr.DiagnosticEvent{
			{
				InSuccessfulContractCall: true,
				Event: xdr.ContractEvent{
					Ext: xdr.ExtensionPoint{
						V: 0,
					},
					ContractId: &xdr.ContractId{},
					Type:       xdr.ContractEventTypeDiagnostic,
					Body: xdr.ContractEventBody{
						V: 0,
						V0: &xdr.ContractEventV0{
							Topics: []xdr.ScVal{
								{
									Type: xdr.ScValTypeScvBool,
									B:    &hardCodedBool,
								},
							},
							Data: xdr.ScVal{
								Type: xdr.ScValTypeScvBool,
								B:    &hardCodedBool,
							},
						},
					},
				},
			},
		},
	}

	genericResultResults := &[]xdr.OperationResult{
		{
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeCreateAccount,
				CreateAccountResult: &xdr.CreateAccountResult{
					Code: 0,
				},
			},
		},
	}
	hardCodedMeta := xdr.TransactionMeta{
		V:  3,
		V3: &hardCodedTxMetaV3,
	}
	hardCodedMetaV4 := xdr.TransactionMeta{
		V:  4,
		V4: &hardCodedTxMetaV4,
	}

	destination := xdr.MuxedAccount{
		Type:    xdr.CryptoKeyTypeKeyTypeEd25519,
		Ed25519: &xdr.Uint256{1, 2, 3},
	}

	transaction = []ingest.LedgerTransaction{
		{
			Index:      1,
			UnsafeMeta: hardCodedMeta,
			Envelope: xdr.TransactionEnvelope{
				Type: xdr.EnvelopeTypeEnvelopeTypeTx,
				V1: &xdr.TransactionV1Envelope{
					Tx: xdr.Transaction{
						Ext: xdr.TransactionExt{
							V: 1,
							SorobanData: &xdr.SorobanTransactionData{
								Ext:         xdr.SorobanTransactionDataExt{},
								Resources:   xdr.SorobanResources{},
								ResourceFee: 0,
							},
						},
						SourceAccount: testAccount1,
						SeqNum:        112351890582290871,
						Memo: xdr.Memo{
							Type: xdr.MemoTypeMemoText,
							Text: &hardCodedMemoText,
						},
						Fee: 90000,
						Cond: xdr.Preconditions{
							Type: xdr.PreconditionTypePrecondTime,
							TimeBounds: &xdr.TimeBounds{
								MinTime: 0,
								MaxTime: 1594272628,
							},
						},
						Operations: []xdr.Operation{
							{
								SourceAccount: &testAccount2,
								Body: xdr.OperationBody{
									Type: xdr.OperationTypePathPaymentStrictReceive,
									PathPaymentStrictReceiveOp: &xdr.PathPaymentStrictReceiveOp{
										Destination: destination,
									},
								},
							},
						},
					},
				},
			},
			Result: xdr.TransactionResultPair{
				TransactionHash: hardCodedTransactionHash,
				Result: xdr.TransactionResult{
					FeeCharged: 300,
					Result: xdr.TransactionResultResult{
						Code:    xdr.TransactionResultCodeTxFailed,
						Results: genericResultResults,
					},
				},
			},
		},
		{
			Index:      2,
			UnsafeMeta: hardCodedMetaV4,
			Envelope: xdr.TransactionEnvelope{
				Type: xdr.EnvelopeTypeEnvelopeTypeTx,
				V1: &xdr.TransactionV1Envelope{
					Tx: xdr.Transaction{
						Ext: xdr.TransactionExt{
							V: 1,
							SorobanData: &xdr.SorobanTransactionData{
								Ext:         xdr.SorobanTransactionDataExt{},
								Resources:   xdr.SorobanResources{},
								ResourceFee: 0,
							},
						},
						SourceAccount: testAccount1,
						SeqNum:        112351890582290871,
						Memo: xdr.Memo{
							Type: xdr.MemoTypeMemoText,
							Text: &hardCodedMemoText,
						},
						Fee: 90000,
						Cond: xdr.Preconditions{
							Type: xdr.PreconditionTypePrecondTime,
							TimeBounds: &xdr.TimeBounds{
								MinTime: 0,
								MaxTime: 1594272628,
							},
						},
						Operations: []xdr.Operation{
							{
								SourceAccount: &testAccount2,
								Body: xdr.OperationBody{
									Type: xdr.OperationTypePathPaymentStrictReceive,
									PathPaymentStrictReceiveOp: &xdr.PathPaymentStrictReceiveOp{
										Destination: destination,
									},
								},
							},
						},
					},
				},
			},
			Result: xdr.TransactionResultPair{
				TransactionHash: hardCodedTransactionHash,
				Result: xdr.TransactionResult{
					FeeCharged: 300,
					Result: xdr.TransactionResultResult{
						Code:    xdr.TransactionResultCodeTxFailed,
						Results: genericResultResults,
					},
				},
			},
		},
	}
	historyHeader = []xdr.LedgerHeaderHistoryEntry{
		{
			Header: xdr.LedgerHeader{
				LedgerSeq: 30521816,
				ScpValue:  xdr.StellarValue{CloseTime: 1594272522},
			},
		},
		{
			Header: xdr.LedgerHeader{
				LedgerSeq: 30521816,
				ScpValue:  xdr.StellarValue{CloseTime: 1594272522},
			},
		},
	}
	return
}
