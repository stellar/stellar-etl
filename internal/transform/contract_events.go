package transform

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/stellar/go-stellar-xdr-json/xdrjson"
	"github.com/stellar/stellar-etl/v2/internal/toid"
	"github.com/stellar/stellar-etl/v2/internal/utils"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
)

// TransformContractEvent converts a transaction's contract events and diagnostic events into a form suitable for BigQuery.
// It is known that contract events are a subset of the diagnostic events XDR definition. We are opting to call all of these events
// contract events for better clarity to data analytics users.
func TransformContractEvent(transaction ingest.LedgerTransaction, lhe xdr.LedgerHeaderHistoryEntry) ([]ContractEventOutput, error) {
	ledgerHeader := lhe.Header
	outputTransactionHash := utils.HashToHexString(transaction.Result.TransactionHash)
	outputLedgerSequence := uint32(ledgerHeader.LedgerSeq)

	transactionIndex := uint32(transaction.Index)

	outputTransactionID := toid.New(int32(outputLedgerSequence), int32(transactionIndex), 0).ToInt64()

	outputCloseTime, err := utils.TimePointToUTCTimeStamp(ledgerHeader.ScpValue.CloseTime)
	if err != nil {
		return []ContractEventOutput{}, fmt.Errorf("for ledger %d; transaction %d (transaction id=%d): %v", outputLedgerSequence, transactionIndex, outputTransactionID, err)
	}

	// GetDiagnosticEvents will return all contract events and diagnostic events emitted
	contractEvents, err := transaction.GetDiagnosticEvents()
	if err != nil {
		return []ContractEventOutput{}, err
	}

	var transformedContractEvents []ContractEventOutput

	for _, contractEvent := range contractEvents {
		var err error
		var outputContractId string
		var outputTopics []interface{}
		var outputTopicsDecoded []interface{}
		var outputData interface{}
		var outputDataDecoded interface{}

		outputInSuccessfulContractCall := contractEvent.InSuccessfulContractCall
		event := contractEvent.Event
		outputType := event.Type
		outputTypeString := event.Type.String()

		eventTopics := getEventTopics(event.Body)
		outputTopics, outputTopicsDecoded, err = serializeScValArray(eventTopics)
		if err != nil {
			return []ContractEventOutput{}, err
		}

		eventData := getEventData(event.Body)
		outputData, outputDataDecoded, err = serializeScVal(eventData)
		if err != nil {
			return []ContractEventOutput{}, err
		}

		// Convert the xdrContactId to string
		// TODO: https://stellarorg.atlassian.net/browse/HUBBLE-386 this should be a stellar/go/xdr function
		if event.ContractId != nil {
			contractId := *event.ContractId
			contractIdByte, _ := contractId.MarshalBinary()
			outputContractId, _ = strkey.Encode(strkey.VersionByteContract, contractIdByte)
		}

		outputContractEventXDR, err := xdr.MarshalBase64(contractEvent)
		if err != nil {
			return []ContractEventOutput{}, err
		}

		outputTransactionID := toid.New(int32(outputLedgerSequence), int32(transactionIndex), 0).ToInt64()
		outputSuccessful := transaction.Result.Successful()

		transformedDiagnosticEvent := ContractEventOutput{
			TransactionHash:          outputTransactionHash,
			TransactionID:            outputTransactionID,
			Successful:               outputSuccessful,
			LedgerSequence:           outputLedgerSequence,
			ClosedAt:                 outputCloseTime,
			InSuccessfulContractCall: outputInSuccessfulContractCall,
			ContractId:               outputContractId,
			Type:                     int32(outputType),
			TypeString:               outputTypeString,
			Topics:                   outputTopics,
			TopicsDecoded:            outputTopicsDecoded,
			Data:                     outputData,
			DataDecoded:              outputDataDecoded,
			ContractEventXDR:         outputContractEventXDR,
		}

		transformedContractEvents = append(transformedContractEvents, transformedDiagnosticEvent)
	}

	return transformedContractEvents, nil
}

// TODO this should be a stellar/go/xdr function
func getEventTopics(eventBody xdr.ContractEventBody) []xdr.ScVal {
	switch eventBody.V {
	case 0:
		contractEventV0 := eventBody.MustV0()
		return contractEventV0.Topics
	default:
		panic("unsupported event body version: " + string(eventBody.V))
	}
}

// TODO this should be a stellar/go/xdr function
func getEventData(eventBody xdr.ContractEventBody) xdr.ScVal {
	switch eventBody.V {
	case 0:
		contractEventV0 := eventBody.MustV0()
		return contractEventV0.Data
	default:
		panic("unsupported event body version: " + string(eventBody.V))
	}
}

func serializeScVal(scVal xdr.ScVal) (interface{}, interface{}, error) {
	var serializedData, serializedDataDecoded interface{}
	serializedData = "n/a"
	serializedDataDecoded = "n/a"

	if _, ok := scVal.ArmForSwitch(int32(scVal.Type)); ok {
		var err error
		var raw []byte
		var jsonMessage json.RawMessage
		raw, err = scVal.MarshalBinary()
		if err != nil {
			return nil, nil, err
		}

		serializedData = base64.StdEncoding.EncodeToString(raw)
		jsonMessage, err = xdrjson.Decode(xdrjson.ScVal, raw)
		if err != nil {
			return nil, nil, err
		}

		serializedDataDecoded = jsonMessage
	}

	return serializedData, serializedDataDecoded, nil
}

func serializeScValArray(scVals []xdr.ScVal) ([]interface{}, []interface{}, error) {
	data := make([]interface{}, 0, len(scVals))
	dataDecoded := make([]interface{}, 0, len(scVals))

	for _, scVal := range scVals {
		serializedData, serializedDataDecoded, err := serializeScVal(scVal)
		if err != nil {
			return nil, nil, err
		}

		data = append(data, serializedData)
		dataDecoded = append(dataDecoded, serializedDataDecoded)
	}

	return data, dataDecoded, nil
}
