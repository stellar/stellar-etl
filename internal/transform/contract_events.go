package transform

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/guregu/null"
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
	// GetTransactionEvents will return all contract events and diagnostic events emitted
	transactionEvents, err := transaction.GetTransactionEvents()
	if err != nil {
		return []ContractEventOutput{}, err
	}

	var transformedContractEvents []ContractEventOutput

	// Need to loop through the 3 different arrays within TransactionEvents and join them all together in a final []ContractEventOutput
	for _, transactionEvent := range transactionEvents.TransactionEvents {
		diagnosticEvent := transactionEvent2DiagnosticEvent(transactionEvent)
		parsedDiagnosticEvent, err := parseDiagnosticEvent(diagnosticEvent, transaction, lhe)
		if err != nil {
			return []ContractEventOutput{}, err
		}

		transformedContractEvents = append(transformedContractEvents, parsedDiagnosticEvent)
	}

	// Note that OperationEvents is an array of operations each with an array of ContractEvents (e.g., [][]ContractEvents)
	for i, operationEvents := range transactionEvents.OperationEvents {
		for _, contractEvent := range operationEvents {
			diagnosticEvent := contractEvent2DiagnosticEvent(contractEvent)
			parsedDiagnosticEvent, err := parseDiagnosticEvent(diagnosticEvent, transaction, lhe)
			if err != nil {
				return []ContractEventOutput{}, err
			}

			operationID := toid.New(int32(parsedDiagnosticEvent.LedgerSequence), int32(transaction.Index), int32(i)+1).ToInt64() //operationIndex needs +1 increment to stay in sync with ingest package

			parsedDiagnosticEvent.OperationID = null.IntFrom(operationID)

			transformedContractEvents = append(transformedContractEvents, parsedDiagnosticEvent)
		}
	}

	for _, diagnosticEvent := range transactionEvents.DiagnosticEvents {
		parsedDiagnosticEvent, err := parseDiagnosticEvent(diagnosticEvent, transaction, lhe)
		if err != nil {
			return []ContractEventOutput{}, err
		}

		transformedContractEvents = append(transformedContractEvents, parsedDiagnosticEvent)
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

	return []xdr.ScVal{}
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

	return xdr.ScVal{}
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

// transactionEvent2DiagnosticEvent converts TransactionEvents into DiagnosticEvents
// Note that the TransactionEvent.Stage is not preserved when changed to a DiagnosticEvent
// stellar-etl/hubble does not need to record the stage/ordering of events
func transactionEvent2DiagnosticEvent(transactionEvent xdr.TransactionEvent) xdr.DiagnosticEvent {
	// TransactionEvents are only emitted in successful contract calls
	// InSuccessfulContractCall will be set to true for classic events
	return xdr.DiagnosticEvent{
		InSuccessfulContractCall: true,
		Event:                    transactionEvent.Event,
	}
}

// contractEvent2DiagnosticEvent converts ContractEvents into DiagnosticEvents
func contractEvent2DiagnosticEvent(contractEvent xdr.ContractEvent) xdr.DiagnosticEvent {
	// ContractEvents are only emitted in successful contract calls
	// InSuccessfulContractCall will be set to true for classic events
	return xdr.DiagnosticEvent{
		InSuccessfulContractCall: true,
		Event:                    contractEvent,
	}
}

// parseDiagnosticEvent will parse out the event information
// As mentioned before, the event output will be renamed to ContractEventOutput even though
// this is a parse of DiagnosticEvents and most likely converted ContractEvents and TransactionEvents.
// This decision can be revisted later to rename these variable/output names as well as the history_contract_events table.
// Note that classic events are also named ContractEvents even though they have no association with a smart contract and
// this is unlikely to change at the core level.
func parseDiagnosticEvent(
	diagnosticEvent xdr.DiagnosticEvent,
	transaction ingest.LedgerTransaction,
	lhe xdr.LedgerHeaderHistoryEntry,
) (ContractEventOutput, error) {
	var err error
	ledgerHeader := lhe.Header
	outputTransactionHash := utils.HashToHexString(transaction.Result.TransactionHash)
	outputLedgerSequence := uint32(ledgerHeader.LedgerSeq)

	transactionIndex := uint32(transaction.Index)

	outputTransactionID := toid.New(int32(outputLedgerSequence), int32(transactionIndex), 0).ToInt64()

	outputCloseTime, err := utils.TimePointToUTCTimeStamp(ledgerHeader.ScpValue.CloseTime)
	if err != nil {
		return ContractEventOutput{}, fmt.Errorf("for ledger %d; transaction %d (transaction id=%d): %v", outputLedgerSequence, transactionIndex, outputTransactionID, err)
	}

	outputSuccessful := transaction.Result.Successful()

	var outputContractId string
	var outputTopics []interface{}
	var outputTopicsDecoded []interface{}
	var outputData interface{}
	var outputDataDecoded interface{}

	outputInSuccessfulContractCall := diagnosticEvent.InSuccessfulContractCall
	event := diagnosticEvent.Event
	outputType := event.Type
	outputTypeString := event.Type.String()

	eventTopics := getEventTopics(event.Body)
	outputTopics, outputTopicsDecoded, err = serializeScValArray(eventTopics)
	if err != nil {
		return ContractEventOutput{}, err
	}

	eventData := getEventData(event.Body)
	outputData, outputDataDecoded, err = serializeScVal(eventData)
	if err != nil {
		return ContractEventOutput{}, err
	}

	// Convert the xdrContactId to string
	// TODO: https://stellarorg.atlassian.net/browse/HUBBLE-386 this should be a stellar/go/xdr function
	if event.ContractId != nil {
		contractId := *event.ContractId
		contractIdByte, _ := contractId.MarshalBinary()
		outputContractId, _ = strkey.Encode(strkey.VersionByteContract, contractIdByte)
	}

	outputContractEventXDR, err := xdr.MarshalBase64(diagnosticEvent)
	if err != nil {
		return ContractEventOutput{}, err
	}

	contractEventOutput := ContractEventOutput{
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

	return contractEventOutput, nil
}
