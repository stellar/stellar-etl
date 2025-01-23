package transform

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/stellar/stellar-etl/internal/toid"
	"github.com/stellar/stellar-etl/internal/utils"

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
		var outputContractId string
		outputTopicsJson := make(map[string][]map[string]string, 1)
		outputTopicsDecodedJson := make(map[string][]map[string]string, 1)

		outputInSuccessfulContractCall := contractEvent.InSuccessfulContractCall
		event := contractEvent.Event
		outputType := event.Type
		outputTypeString := event.Type.String()

		eventTopics := getEventTopics(event.Body)
		outputTopics, outputTopicsDecoded, err := serializeScValArray(eventTopics)
		if err != nil {
			return []ContractEventOutput{}, err
		}
		outputTopicsJson["topics"] = outputTopics
		outputTopicsDecodedJson["topics_decoded"] = outputTopicsDecoded

		eventData := getEventData(event.Body)
		outputData, outputDataDecoded, err := serializeScVal(eventData)
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
			Topics:                   outputTopicsJson,
			TopicsDecoded:            outputTopicsDecodedJson,
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

// TODO this should also be used in the operations processor
func serializeScVal(scVal xdr.ScVal) (map[string]string, map[string]string, error) {
	serializedData := map[string]string{}
	serializedData["value"] = "n/a"
	serializedData["type"] = "n/a"

	serializedDataDecoded := map[string]string{}
	serializedDataDecoded["value"] = "n/a"
	serializedDataDecoded["type"] = "n/a"

	if scValTypeName, ok := scVal.ArmForSwitch(int32(scVal.Type)); ok {
		serializedData["type"] = scValTypeName
		serializedDataDecoded["type"] = scValTypeName
		raw, err := scVal.MarshalBinary()
		if err != nil {
			return nil, nil, err
		}

		serializedData["value"] = base64.StdEncoding.EncodeToString(raw)
		serializedDataDecoded["value"], err = printScValJSON(scVal)
		if err != nil {
			return nil, nil, err
		}
	}

	return serializedData, serializedDataDecoded, nil
}

// TODO this should also be used in the operations processor
func serializeScValArray(scVals []xdr.ScVal) ([]map[string]string, []map[string]string, error) {
	data := make([]map[string]string, 0, len(scVals))
	dataDecoded := make([]map[string]string, 0, len(scVals))

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

// printScValJSON is used to print json parsable ScVal output instead of
// the ScVal.String() output which prints out %v values that aren't parsable
// Example: {"type":"Map","value":"[{collateral [{1 2386457777}]} {liabilities []} {supply []}]"}
//
// This function traverses through the ScVal structure by calling removeNulls which recursively removes
// the null pointers from the given ScVal.
//
// The output is then: {"type":"Map","value":"[{\"collateral\": \"[{\"1\": \"2386457777\"}]\"} {\"liabilities\": \"[]\"} {\"supply\": \"[]\"}]\"}
// where "value" is a valid JSON string
func printScValJSON(scVal xdr.ScVal) (string, error) {
	var data map[string]interface{}

	rawData, err := json.Marshal(scVal)
	if err != nil {
		return "", err
	}

	json.Unmarshal(rawData, &data)
	cleaned := removeNulls(data)
	cleanedData, err := json.Marshal(cleaned)
	if err != nil {
		return "", err
	}

	return string(cleanedData), nil
}

// removeNulls will recursively traverse through the data map and remove any null values
// In theory ScVals can be nested infinitely but the depth of recursion is unlikely to be very high
// given the resource limits of smart contracts
func removeNulls(data map[string]interface{}) map[string]interface{} {
	cleaned := make(map[string]interface{})

	for k, v := range data {
		switch value := v.(type) {
		// There don't seem to be other data types that need traversing aside from maps and arrays
		case map[string]interface{}: // recurse through maps
			nested := removeNulls(value)
			if len(nested) > 0 {
				cleaned[k] = nested
			}
		case []interface{}: // recurse through arrays
			var newArr []interface{}
			for _, item := range value {
				if itemMap, ok := item.(map[string]interface{}); ok {
					filteredItem := removeNulls(itemMap)
					if len(filteredItem) > 0 {
						newArr = append(newArr, filteredItem)
					}
				} else if item != nil {
					newArr = append(newArr, item)
				}
			}
			if len(newArr) > 0 {
				cleaned[k] = newArr
			}
		default:
			if v != nil {
				cleaned[k] = v
			}
		}
	}
	return cleaned
}
