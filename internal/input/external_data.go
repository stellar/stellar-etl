package input

import (
	"fmt"
	"log"
	"net/url"

	"github.com/stellar/go/utils/apiclient"
	"github.com/stellar/stellar-etl/internal/utils"
)

type ProviderConfig struct {
	BaseURL     string
	AuthType    string
	AuthKeyEnv  string
	AuthHeaders map[string]interface{}
	Endpoint    string
	QueryParams url.Values
	RequestType string
}

func GetProviderConfig(provider string) ProviderConfig {
	var providerConfig ProviderConfig
	switch provider {
	case "retool":
		providerConfig = ProviderConfig{
			BaseURL:     "https://xrri-vvsg-obfa.n7c.xano.io/api:glgSAjxV",
			AuthType:    "api_key",
			AuthHeaders: map[string]interface{}{"api_key": utils.GetEnv("RETOOL_API_KEY", "test-api-key")},
			RequestType: "GET",
			Endpoint:    "apps_details",
			QueryParams: url.Values{},
		}
	default:
		panic("unsupported provider: " + provider)
	}
	return providerConfig
}

func GetEntityData[T any](client *apiclient.APIClient, provider string, startTime string, endTime string) ([]T, error) {
	providerConfig := GetProviderConfig(provider)

	if client == nil {
		client = &apiclient.APIClient{
			BaseURL:     providerConfig.BaseURL,
			AuthType:    providerConfig.AuthType,
			AuthHeaders: providerConfig.AuthHeaders,
		}
	}
	reqParams := apiclient.RequestParams{
		RequestType: providerConfig.RequestType,
		Endpoint:    providerConfig.Endpoint,
		QueryParams: providerConfig.QueryParams,
	}
	result, err := client.CallAPI(reqParams)
	if err != nil {
		return nil, fmt.Errorf("failed to call API: %w", err)
	}

	// Assert that the result is a slice of interfaces
	resultSlice, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("Result is not a slice of interface")
	}
	dataSlice := []T{}

	for i, item := range resultSlice {
		if itemMap, ok := item.(map[string]interface{}); ok {
			var resp T
			err := utils.MapToStruct(itemMap, &resp)
			if err != nil {
				log.Printf("Error converting map to struct: %v", err)
				continue
			}
			dataSlice = append(dataSlice, resp)
		} else {
			fmt.Printf("Item %d is not a map\n", i)
		}
	}

	return dataSlice, nil
}
