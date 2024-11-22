package input

import (
	"fmt"
	"log"
	"net/http"
	"net/url"

	"github.com/stellar/go/support/http/httptest"
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

func GetMockClient(provider string) *apiclient.APIClient {
	var mockResponses []httptest.ResponseData
	switch provider {
	case "retool":
		mockResponses = []httptest.ResponseData{
			{
				Status: http.StatusOK,
				Body: `[
					{
						"id": 16,
						"created_at": 1706749912776,
						"updated_at": null,
						"custodial": true,
						"non_custodial": true,
						"home_domains_id": 240,
						"name": "El Dorado",
						"description": "",
						"website_url": "",
						"sdp_enabled": false,
						"soroban_enabled": false,
						"notes": "",
						"verified": false,
						"fee_sponsor": false,
						"account_sponsor": false,
						"live": true,
						"status": "live",
						"_home_domain": {
							"id": 240,
							"created_at": 1706749903897,
							"home_domain": "eldorado.io",
							"updated_at": 1706749903897
						},
						"_app_geographies_details": [
							{
								"id": 39,
								"apps_id": 16,
								"created_at": 1707887845605,
								"geographies_id": [
									{
										"id": 176,
										"created_at": 1691020699576,
										"updated_at": 1706650713745,
										"name": "Argentina",
										"official_name": "The Argentine Republic"
									},
									{
										"id": 273,
										"created_at": 1691020699834,
										"updated_at": 1706650708355,
										"name": "Brazil",
										"official_name": "The Federative Republic of Brazil"
									}
								],
								"retail": false,
								"enterprise": false
							}
						],
						"_app_to_ramps_integrations": [
							{
								"id": 18,
								"created_at": 1707617027154,
								"anchors_id": 28,
								"apps_id": 16,
								"_anchor": {
									"id": 28,
									"created_at": 1705423531705,
									"name": "MoneyGram",
									"updated_at": 1706596979487,
									"home_domains_id": 203
								}
							}
						]
					}
				]`,
				Header: nil,
			},
		}
	default:
		panic("unsupported provider: " + provider)
	}
	hmock := httptest.NewClient()
	providerConfig := GetProviderConfig(provider)

	hmock.On("GET", fmt.Sprintf("%s/%s", providerConfig.BaseURL, providerConfig.Endpoint)).
		ReturnMultipleResults(mockResponses)

	mockClient := &apiclient.APIClient{
		BaseURL: providerConfig.BaseURL,
		HTTP:    hmock,
	}
	return mockClient
}
