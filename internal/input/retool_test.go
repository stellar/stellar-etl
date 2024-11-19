package input

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stellar/go/support/http/httptest"
	"github.com/stellar/go/utils/apiclient"
	"github.com/stretchr/testify/assert"
)

func TestGetRetoolData(t *testing.T) {
	hmock := httptest.NewClient()

	mockResponses := []httptest.ResponseData{
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
					"status": "live"
				}
			]`,
			Header: nil,
		},
	}

	hmock.On("GET", fmt.Sprintf("%s/apps_details", baseUrl)).
		ReturnMultipleResults(mockResponses)

	mockClient := &apiclient.APIClient{
		BaseURL: baseUrl,
		HTTP:    hmock,
	}

	result, err := GetRetoolData(mockClient)
	if err != nil {
		t.Fatalf("Error calling GetRetoolData: %v", err)
	}

	expected := []RetoolEntityDataTransformInput{
		{
			ID:             16,
			Name:           "El Dorado",
			Status:         "live",
			CreatedAt:      1706749912776,
			UpdatedAt:      nil,
			Custodial:      true,
			NonCustodial:   true,
			HomeDomainsID:  240,
			Description:    "",
			WebsiteURL:     "",
			SdpEnabled:     false,
			SorobanEnabled: false,
			Notes:          "",
			Verified:       false,
			FeeSponsor:     false,
			AccountSponsor: false,
			Live:           true,
		},
	}

	assert.Equal(t, expected, result)
}
