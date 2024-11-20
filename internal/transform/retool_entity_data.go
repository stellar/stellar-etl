package transform

import (
	"github.com/stellar/stellar-etl/internal/utils"
)

func TransformRetoolEntityData(entityData utils.RetoolEntityDataTransformInput) (EntityDataTransformOutput, error) {
	transformedRetoolEntityData := EntityDataTransformOutput{
		ID:             entityData.ID,
		Name:           entityData.Name,
		HomeDomain:     entityData.HomeDomain.HomeDomain,
		Status:         entityData.Status,
		CreatedAt:      entityData.CreatedAt,
		UpdatedAt:      entityData.UpdatedAt,
		Custodial:      entityData.Custodial,
		NonCustodial:   entityData.NonCustodial,
		HomeDomainsID:  entityData.HomeDomainsID,
		Description:    entityData.Description,
		WebsiteURL:     entityData.WebsiteURL,
		SdpEnabled:     entityData.SdpEnabled,
		SorobanEnabled: entityData.SorobanEnabled,
		Notes:          entityData.Notes,
		Verified:       entityData.Verified,
		FeeSponsor:     entityData.FeeSponsor,
		AccountSponsor: entityData.AccountSponsor,
		Live:           entityData.Live,
		AppGeographies: mapGeographies(entityData.AppGeographiesDetails),
		Ramps:          mapRamps(entityData.AppToRampsIntegrations),
	}

	return transformedRetoolEntityData, nil
}

func mapGeographies(appGeographies []utils.AppGeographyDetail) []string {
	var geographies []string
	for _, geoDetail := range appGeographies {
		for _, geo := range geoDetail.GeographiesID {
			geographies = append(geographies, geo.Name)
		}
	}
	return geographies
}

func mapRamps(appRamps []utils.AppToRampIntegration) []string {
	var ramps []string
	for _, ramp := range appRamps {
		ramps = append(ramps, ramp.Anchor.Name)
	}
	return ramps
}
