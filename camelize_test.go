package freetds

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCamelize(t *testing.T) {
	assert.Equal(t, "DinoParty", camelize("dino_party"))
	assert.Equal(t, "PonudaId", camelize("ponuda_id"))
	assert.Equal(t, "IsoMedoUDucan", camelize("iso_medo_u_ducan"))
	assert.Equal(t, "IsoMedoUDucan", camelize("iso-medo-u-ducan"))
	assert.Equal(t, "IsoMedoUDucan", camelize("isoMedoUDucan"))
	assert.Equal(t, "IsoMedoUDucan", camelize("iso_medo-uDucan"))
	assert.Equal(t, "IPAddress", camelize("IPAddress"))
	assert.Equal(t, "IpAddress", camelize("ip_address"))
	assert.Equal(t, "SomeFieldName", camelize("some-field:name"))
}
