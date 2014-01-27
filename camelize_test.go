package freetds

import (
	"github.com/stretchrcom/testify/assert"
	"testing"
)

func TestCamelize(t *testing.T) {
	assert.Equal(t, "DinoParty", camelize("dino_party"))
	assert.Equal(t, "PonudaId", camelize("ponuda_id"))
	assert.Equal(t, "IsoMedoUDucan", camelize("iso_medo_u_ducan"))
	assert.Equal(t, "IsoMedoUDucan", camelize("iso-medo-u-ducan"))
	assert.Equal(t, "IsoMedoUDucan", camelize("isoMedoUDucan"))
	assert.Equal(t, "IsoMedoUDucan", camelize("iso_medo-uDucan"))
}
