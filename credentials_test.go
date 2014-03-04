package freetds

import (
	"github.com/stretchrcom/testify/assert"
	"testing"
)

func TestParseConnectionString(t *testing.T) {
	validConnStrings := []string{
		"Server=myServerAddress;Database=myDataBase;User Id=myUsername;Password=myPassword;Failover Partner=myMirror;Max Pool Size=200",
		"Server=myServerAddress;Database=myDataBase;User_Id=myUsername;Password=myPassword;Failover_Partner=myMirror;Max_Pool_Size=200",
		"server=myServerAddress;database=myDataBase;user_id=myUsername;password=myPassword;failover_partner=myMirror;max_pool_size=200",
		"host=myServerAddress;database=myDataBase;user=myUsername;pwd=myPassword;mirror=myMirror;max_pool_size=200",
		"host=myServerAddress;database=myDataBase;user=myUsername;pwd=myPassword;mirror=myMirror;max_pool_size=200",
	}
	for _, connStr := range validConnStrings {
		testCredentials(t, NewCredentials(connStr))
	}
}

func testCredentials(t *testing.T, crd *credentials) {
	assert.NotNil(t, crd)
	assert.Equal(t, "myServerAddress", crd.host)
	assert.Equal(t, "myDataBase", crd.database)
	assert.Equal(t, "myUsername", crd.user)
	assert.Equal(t, "myPassword", crd.pwd)
	assert.Equal(t, "myMirror", crd.mirrorHost)
	assert.Equal(t, 200, crd.maxPoolSize)
}
