package freetds

import (
	"testing"
)

func TestParseConnectionString(t *testing.T) {
	str1 := "Server=myServerAddress;Database=myDataBase;User Id=myUsername;Password=myPassword;Failover Partner=myMirror"
	testCredentials(t, parseConnectionString(str1))

	alternateNaming := "Server=myServerAddress;Database=myDataBase;User_Id=myUsername;Password=myPassword;Failover_Partner=myMirror"
	testCredentials(t, parseConnectionString(alternateNaming))

	alternateCasing := "server=myServerAddress;database=myDataBase;user_id=myUsername;password=myPassword;failover_partner=myMirror"
	testCredentials(t, parseConnectionString(alternateCasing))

	alternateNaming2 := "host=myServerAddress;database=myDataBase;user=myUsername;pwd=myPassword;mirror=myMirror"
	testCredentials(t, parseConnectionString(alternateNaming2))
}

func testCredentials(t *testing.T, crd *credentials) {
	if crd == nil {
		t.Error()
	}
	if crd.host != "myServerAddress" {
		t.Error()
	}
	if crd.database != "myDataBase" {
		t.Error()
	}
	if crd.user != "myUsername" {
		t.Error()
	}
	if crd.pwd != "myPassword" {
		t.Error()
	}
	if crd.mirrorHost != "myMirror" {
		t.Error()
	}
}
