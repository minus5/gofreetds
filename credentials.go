package freetds

import (
	"strconv"
	"strings"
)

type credentials struct {
	user, pwd, host, database, mirrorHost string
	maxPoolSize                           int
}

func NewCredentials(connStr string) *credentials {
	parts := strings.Split(connStr, ";")
	crd := &credentials{maxPoolSize: 100}
	for _, part := range parts {
		kv := strings.Split(part, "=")
		if len(kv) == 2 {
			key := strings.ToLower(strings.Trim(kv[0], " "))
			value := kv[1]
			switch key {
			case "server", "host":
				crd.host = value
			case "database":
				crd.database = value
			case "user id", "user_id", "user":
				crd.user = value
			case "password", "pwd":
				crd.pwd = value
			case "failover partner", "failover_partner", "mirror", "mirror_host", "mirror host":
				crd.mirrorHost = value
			case "max pool size", "max_pool_size":
				if i, err := strconv.Atoi(value); err == nil {
					crd.maxPoolSize = i
				}
			}
		}
	}
	return crd
}
