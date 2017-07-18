package freetds

import (
	"strconv"
	"strings"
)

type credentials struct {
	user, pwd, host, database, mirrorHost, compatibility string
	maxPoolSize, lockTimeout                             int
}

// NewCredentials fills credentials stusct from connection string
func NewCredentials(connStr string) *credentials {
	parts := strings.Split(connStr, ";")
	crd := &credentials{maxPoolSize: 100}
	for _, part := range parts {
		kv := strings.SplitN(part, "=", 2)
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
			case "compatibility_mode", "compatibility mode", "compatibility":
				crd.compatibility = strings.ToLower(value)
			case "lock timeout", "lock_timeout":
				if i, err := strconv.Atoi(value); err == nil {
					crd.lockTimeout = i
				}
			}

		}
	}
	return crd
}
