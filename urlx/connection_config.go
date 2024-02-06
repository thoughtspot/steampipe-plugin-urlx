package urlx

import (
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
)

type urlConfig struct {
	URLs []string `hcl:"urls"`
	Separator *string `hcl:"separator"`
	Comment *string `hcl:"comment"`
	Header *string `hcl:"header"`
}

func ConfigInstance() interface{} {
	return &urlConfig{}
}

// GetConfig :: retrieve and cast connection config from query data
func GetConfig(connection *plugin.Connection) urlConfig {

	if connection == nil || connection.Config == nil {
		return urlConfig{}
	}
	config, _ := connection.Config.(urlConfig)
	return config
}
