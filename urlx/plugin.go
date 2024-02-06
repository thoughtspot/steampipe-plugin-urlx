package urlx

import (
	"context"
	"sync"

	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"

)

func Plugin(ctx context.Context) *plugin.Plugin {
	p := &plugin.Plugin{
		Name: "steampipe-plugin-urlx",
		ConnectionConfigSchema: &plugin.ConnectionConfigSchema{
			NewInstance: ConfigInstance,
		},
		DefaultTransform: transform.FromGo().NullIfZero(),
		SchemaMode:       plugin.SchemaModeDynamic,
		TableMapFunc:     PluginTables,
	}
	return p
}

type key string

const (
	// keyURL has been added to avoid key collisions
	keyURL key = "url"
)


func PluginTables(ctx context.Context, d *plugin.TableMapData) (map[string]*plugin.Table, error) {

	tables := map[string]*plugin.Table{}

	// Grab URLs from Config to create as tables
	csvConfig := GetConfig(d.Connection)
	if csvConfig.URLs == nil {
		plugin.Logger(ctx).Error("There are no URLs in the Config File.")
		return nil, nil
	}
	urls := csvConfig.URLs

	var wg sync.WaitGroup

	for _, url := range urls {

		wg.Add(1)

		go func(s_url string) {
			defer wg.Done()
			tableData, err := urlData(ctx, d.Connection, s_url, false)
			if err != nil {
				plugin.Logger(ctx).Error("url.PluginTables", "create_table_error", err, "url", s_url)
				tableData = nil
			}
			if tableData != nil {
				tables[s_url] = tableData
			}
		}(url)

	}
	wg.Wait()


	return tables, nil

}
