package main

import (
        "urlx"
        "github.com/turbot/steampipe-plugin-sdk/v5/plugin"
)

func main() {
        plugin.Serve(&plugin.ServeOpts{PluginFunc: urlx.Plugin})
}
