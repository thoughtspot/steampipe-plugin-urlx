package urlx

import (
	"context"
	"encoding/csv"
	"io"
	"strings"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"

	"net/http"
	"net"
	"time"
	"strconv"
)


func urlData(ctx context.Context, connection *plugin.Connection, dataURL string, b_get_data bool) (*plugin.Table, error) {


	cols := []*plugin.Column{}

	// b_get_data is false. All data still needs to be read for proper column data typing.
	sa_column_map, _, err := readDataHTTP(ctx, dataURL, b_get_data)
	if err != nil {
		plugin.Logger(ctx).Error("1000 [" + dataURL + "][" + err.Error() + "]")
		return nil, err
	}
	
	for s_column_name, s_column_type := range sa_column_map {
		if s_column_type == "INTEGER" {
			cols = append(cols, &plugin.Column{Name: s_column_name, Type: proto.ColumnType_INT, Transform: transform.FromField(helpers.EscapePropertyName(s_column_name))})
		} else if s_column_type == "NUMERIC" {
			cols = append(cols, &plugin.Column{Name: s_column_name, Type: proto.ColumnType_DOUBLE, Transform: transform.FromField(helpers.EscapePropertyName(s_column_name))})
		} else if s_column_type == "DATE" || s_column_type == "TIMESTAMP" {
			cols = append(cols, &plugin.Column{Name: s_column_name, Type: proto.ColumnType_TIMESTAMP, Transform: transform.FromField(helpers.EscapePropertyName(s_column_name))})
		} else {
			cols = append(cols, &plugin.Column{Name: s_column_name, Type: proto.ColumnType_STRING, Transform: transform.FromField(helpers.EscapePropertyName(s_column_name))})
		}
	}


	return &plugin.Table {
		Name: dataURL,
		List: &plugin.ListConfig{
			Hydrate: listDataWithURL(dataURL, true),
		},
		Columns: cols,
	}, nil
}


func listDataWithURL (s_url string, b_get_data bool) func(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	return func(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
		_, sa_rows, _ := readDataHTTP(ctx, s_url, true)
		for _, sm_row := range sa_rows {
			d.StreamListItem(ctx, sm_row)
		}
		return nil, nil
	}
}


func readDataHTTP(ctx context.Context, s_url string, b_get_data bool) (map[string]string, []map[string]string, error) {

	plugin.Logger(ctx).Debug("readData RUNINIT")

	var sa_columns [] string
	var sa_data []map[string]string

	req, err := http.NewRequest(http.MethodGet, s_url, nil)
    if err != nil {
		plugin.Logger(ctx).Error("readData req Error [001][" + s_url + "][" + err.Error() + "]")
    }

	tr := &http.Transport{
		Dial: (&net.Dialer{
			Timeout:   3 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		TLSHandshakeTimeout:   3 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		IdleConnTimeout: 5 * time.Second,
		// KeepAlive: 30 * time.Second,
		DisableKeepAlives: true,
	}
	client := &http.Client{
		// Timeout: time.Second * 10,
	}
	client.Transport = tr

	resp, err := client.Do(req)
    if err != nil {
        plugin.Logger(ctx).Error("readData Error [010][" + s_url + "][" + err.Error() + "]")
    	return nil, nil, nil
    }

    defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			return
		}
	}(resp.Body)

    var sb_data strings.Builder
    var i_size_total int = 0
    var i_size_max int = 20971520
    var i_read_buff int = 4096
    var b_eof bool = false
    buff := make([]byte, i_read_buff)
    for i_size_total < i_size_max {
        var bytesRead int
        bytesRead, err = resp.Body.Read(buff)
        if err == io.EOF {
        	err = nil
            b_eof = true
            if bytesRead <= 0 {  
                break  
            }
        }
        if err != nil {
        	plugin.Logger(ctx).Error("readData  Error [020][" + s_url + "][" + err.Error() + "]")
        	return nil, nil, err
        }
        s_data := string(buff[:bytesRead])
        s_data = strings.Replace(s_data, "\r\n", "\n", -1) // handle DOS/Windows newlines
        s_data = strings.Replace(s_data, "\r", "\n", -1) // handle DOS/Windows newlines
        // i_size_total = len(sb_data.String())
        i_size_total = (i_size_total + bytesRead)
        plugin.Logger(ctx).Debug("readData Read [2000][" + s_url + "][" + strconv.Itoa(i_size_total) + "]")
        sb_data.WriteString(sanitizeUTF8(s_data))
    }


	s_final_data := sb_data.String()
    sb_data.Reset()

    if b_eof == false {
        var i_final_newline int = strings.LastIndex(s_final_data, "\n")
        s_final_data = s_final_data[0:i_final_newline]
    }

    // use detector to detect the file delimiter
    detector := New()
	sampleLines := 4
	detector.Configure(&sampleLines, nil)
	delimiters := detector.DetectDelimiter(strings.NewReader(s_final_data), '"')

	if len(delimiters) == 0 {
    	return nil, nil, nil
    }

	// read data into csv to take advantage of csv functions
    nr := csv.NewReader(strings.NewReader(s_final_data))
    s_final_data = ""
    s_separator := strings.Replace(delimiters[0], "/", "//", -1)
    nr.Comma = GetSeparator(s_separator)

    plugin.Logger(ctx).Debug("readData Separator [2100][" + s_url + "][" + s_separator + "]")

    // get every record for purposes of column data type determination and hydration (depending on b_get_data boolean)
	records, err := nr.ReadAll()
	if err != nil {
		plugin.Logger(ctx).Error(err.Error())
		return nil, nil, err
	}

	plugin.Logger(ctx).Debug("readData [2200][" + s_url + "][" + s_separator + "]")

	// grab all of the columns from the first row of the file
	for _, s_value := range records[0] {
		sa_columns = append(sa_columns, s_value)
	}

	plugin.Logger(ctx).Debug("readData [2300][" + s_url + "][" + s_separator + "]")

	// build the column map with data types
	sa_column_map := make(map[string]string)
	if b_get_data == false {
		for idx, s_column := range sa_columns {
			i_false_date := 0
			i_false_integer := 0
			i_false_numeric := 0
			s_data_type := "STRING"
			for idx0, sa_row := range records {
				if idx0 == 0 {
					continue;
				}
				s_value := sa_row[idx]
				if !isDate(s_value) {
					i_false_date++
				}
				if !isInteger(s_value) {
					i_false_integer++
				}
				if !isNumeric(s_value) {
					i_false_numeric++
				}
				if i_false_date > 0 && i_false_integer > 0 && i_false_numeric > 0 {
					break
				}
			}
			if i_false_date == 0 {
				s_data_type = "DATE"
				// s_data_type = "STRING"
			} else if i_false_integer == 0 {
				s_data_type = "INTEGER"
			} else if i_false_numeric == 0 {
				s_data_type = "NUMERIC"
			}
			
			sa_column_map[s_column] = s_data_type
		}
	}

	// if b_get_data is true, return the data to the streaming list hydrate function
	if b_get_data == true {
		for idx, record := range records {
			if idx == 0 {
				continue
			}
			sm_row := map[string]string{}
			for idx0, s_value := range record {
				sm_row[sa_columns[idx0]] = s_value
			}
			sa_data = append(sa_data, sm_row)
		}
	}

	plugin.Logger(ctx).Debug("readData [2400][" + s_url + "][" + s_separator + "]")

	plugin.Logger(ctx).Debug("readData Complete [" + s_url + "]")

	return sa_column_map, sa_data, err
}
