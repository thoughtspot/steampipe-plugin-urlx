package urlx

import (
	"strconv"
	"regexp"
	"strings"
)

func isInteger(s string) bool {
	_, err := strconv.ParseInt(s, 0, 64)
	return err == nil
}

func isNumeric(s string) bool {
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

func isDate(s string) bool {
	match := false
	r0, _ := regexp.Compile("^\\d{4}-\\d{2}-\\d{2}$")
	r1, _ := regexp.Compile("^\\d{2}/\\d{2}/\\d{4}$")
	if r0.MatchString(s) ||
		r1.MatchString(s) {
		match = true
	}
	return match
}

func sanitizeUTF8(s string) string {
    var b strings.Builder
    for _, c := range s {
        if c == '\uFFFD' {
            continue
        }
        b.WriteRune(c)
    }
    return b.String()
}

func GetSeparator(s string) rune {
	var sep string
	s = `'` + s + `'`
	sep, _ = strconv.Unquote(s) // please handle the err properly
	return ([]rune(sep))[0]
}
