package files

import "strings"

var PandasEmptyValues = map[string]bool {
	"null": true,
	"NULL": true,
	"Null": true,
	"NaN": true,
	"nan": true,
	"N/A": true,
	"n/a": true,
}

func PandasFriendly(v string) string {
	v = strings.TrimSpace(v)
	if v == "" || PandasEmptyValues[v] {
		return "#"
	}
	return v
}

func RemoveHash(v string) string {
	if v == "#" {
		return ""
	}
	return v
}

