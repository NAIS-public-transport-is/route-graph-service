package helper

import (
	"fmt"
)

func AnyToInt32(v any) int32 {
	switch t := v.(type) {
	case int32:
		return t
	case int:
		return int32(t)
	case int64:
		return int32(t)
	case float64:
		return int32(t)
	default:
		return 0
	}
}

func AnyToInt64(v any) int64 {
	switch t := v.(type) {
	case int64:
		return t
	case int:
		return int64(t)
	case int32:
		return int64(t)
	case float64:
		return int64(t)
	default:
		return 0
	}
}

func AnyToString(v any) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	default:
		return fmt.Sprintf("%v", t)
	}
}
