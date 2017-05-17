package base

import "strconv"

func stringToInt64(str string, defaults int64) int64 {
	i, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return defaults
	}
	return i
}

func stringToFloat64(str string, defaults float64) float64 {
	f, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return defaults
	}
	return f
}

func stringToBool(str string, defaults bool) bool {
	if str == "0" {
		return false
	} else if str == "1" {
		return true
	} else {
		return defaults
	}
}
