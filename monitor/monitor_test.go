package monitor

import (
	"testing"
	"math"
)

var (
	series = []float64{6.4, 5.6, 7.8, 8.8, 11.0, 11.6, 16.7, 15.3, 21.6, 22.4}

)

func TestExponentialSmoothing(t *testing.T) {
	result := exponentialSmoothing(series, 0.3)
	if math.Abs(result[1] - 6.16) > 0.00001  {
		t.Log(result[1])
		t.Error("Single expoential smoothing compute error ")
	}
}

func TestDoubleExponentialSmoothing(t *testing.T) {
	result := doubleExponentialSmoothing(series, 0.3, 0.3)
	if math.Abs(result[1] - 4.8) > 0.00001  {
		t.Log(result[1])
		t.Error("Double expoential smoothing compute error ")
	}
}