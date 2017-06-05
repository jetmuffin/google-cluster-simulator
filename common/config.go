package common

type Config struct {
	Debug     bool
	Directory string

	Cpu float64
	Mem float64

	Scheduler int

	Alpha  float64 // single exponential influence
	Beta   float64 // double exponential influence
	Theta  float64 // punish parameter
	Lambda float64 // threshold parameter
	Gamma  float64 // predictor error feedback
}
