package worker

type PanicError struct {
	Err             any
	PanicStackTrace []byte
}
