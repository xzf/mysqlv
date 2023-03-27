package mysql

type dbError struct {
	code string
	err  error
}

func newDbErr(code string, err error) dbError {
	return dbError{
		code: code,
		err:  err,
	}
}

func (err dbError) Error() string {
	return err.code + " " + err.err.Error()
}
