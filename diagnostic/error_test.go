package diagnostic

import "testing"

/*
func TestError(pTest *testing.T) {

	vOriginal := errors.New("Original error")

	vError := NewError("errore", vOriginal)
	for vCnt := 0; vCnt < 3; vCnt++ {
		vError = NewError("errore %d", vError, vCnt)
	}

	fmt.Printf("STRING: %s \nVALUE: %#v \n", vError, *vError)
}
*/

type CustomError struct {
	error
}

func TestGetMainError(pTest *testing.T) {

	_, vIsRightType := GetMainError(&CustomError{}, false).(*CustomError)

	if vIsRightType == false {
		pTest.Fatal("something wrong in error type assertion")
	}

	vError := NewError("aaa", NewError("bbb", &CustomError{}))

	_, vIsRightType = GetMainError(vError, false).(*CustomError)

	if vIsRightType == false {
		pTest.Fatal("something wrong in error type assertion of Improved errors")
	}

}
