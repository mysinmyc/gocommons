package diagnostic

import (
	"errors"
	"fmt"
	"testing"
)

func TestError(pTest *testing.T) {

	vOriginal := errors.New("Original error")

	vError := NewError("errore", vOriginal)
	for vCnt := 0; vCnt < 3; vCnt++ {
		vError = NewError("errore %d", vError, vCnt)
	}

	fmt.Printf("STRING: %s \nVALUE: %#v \n", vError, *vError)
}
