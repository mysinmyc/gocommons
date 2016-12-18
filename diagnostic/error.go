package diagnostic

import (
	"fmt"
	"runtime/debug"
	"strings"
)

//ImprovedError custom error containing additional diagnostics information
type ImprovedError struct {
	Message string
	Stack   []byte
	Cause   error
}

func (vSelf *ImprovedError) Error() string {

	vRis := "--- ERROR MESSAGE: " + vSelf.Message

	if vSelf.Stack != nil {

		vRis += "\n\n\t--- AT ---\n"

		vLines := strings.Split(fmt.Sprintf("%s", vSelf.Stack), "\n")
		vStart := 6
		if len(vLines) < 6 {
			vStart = 0
		}
		for _, vCurLine := range vLines[vStart:] {
			vRis += vCurLine + "\n"
		}

	}

	if vSelf.Cause != nil {
		vRis += fmt.Sprintf("\n\n\t--- Caused by: %T ---\n", vSelf.Cause)
		vRis += vSelf.Cause.Error()
	}

	return vRis
}

func (vSelf *ImprovedError) String() string {
	return vSelf.Message
}

//NewError create an error
//PARAMETERS:
// pMessage = error message to be formatted
// pCause = optional, the original Cause
// pFormat = optional, parameters to format error message
//RETURNS:
// An improved errror
func NewError(pMessage string, pCause error, pFormat ...interface{}) *ImprovedError {

	var vStack []byte
	vRootError:=false
	if pCause == nil {
		vStack = debug.Stack()
		vRootError=true
	} else {
		_, vIsCauseAnImprovedError := pCause.(*ImprovedError)

		if vIsCauseAnImprovedError == false {
			vStack = debug.Stack()
			vRootError=true
		}
	}

	vRis:= &ImprovedError{Message: fmt.Sprintf(pMessage, pFormat...), Cause: pCause, Stack: vStack}
	if vRootError && IsLogSupport() {
		log(LogLevel_Support,"NewError","Root error intercepted",vRis)
	}
	return vRis
	
}

type NotAnError struct {
	error
}

//GetMainError return the original error
//Parameters:
// pError = error to unwrap
// pAllowNil = if true return nil in case of no error, otherwise a dummy arror (to avoid panic)
func GetMainError(pError error, pAllowNil bool) error {

	if pError == nil {
		if pAllowNil {
			return nil
		} else {
			return &NotAnError{}
		}
	}

	vImprovedError, vIsAnImprovedError := pError.(*ImprovedError)

	if vIsAnImprovedError && vImprovedError.Cause != nil {
		return GetMainError(vImprovedError.Cause, pAllowNil)
	}

	return pError
}
