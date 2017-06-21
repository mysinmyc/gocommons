package myfileutils

import (
	"os"
	"github.com/mysinmyc/gocommons/diagnostic"
)

func SafeMoveFile(pSource, pDestination string) error {

	var vError error
	
	if vError=CopyFile(pSource,pDestination);vError!=nil {
		return diagnostic.NewError("Failed to copy %s into %s",vError,pSource,pDestination)
	}

	if vError=os.Remove(pSource); vError!=nil {
		return diagnostic.NewError("Failed to delete source file %s",vError,pSource)
	}

	return nil
}
