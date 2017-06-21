package myfileutils

import (
	"io"
	"os"
	"log"
	"github.com/mysinmyc/gocommons/diagnostic"
)

func CopyFile(pSource,pDestination string) error {

	vInputFile,vError:=os.Open(pSource) 
	if vError!=nil {
		return diagnostic.NewError("Failed to open source file %s",vError,pSource)
	}
	
	defer vInputFile.Close()

	vOutputFile,vError:=os.Create(pDestination)
	if vError!=nil {
		return diagnostic.NewError("Failed to create destination file %s",vError,pDestination)
	}
	
	defer vOutputFile.Close()

	_,vError:=io.Copy(vOutputFile,vInputFile)
	if vError != nil {
		return diagnostic.NewError("Failed to copy %s into %s",vError,pSource,pDestination)
	}

	vError=vOutputFile.Sync()
	if vError != nil {
		return diagnostic.NewError("Failed to write into destination file %s",vError,pDestination)
	}

	vError=vOutputFile.Close()
	if vError != nil {
		return diagnostic.NewError("Failed to write into destination file %s",vError,pDestination)
	}

	return nil
}
