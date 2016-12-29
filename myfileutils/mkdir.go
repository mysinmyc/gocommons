package myfileutils

import (
	"os"
	"strings"
	"sync"
	"github.com/mysinmyc/gocommons/diagnostic"
)

var (
	_Mutex sync.Mutex 
	_Dirs=make(map[string]bool)
)


func GetParentPath(pPath string) string {
	
	vLastSlash:=strings.LastIndex(pPath, "/")

	if vLastSlash>1 {
		return pPath[:vLastSlash]
	}

	return "/"

}

func MkDir(pPath string) error {
	if diagnostic.IsLogTrace() {
		diagnostic.LogTrace("MkDir", pPath)
	}
	_Mutex.Lock()
	vRis:= mkDir(pPath)
	_Mutex.Unlock()
	return vRis
}

func mkDir(pPath string) error {

	if _Dirs[pPath]== true {
		return  nil
	}

	vLastSlash:=strings.LastIndex(pPath, "/")

	if vLastSlash>0 {
		vParentError:=mkDir(pPath[:vLastSlash])
		if vParentError!=nil  {
			return vParentError
		}
	}
	vError:=os.MkdirAll(pPath, 0700)

	if vError !=nil {
		return diagnostic.NewError("Error creating folder %s",vError,pPath)
	}
	_Dirs[pPath]=true
	return nil	
}
