package persistency

import (
	"encoding/json"
	"os"
	"bytes"
	"fmt"
	"io/ioutil"
	"github.com/mysinmyc/gocommons/diagnostic"
)


type BeanNotFoundError struct {
        error
       	BeanID string
}

func (vSelf *BeanNotFoundError) Error() string {
        return fmt.Sprintf("Bean with id %s not found", vSelf.BeanID)
}


func LoadBeanFromFile(pFile string, pBean interface{}) error {

	vFileContent,vFileContentError:=ioutil.ReadFile(pFile)
	if os.IsNotExist(vFileContentError) {
		return &BeanNotFoundError{}
	}	
	if vFileContentError != nil {
		return diagnostic.NewError("error while reading file %s", vFileContentError, pFile)
	}

	vUnmarshalError := json.Unmarshal(vFileContent, pBean)

	if vUnmarshalError != nil {
		return diagnostic.NewError("error while unmarshalling file %s", vFileContentError, pFile)
	}

	return nil
}

func SaveBeanIntoFile(pBean interface{},pFile string ) error {
	
	vFile,vFileError:=os.Create(pFile)

	if vFileError != nil {
		return diagnostic.NewError("error while opening file %s", vFileError, pFile)
	}
	defer vFile.Close()

	vMarshalledBean, vMarshallingError := json.Marshal(pBean)

	if vMarshallingError != nil {
		return diagnostic.NewError("Error while marshalling bean to json",vMarshallingError)
	}

	var vNiceJson bytes.Buffer
	json.Indent(&vNiceJson,vMarshalledBean,"","\t")


	_,vWriteError:=vNiceJson.WriteTo(vFile)
	if vWriteError != nil {
		return diagnostic.NewError("Error while marshalling bean to json",vWriteError)
	}

	return nil
}


func IsBeanNotFound(pError error) bool {
	if pError == nil {
		return false
	}

	vMainError := diagnostic.GetMainError(pError,false)

	_,vIsBeanNotFound := vMainError.(*BeanNotFoundError)

	return vIsBeanNotFound
}
