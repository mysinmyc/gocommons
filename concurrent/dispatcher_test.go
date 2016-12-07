package concurrent

import (
	"fmt"
	"testing"
	"time"
)

func TestDispatcher(pTest *testing.T) {

	vIterations := 1000
	vPrintStats := 100
	vTimeout := time.Second * 10
	vDispatcher := NewDispatcher(func(vSelf *Dispatcher, pWorkerCnt int, pValue interface{}) error {

		vValue, _ := pValue.(int)

		if vValue%vPrintStats == 0 {
			fmt.Printf("TestDispatcher <%d> Processing value %d/%d...\n", pWorkerCnt, vValue, vIterations)
		}
		if vValue < vIterations {
			vSelf.Enqueue(vValue + 1)

		}

		return nil

	}, 10)

	vEndChannel := make(chan bool)
	go func() {
		vDispatcher.Enqueue(1)
		vDispatcher.Start(4)
		vDispatcher.WaitForCompletition()
		close(vEndChannel)
	}()

	select {

	case <-vEndChannel:
		return
	case <-time.After(vTimeout):
		pTest.Fatal("Timeout occurred something doesn't works well in the dispatcher")

	}

}
