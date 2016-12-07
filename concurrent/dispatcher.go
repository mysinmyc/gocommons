package concurrent

import (
	"fmt"
	"log"
	"sync"
	"time"
)

const (
	STATUS_READY   = 0
	STATUS_STARTED = iota
	STATUS_ENDING  = iota
)

//ConsumerFunc signature of consumer functions
//Parameters:
//	*Dispatcher = dispatcher instance
//	int = worker id
//  interface{} = item
//Returns
//	nil if succeded otherwise an error
type ConsumerFunc func(*Dispatcher, int, interface{}) error

//ErrorHandlerFunc signature for item error handlers
//Parameters:
//	*Dispatcher = dispatcher instance
//	int = worker id
//  interface{} = item
// 	error = error occurred
type ErrorHandlerFunc func(*Dispatcher, int, interface{}, error)

//Dispatcher is an object that can be used to enqueue multithreading operations
// it is studied for recursive operations, so it's safe for consumers to enqueue new data
type Dispatcher struct {
	pendingItems     []interface{}
	consumerFunc     ConsumerFunc
	errorHandlerFunc ErrorHandlerFunc
	mux              sync.Mutex
	runningWorkers   sync.WaitGroup
	status           int
	batchSize        int
}

//NewDispatcher create a new dispatcher
//Parameters:
// pConsumerFunc = consumer function
// pBatchSize = number of items thata worker thread can dequeue per time
func NewDispatcher(pConsumerFunc ConsumerFunc, pBatchSize int) *Dispatcher {
	return &Dispatcher{pendingItems: make([]interface{}, 0, pBatchSize), consumerFunc: pConsumerFunc, batchSize: pBatchSize}
}

func (vSelf *Dispatcher) dequeue() []interface{} {

	vSelf.mux.Lock()
	defer vSelf.mux.Unlock()
	vRis := vSelf.pendingItems
	vLen := len(vRis)

	if vLen < vSelf.batchSize {
		vSelf.pendingItems = make([]interface{}, 0, vSelf.batchSize)
		return vRis[0:vLen]
	}

	vSelf.pendingItems = vSelf.pendingItems[vSelf.batchSize:]
	return vRis[0:vSelf.batchSize]

}

//Enqueue items
//Parameters:
// pItems = Items to enqueue
func (vSelf *Dispatcher) Enqueue(pItems ...interface{}) {
	vSelf.mux.Lock()
	defer vSelf.mux.Unlock()
	vSelf.pendingItems = append(vSelf.pendingItems, pItems...)

}

func (vSelf *Dispatcher) worker(pCntWorker int) {
	for {
		vItems := vSelf.dequeue()
		if len(vItems) > 0 {

			for _, vCurItem := range vItems {

				vError := vSelf.consumerFunc(vSelf, pCntWorker, vCurItem)

				if vError != nil {
					if vSelf.errorHandlerFunc == nil {
						log.Printf("<%d> ATTENTION: an error occurred processing item %v: %v ", pCntWorker, vCurItem, vError)
					} else {
						vSelf.errorHandlerFunc(vSelf, pCntWorker, vCurItem, vError)
					}
				}
			}
		} else {
			if vSelf.status >= STATUS_ENDING {
				vSelf.runningWorkers.Done()
				return
			}
			time.Sleep(time.Microsecond * 100)
		}
	}
}

//SetErrorHandler set the error handling functions
//Parameters:
// pErrorHandlerFunc = error handler function
func (vSelf *Dispatcher) SetErrorHandler(pErrorHandlerFunc ErrorHandlerFunc) {
	vSelf.errorHandlerFunc = pErrorHandlerFunc
}

//Start dispatching threads
//Parameters:
// pNumWorker = number of worker threads
//Returns:
// nil in case of success
func (vSelf *Dispatcher) Start(pNumWorker int) error {

	if vSelf.status != STATUS_READY {
		return fmt.Errorf("Dispatcher in status %d", vSelf.status)
	}

	vSelf.status = STATUS_STARTED
	for vCnt := 0; vCnt < pNumWorker; vCnt++ {
		vSelf.runningWorkers.Add(1)
		go vSelf.worker(vCnt)
	}

	return nil
}

//WaitForCompletition wait for activity completition and notifies workers to stop
func (vSelf *Dispatcher) WaitForCompletition() {
	if vSelf.status < STATUS_STARTED {
		return
	}

	vSelf.status = STATUS_ENDING
	vSelf.runningWorkers.Wait()
	vSelf.status = STATUS_READY
}
