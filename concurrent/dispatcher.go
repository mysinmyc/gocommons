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
//Returns:
//  true in case the error has been recovered
type ErrorHandlerFunc func(*Dispatcher, int, interface{}, error) bool

//Dispatcher is an object that can be used to enqueue multithreading operations
// it is studied for recursive operations, so it's safe for consumers to enqueue new data
type Dispatcher struct {
	pendingItems          []interface{}
	consumerFunc          ConsumerFunc
	errorHandlerFunc      ErrorHandlerFunc
	itemsLock             *sync.Mutex
	runningWorkers        sync.WaitGroup
	runningWorkersCounter *Counter
	status                int
	batchSize             int
	failed                bool
}

//NewDispatcher create a new dispatcher
//Parameters:
// pConsumerFunc = consumer function
// pBatchSize = number of items thata worker thread can dequeue per time
func NewDispatcher(pConsumerFunc ConsumerFunc, pBatchSize int) *Dispatcher {
	vMutex := &sync.Mutex{}
	vRis := &Dispatcher{pendingItems: make([]interface{}, 0, pBatchSize), consumerFunc: pConsumerFunc, batchSize: pBatchSize, itemsLock: vMutex, runningWorkersCounter: NewCounter()}
	return vRis
}

func (vSelf *Dispatcher) dequeue() []interface{} {
	vSelf.itemsLock.Lock()
	defer vSelf.itemsLock.Unlock()
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
	vSelf.itemsLock.Lock()
	defer vSelf.itemsLock.Unlock()
	vSelf.pendingItems = append(vSelf.pendingItems, pItems...)
}

func (vSelf *Dispatcher) IsWorking() bool {
	vSelf.itemsLock.Lock()
	defer vSelf.itemsLock.Unlock()
	return len(vSelf.pendingItems) > 0 || vSelf.runningWorkersCounter.GetValue() > 0
}

func (vSelf *Dispatcher) worker(pCntWorker int) {

	for {

		vItems := vSelf.dequeue()

		//log.Printf("worker %d: %d\n", pCntWorker, len(vItems))
		if len(vItems) > 0 {
			vSelf.runningWorkersCounter.IncreaseBy(1)
			for _, vCurItem := range vItems {

				vError := vSelf.consumerFunc(vSelf, pCntWorker, vCurItem)
				if vError != nil {
					vSelf.onItemError(vCurItem, vError, pCntWorker)
				}

			}
			vSelf.runningWorkersCounter.IncreaseBy(-1)
		} else {

			if vSelf.IsWorking() == false && vSelf.status >= STATUS_ENDING {
				vSelf.runningWorkers.Done()
				return
			}
			time.Sleep(time.Millisecond * 50)
		}

	}
}

func (vSelf *Dispatcher) onItemError(pItem interface{}, pError error, pCntWorker int) {

	if vSelf.errorHandlerFunc == nil {
		log.Printf("<%d> ATTENTION: an error occurred processing item %v: %v ", pCntWorker, pItem, pError)
	} else {
		vRecovered := vSelf.errorHandlerFunc(vSelf, pCntWorker, pItem, pError)
		if vRecovered == false {
			vSelf.failed = true
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
	vSelf.failed = false
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

	for {
		if vSelf.IsWorking() == false {
			vSelf.status = STATUS_ENDING
			break
		}
		time.Sleep(time.Millisecond * 100)
	}
	vSelf.runningWorkers.Wait()
	vSelf.status = STATUS_READY
}

//IsSucceded returns true if the operation is succeded. It must be requested only after WaitForCompletition method invocation
func (vSelf *Dispatcher) IsSucceded() bool {
	return vSelf.failed == false
}
