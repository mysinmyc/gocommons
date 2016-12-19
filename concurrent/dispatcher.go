package concurrent

import (
	"sync"
	"time"

	"github.com/mysinmyc/gocommons/diagnostic"
)

const (
	STATUS_READY   = 0
	STATUS_STARTED = iota
	STATUS_ENDING  = iota
)

type WorkerLocals interface{}

//ConsumerFunc signature of consumer functions
//Parameters:
//	*Dispatcher = dispatcher instance
//	int = worker id
//  interface{} = item
//  workerLocals = worker local variables
//Returns
//	nil if succeded otherwise an error
type ConsumerFunc func(*Dispatcher, int, interface{}, WorkerLocals) error

//ErrorHandlerFunc signature for item error handlers
//Parameters:
//	*Dispatcher = dispatcher instance
//	int = worker id
//  interface{} = item
// 	error = error occurred
//  workerLocals = worker local variables
//Returns:
//  true in case the error has been recovered
type ErrorHandlerFunc func(*Dispatcher, int, interface{}, error, WorkerLocals) bool

type WorkerLifeCycleEvent int

const (
	WorkerLifeCycleEvent_None    WorkerLifeCycleEvent = 0
	WorkerLifeCycleEvent_Started WorkerLifeCycleEvent = iota
	WorkerLifeCycleEvent_Stopped WorkerLifeCycleEvent = iota
)

type WorkerLifeCycleFunc func(*Dispatcher, int, WorkerLifeCycleEvent, WorkerLocals) (WorkerLocals, error)

//Dispatcher is an object that can be used to enqueue multithreading operations
// it is studied for recursive operations, so it's safe for consumers to enqueue new data
type Dispatcher struct {
	pendingItems               []interface{}
	consumerFunc               ConsumerFunc
	ErrorHandlerFunc           ErrorHandlerFunc
	itemsLock                  *sync.Mutex
	runningWorkers             sync.WaitGroup
	runningWorkersCounter      *Counter
	status                     int
	batchSize                  int
	failed                     bool
	workersLocals              []WorkerLocals
	WorkerLifeCycleHandlerFunc WorkerLifeCycleFunc
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

	vRis := vSelf.pendingItems
	vLen := len(vRis)
	if vLen == 0 {
		return []interface{}{}
	}

	vSelf.itemsLock.Lock()
	defer vSelf.itemsLock.Unlock()
	vRis = vSelf.pendingItems
	vLen = len(vRis)

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

	if vSelf.WorkerLifeCycleHandlerFunc != nil {
		diagnostic.LogInfo("Dispatcher.worker", "performing initialization of worker %d", pCntWorker)
		vNewWorkerLocals, vInitError := vSelf.WorkerLifeCycleHandlerFunc(vSelf, pCntWorker, WorkerLifeCycleEvent_Started, nil)
		vSelf.workersLocals[pCntWorker] = vNewWorkerLocals
		if vInitError != nil {
			vSelf.runningWorkers.Done()
			diagnostic.LogError("Dispatcher.worker", "failed to init worker", vInitError)
			return
		}

	}

	for {

		vItems := vSelf.dequeue()

		if len(vItems) > 0 {
			vSelf.runningWorkersCounter.IncreaseBy(1)
			for _, vCurItem := range vItems {

				vError := vSelf.consumerFunc(vSelf, pCntWorker, vCurItem, vSelf.workersLocals[pCntWorker])
				if vError != nil {
					vSelf.onItemError(vCurItem, vError, pCntWorker, vSelf.workersLocals[pCntWorker])
				}

			}
			vSelf.runningWorkersCounter.IncreaseBy(-1)
		} else {

			if vSelf.IsWorking() == false && vSelf.status >= STATUS_ENDING {

				var vEndWorkerError error
				if vSelf.WorkerLifeCycleHandlerFunc != nil {
					_, vEndWorkerError = vSelf.WorkerLifeCycleHandlerFunc(vSelf, pCntWorker, WorkerLifeCycleEvent_Stopped, vSelf.workersLocals[pCntWorker])
				}

				vSelf.runningWorkers.Done()
				if vEndWorkerError != nil {
					diagnostic.LogError("Dispatcher.worker", "failed to stop worker", vEndWorkerError)
					return
				}

				return
			}
			time.Sleep(time.Millisecond * 50)
		}

	}
}

func (vSelf *Dispatcher) onItemError(pItem interface{}, pError error, pCntWorker int, pWorkerLocals WorkerLocals) {

	if vSelf.ErrorHandlerFunc == nil {
		diagnostic.LogWarning("Dispatcher.onItemError", "worker %d failed to process item %v", pError, pCntWorker, pItem)
	} else {
		vRecovered := vSelf.ErrorHandlerFunc(vSelf, pCntWorker, pItem, pError, pWorkerLocals)
		if vRecovered == false {
			vSelf.failed = true
		}
	}
}

//SetErrorHandler set the error handling functions
//Parameters:
// pErrorHandlerFunc = error handler function
func (vSelf *Dispatcher) SetErrorHandler(pErrorHandlerFunc ErrorHandlerFunc) {
	vSelf.ErrorHandlerFunc = pErrorHandlerFunc
}

//Start dispatching threads
//Parameters:
// pNumWorkers = number of worker threads
//Returns:
// nil in case of success
func (vSelf *Dispatcher) Start(pNumWorkers int) error {

	if vSelf.status != STATUS_READY {
		return diagnostic.NewError("Dispatcher in status %d", nil, vSelf.status)
	}

	vSelf.status = STATUS_STARTED
	vSelf.failed = false

	vSelf.workersLocals = make([]WorkerLocals, pNumWorkers)
	for vCnt := 0; vCnt < pNumWorkers; vCnt++ {
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
