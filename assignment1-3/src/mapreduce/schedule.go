package mapreduce

import (
	"fmt"
	"sync"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// schedule only needs to tell the workers the name of the original input file
	// (mr.files[task]) and the task `task`; each worker knows from which files to read
	// its input and to which files to write its output. The master tells the worker
	// about a new task by sending it the RPC call Worker.DoTask, giving a DoTaskArgs
	// object as the RPC argument.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	var wg sync.WaitGroup
	wg.Add(ntasks)
	for i := 0; i < ntasks; i++ {
		go mr.callRPC(i, nios, &wg, phase)

	}
	wg.Wait()
	debug("all tasks completed\n")
}

func (mr *Master) callRPC(i, nios int, wg *sync.WaitGroup, phase jobPhase) {
	w := <-mr.registerChannel
	args := &DoTaskArgs{
		JobName:       mr.jobName,
		File:          mr.files[i],
		Phase:         phase,
		TaskNumber:    i,
		NumOtherPhase: nios,
	}
	ok := call(w, "Worker.DoTask", args, &struct{}{})
	if !ok {
		fmt.Printf("Master: RPC %s do_task error\n", w)
	}
	go func() {
		mr.registerChannel <- w
	}()
	wg.Done()
}
