package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"os"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce tasks that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	buffer, err := ioutil.ReadFile(inFile)
	checkError(err)
	kvs := mapF(inFile, string(buffer))

	createIntermediateFiles(jobName, mapTaskNumber, nReduce)
	for _, kv := range kvs {
		writeKeyToFile(kv, jobName, mapTaskNumber, nReduce)
	}

}

// ihash determines which file a given key belongs to.
func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func createIntermediateFiles(jobName string, mapTaskNumber, nReduce int) {
	for i := 0; i < nReduce; i++ {
		fn := reduceName(jobName, mapTaskNumber, i)
		f, err := os.Create(fn)
		checkError(err)
		f.Close()
	}
}

func writeKeyToFile(kv KeyValue, jobName string, mapTaskNumber, nReduce int) {
	target := ihash(kv.Key) % uint32(nReduce)
	fn := reduceName(jobName, mapTaskNumber, int(target))
	f, err := os.OpenFile(fn, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	checkError(err)
	defer f.Close()
	enc := json.NewEncoder(f)
	err = enc.Encode(&kv)
	checkError(err)
}
