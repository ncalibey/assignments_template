package mapreduce

import (
	"encoding/json"
	"os"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	kvs := map[string][]string{}
	for i := 0; i < nMap; i++ {
		fn := reduceName(jobName, i, reduceTaskNumber)
		f, err := os.Open(fn)
		checkError(err)
		decoder := json.NewDecoder(f)
		for decoder.More() {
			kv := &KeyValue{}
			err := decoder.Decode(kv)
			checkError(err)

			// If we have seen the key, append the value. Otherwise create a new slice.
			if val, ok := kvs[kv.Key]; ok {
				kvs[kv.Key] = append(val, kv.Value)
			} else {
				kvs[kv.Key] = []string{kv.Value}
			}
		}
		f.Close()
	}

	fn := mergeName(jobName, reduceTaskNumber)
	mergeFile, err := os.Create(fn)
	checkError(err)
	defer mergeFile.Close()

	enc := json.NewEncoder(mergeFile)
	for k, v := range kvs {
		err := enc.Encode(KeyValue{k, reduceF(k, v)})
		checkError(err)
	}
}
