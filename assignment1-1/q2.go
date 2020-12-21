package cos418_hw1_1

import (
	"bufio"
	"io"
	"os"
	"strconv"
)

// Sum numbers from channel `nums` and output sum to `out`.
// You should only output to `out` once.
// Do NOT modify function signature.
func sumWorker(nums chan int, out chan int) {
	sum := 0
	for num := range nums {
		sum += num
	}
	out <- sum
}

// Read integers from the file `fileName` and return sum of all values.
// This function must launch `num` go routines running
// `sumWorker` to find the sum of the values concurrently.
// You should use `checkError` to handle potential errors.
// Do NOT modify function signature.
func sum(num int, fileName string) int {
	// First, open the file and scan the numbers into []int.
	f, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	ints, err := readInts(f)
	if err != nil {
		panic(err)
	}

	// Next, create our `out` and `nums` channels. Create `num` workers and send
	// the integers into the `nums` channel.
	out := make(chan int, len(ints))
	nums := make(chan int, num)
	for i := 0; i < num; i++ {
		go sumWorker(nums, out)
	}
	for _, i := range ints {
		nums <- i
	}
	close(nums)

	// Since we know that we only send from each of the workers once into `out`,
	// we only need to pull off of `out` `num` times.
	sum := 0
	for i := 0; i < num; i++ {
		n := <-out
		sum += n
	}
	close(out)

	return sum
}

// Read a list of integers separated by whitespace from `r`.
// Return the integers successfully read with no error, or
// an empty slice of integers and the error that occurred.
// Do NOT modify this function.
func readInts(r io.Reader) ([]int, error) {
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanWords)
	var elems []int
	for scanner.Scan() {
		val, err := strconv.Atoi(scanner.Text())
		if err != nil {
			return elems, err
		}
		elems = append(elems, val)
	}
	return elems, nil
}
