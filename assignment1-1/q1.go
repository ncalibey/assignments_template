package cos418_hw1_1

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
)

var filter = regexp.MustCompile(`[^0-9a-zA-Z]+`)

// Find the top K most common words in a text document.
// 	path: location of the document
//	numWords: number of words to return (i.e. k)
//	charThreshold: character threshold for whether a token qualifies as a word,
//		e.g. charThreshold = 5 means "apple" is a word but "pear" is not.
// Matching is case insensitive, e.g. "Orange" and "orange" is considered the same word.
// A word comprises alphanumeric characters only. All punctuation and other characters
// are removed, e.g. "don't" becomes "dont".
// You should use `checkError` to handle potential errors.
func topWords(path string, numWords int, charThreshold int) []WordCount {
	// TODO: implement me
	// HINT: You may find the `strings.Fields` and `strings.ToLower` functions helpful
	// HINT: To keep only alphanumeric characters, use the regex "[^0-9a-zA-Z]+"
	seen := map[string]int{}

	// First we open up the file and scan over each of the wordCounts.
	f, err := os.Open(path)
	if err != nil {
		panic("no file found at supplied path")
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanWords)
	for scanner.Scan() {
		// We grab the word and convert it to lowercase. We then strip it of punctuation.
		// If the length of the word is less than `charThreshold`, we skip it.
		word := strings.ToLower(scanner.Text())
		word = string(filter.ReplaceAll([]byte(word), []byte("")))
		if len(word) < charThreshold {
			continue
		}

		// If we've seen the word, increment the count. Otherwise add it.
		if _, ok := seen[word]; !ok {
			seen[word] = 1
		} else {
			seen[word]++
		}
	}

	// Iterate over the map and turn it into a []WordCount. Then sort the results and return.
	wordCounts := []WordCount{}
	for k, v := range seen {
		wc := WordCount{
			Word:  k,
			Count: v,
		}
		wordCounts = append(wordCounts, wc)
	}
	sortWordCounts(wordCounts)
	return wordCounts[0:numWords]
}

// A struct that represents how many times a word is observed in a document
type WordCount struct {
	Word  string
	Count int
}

func (wc WordCount) String() string {
	return fmt.Sprintf("%v: %v", wc.Word, wc.Count)
}

// Helper function to sort a list of word counts in place.
// This sorts by the count in decreasing order, breaking ties using the word.
// DO NOT MODIFY THIS FUNCTION!
func sortWordCounts(wordCounts []WordCount) {
	sort.Slice(wordCounts, func(i, j int) bool {
		wc1 := wordCounts[i]
		wc2 := wordCounts[j]
		if wc1.Count == wc2.Count {
			return wc1.Word < wc2.Word
		}
		return wc1.Count > wc2.Count
	})
}
