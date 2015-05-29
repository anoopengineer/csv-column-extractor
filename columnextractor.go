package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime/pprof"
	"strings"
	"syscall"
	"time"
)

var i = flag.String("i", "", "-i = d:\\path\\to\\input.csv - If the path doesn't end in .csv, path will be assumed to be a directory and name assumed to be \"input.csv\"")
var o = flag.String("o", "", "-o = d:\\path\\to\\column_extracted.csv - If the path doesn't end in .csv, path will be assumed to be a directory and output name assumed to be \"column_extracted.csv\"")
var p = flag.String("p", "", "-p = profile file that contains one column header in one line")

func main() {
	now := time.Now()
	defer func() {
		duration := time.Now().Sub(now)
		fmt.Printf("\nExecution completed. Time taken = %v ", duration)
	}()

	cpf, err := os.Create("cpuprof.prof")
	if err != nil {
		panic(err)
	}
	pprof.StartCPUProfile(cpf)
	defer pprof.StopCPUProfile()

	fmt.Println("CSV Column extractor by Anoop Kunjuraman. Report bugs to anoopengineer@gmail.com")
	flag.Parse()
	HandleInterrupts()

	if *p == "" {
		fmt.Println("Profile file not mentioned")
		flag.PrintDefaults()
		return
	}

	f := "input.csv"
	if *i != "" {
		f = *i
		l := strings.ToLower(f)
		if !strings.HasSuffix(l, ".csv") {
			f = f + "/input.csv"
		}
	}
	csvFile, err := os.Open(f)
	if err != nil {
		panic(err)
	}
	defer csvFile.Close()
	reader := csv.NewReader(csvFile)

	//write file

	ocsvName := "column_extracted.csv"
	if *o != "" {
		ocsvName = *o
		l := strings.ToLower(*o)
		if strings.HasSuffix(l, ".csv") {
			ocsvName = ocsvName + "/column_extracted.csv"
		}
	}
	ocsvF, err := os.Create(ocsvName)
	if err != nil {
		panic(err)
	}
	writer := csv.NewWriter(ocsvF)
	defer writer.Flush()

	header, err := reader.Read()
	if err != nil {
		panic(err)
	}
	indices := FindIndices(*p, header)

	record := make([]string, 0)
	for _, val := range indices {
		record = append(record, header[val])
		fmt.Println(val, " = ", header[val])
	}
	//fmt.Println(record)
	writer.Write(record)
	counter := 0
	for {
		counter++
		fmt.Printf("\r%v", counter)
		fields, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		record := make([]string, 0)
		for _, val := range indices {
			record = append(record, fields[val])
		}
		writer.Write(record)
	}

	// fmt.Println(indices)
	if err != nil {
		panic(err)
	}

}

func HandleInterrupts() {
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT)
	go func() {
		<-ch
		fmt.Println("CTRL-C; exiting")
		// time.Sleep(2 * time.Second)
		os.Exit(0)
	}()
}

func FindIndices(profileLocation string, header []string) (retVal []int) {
	f, err := os.Open(profileLocation)
	if err != nil {
		panic(err)
	}
	cNames := make([]string, 0)
	var l string
	s := bufio.NewScanner(f)
	for s.Scan() {
		l = strings.TrimSpace(s.Text())
		if l != "" {
			cNames = append(cNames, l)
		}
	}

	for i, val := range header {
		for _, val2 := range cNames {
			if val == val2 {
				retVal = append(retVal, i)
				break
			}
		}
	}
	return
}
