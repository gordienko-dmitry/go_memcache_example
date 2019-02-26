package main

import (
	apps "./appinstalled"
	"compress/gzip"
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/golang/protobuf/proto"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
)

type userApps struct {
	devType, devId  string
	lat, lon float64
	apps     []uint32
}

func newMemcConnection(addresses map[string]string) map[string]*memcache.Client {
	clients := make(map[string]*memcache.Client, 4)
	for key, value := range(addresses) {
		clients[key] = memcache.New(value)
		clients[key].Timeout = 2
		clients[key].MaxIdleConns = 3
	}
	return clients
}

func writer(writeCh chan []userApps, clients map[string]*memcache.Client, numErrors *uint64) {
	for uas := range writeCh {
		var localNumErrors uint64 = 0
		for _, ua := range(uas) {

			key := fmt.Sprintf("%s:%s", ua.devType, ua.devId)

			/* Marshall UserApps instance to get value to store */
			if data, err := proto.Marshal(
				&apps.UserApps{Apps: ua.apps, Lat: &ua.lat, Lon: &ua.lon});
				err == nil {
				if clients[ua.devType].Set(&memcache.Item{Key: key, Value: data}) != nil {
					localNumErrors++
				}
			}
			atomic.AddUint64(numErrors, localNumErrors)
		}
	}
}


func parseLine(line []string) (userApps, error) {
	var err error

	ua := new(userApps)

	// need for key
	ua.devType = line[0]
	ua.devId = line[1]

	// lat
	if lat, err := strconv.ParseFloat(line[2], 64); err == nil {
		ua.lat = lat
	}

	// lon
	if lon, err := strconv.ParseFloat(line[3], 64); err == nil {
		ua.lon = lon
	}

	// apps
	for _, app := range(strings.Split(line[4], ",")) {
		if val, err := strconv.ParseUint(app, 10, 64); err == nil {
			ua.apps = append(ua.apps, uint32(val))
		}
	}

	return *ua, err

}


func reader(readCh chan string, writeCh chan []userApps, numberLines int, numLines, numErrors *uint64) {
	for filename := range readCh {

		filereader, err := os.Open(filename)
		if err != nil {
			log.Fatal("Cannot open file %s", err)
		}

		// gzip oopener
		greader, err := gzip.NewReader(filereader)
		if err != nil {
			log.Fatal(err)
		}

		// csv reader
		csvreader := csv.NewReader(greader)
		csvreader.Comma = '\t'
		csvreader.FieldsPerRecord = 5

		counterLines := numberLines
		var lines []userApps
		var localNumErrors uint64 = 0
		var localNumLines uint64 = 0

		for {
			localNumLines++
			counterLines--
			line, err := csvreader.Read()
			if err != nil {
				if err == io.EOF {
					if len(lines) > 0 { writeCh <- lines }
					localNumLines--
					break
				} else {
					log.Printf("CSV-Error in reading line %s", line)
					localNumErrors++
					continue
				}
			}

			ua, err := parseLine(line)
			if err == nil {
				lines = append(lines, ua)
				if counterLines == 0 {
					writeCh <- lines
					lines = lines[:0]
				}
			} else {
				log.Printf("Error parse line %s", err)
				localNumErrors++
			}
		}

		// common vars
		atomic.AddUint64(numLines, localNumLines)
		atomic.AddUint64(numErrors, localNumErrors)

		// rename file
		greader.Close()
		filereader.Close()

		os.Rename(filename, filepath.Join(filepath.Dir(filename), "." + filepath.Base(filename)))
	}
}


func readFiles(pattern string, readCh chan string) {
	var (
		files []string
		err   error
	)

	if files, err = filepath.Glob(pattern); err != nil {
		log.Println("No files in current path: ", err)
		os.Exit(1)
	}

	for _, filename := range (files) {
		readCh <- filename
	}
}


func main() {

	// Parameters from command line
	logfile := flag.String("log", "memc.log", "")
	pattern := flag.String("pattern", "./data/*.tsv.gz", "")

	idfa := flag.String("idfa", "127.0.0.1:33013", "")
	gaid := flag.String("gaid", "127.0.0.1:33014", "")
	adid := flag.String("adid", "127.0.0.1:33015", "")
	dvid := flag.String("dvid", "127.0.0.1:33016", "")

	writers := flag.Int("w", 3, "")
	readers := flag.Int("r", 3, "")
	numberLines := flag.Int("n", 100, "")
	flag.Parse()

	logTo, ok := os.OpenFile(*logfile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0664)
	if ok != nil {
		logTo = os.Stdin
		log.Printf("Cannot log to file %s, logging to StdIn", *logfile)
	}
	log.SetOutput(logTo)

	// os exist
	if _, err := os.Stat(*pattern); os.IsNotExist(err) {
		log.Println("Path %s not exist (And there no files, logically)", *pattern)
		os.Exit(1)
	}

	clients := newMemcConnection(map[string]string{
		"idfa": *idfa, "gaid": *gaid, "adid": *adid, "dvid": *dvid})

	readCh := make(chan string, 100)
	writeCh := make(chan []userApps, 200)
	var numLines  uint64 = 0
	var numErrors uint64 = 0

	for i := 0; i < *readers; i++ {
		go reader(readCh, writeCh, *numberLines, &numLines, &numErrors)
	}

	for i := 0; i < *writers; i++ {
		go writer(writeCh, clients, &numErrors)
	}

	readFiles(*pattern, readCh)

	var normallErrorRate float64 = 0.01
	if numLines != numErrors {
		errRate := float64(numErrors) / float64(numLines - numErrors)
		if errRate < normallErrorRate {
			log.Println("Acceptable error rate %s. Successfull load", errRate)
		} else {
			log.Println("High error rate %s. Failed load", errRate)
		}
	} else {
		log.Println("Very sad - 0% preceed")
	}
}
