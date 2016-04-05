package main

import (
	"bytes"
	"errors"
	xp "gopkg.in/xmlpath.v2"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

var UnexpectedName = errors.New("Name does not satisfy expected pattern.")

var watchDir = "/data"
var subDir = "result"
var prefix = "NPP"
var period = 15 * time.Second

const m10 = "SVM10"

var required = []string{
	"GMTCO",
	"IICMO",
	"SVDNB",
	"SVM07",
	"SVM08",
	"SVM10",
	"SVM12",
	"SVM13",
	"SVM14",
	"SVM15",
	"SVM16",
}

var H5DumpBinary = "h5dump"
var DetectBinary = "viirs_detect"
var FitBinary = "viirs_fit"

var OutputDir = "/output"

const version = "v2.1"

func getId(m10file string) (string, error) {
	name := filepath.Base(m10file)
	parts := strings.Split(name, "_")
	if len(parts) < 6 {
		return "", UnexpectedName
	}
	return strings.Join(parts[1:5], "_"), nil

}

func hasNight(m10file string) bool {
	h5dump := exec.Command(H5DumpBinary, "-x", "-A", m10file)
	out, err := h5dump.Output()
	if nil != err {
		log.Printf("WARN: H5Dump failed: %s\n", err.Error())
		return true
	}
	path, err := xp.Compile("//Attribute[contains(@Name, 'Ascending/Descending_Indicator')]/Data/DataFromFile")
	if nil != err {
		log.Printf("WARN: Failed to compile xpath query due to %s", err.Error())
		return true
	}
	root, err := xp.Parse(bytes.NewReader(out))
	if nil != err {
		log.Printf("WARN: Failed to retreive root node due to %s", err.Error())
		return true
	}
	iter := path.Iter(root)
	for iter.Next() {
		if strings.TrimSpace(iter.Node().String()) != "0" {
			return true
		}
	}
	return false
}

func Process(m10file string) {
	if !hasNight(m10file) {
		log.Printf("No night data for %s ignoreing", m10file)
		return
	}
	id, err := getId(m10file)
	if nil != err {
		log.Printf("ERROR: Processing of %s failed due to %s", m10file, err.Error())
		return
	}
	detfile := filepath.Join(OutputDir, strings.Join([]string{"VNFD", id, version}, "_")) + ".csv"
	detect := exec.Command(DetectBinary, m10file, "-output", detfile, "-cloud", "0")
	out, err := detect.Output()
	if nil != err {
		log.Printf("ERROR: Detect failed with the following output: %s\n", string(out))
		return
	}
	fitfile := filepath.Join(OutputDir, strings.Join([]string{"VNFF", id, version}, "_")) + ".csv"
	fit := exec.Command(FitBinary, detfile, "-output", fitfile, "-plot", "1", "-map", "1", "-localmax", "1", "-size", "100", "-font", "10")
	out, err = fit.Output()
	if nil != err {
		log.Printf("ERROR: Fit failed with the following output: %s\n", string(out))
		return
	}

	log.Printf("INFO: Processing success %s", m10file)
}

func isRequired(s string) bool {
	for i := range required {
		if strings.HasPrefix(s, required[i]) {
			return true
		}
	}
	return false
}

func fileDone(f string) bool {
	finfo, err := os.Stat(f)
	if nil != err {
		log.Printf("Failed to stat file %s", f)
	}
	osz := finfo.Size()
	<-time.After(3 * time.Second)
	finfo, err = os.Stat(f)
	if nil != err {
		log.Printf("Failed to stat file %s", f)
	}
	if osz == finfo.Size() {
		return true
	}
	return false
}

func watchSub(dir string) {
	log.Printf("Watching subdirectory %s", dir)
	found := 0
	marked := make(map[string]bool)
	var m10file string
	for found != len(required) {
		finfos, err := ioutil.ReadDir(dir)
		if nil != err {
			log.Printf("Failed to read subdirectory %s", dir)
		}
		for _, f := range finfos {
			if isRequired(f.Name()) {
				_, ok := marked[f.Name()]
				if !ok && fileDone(filepath.Join(dir, f.Name())) {
					found += 1
					log.Printf("Required file %s found", f.Name())
					marked[f.Name()] = true
					if strings.HasPrefix(f.Name(), m10) {
						m10file = filepath.Join(dir, f.Name())
					}
				}
			}
		}
		<-time.After(period)
	}
	Process(m10file)
}

func watchRoot(dir string, since time.Time) time.Time {
	maxTime := since
	finfos, err := ioutil.ReadDir(dir)
	if nil != err {
		log.Printf("Failed to read directory %s", dir)
	}

	for _, finfo := range finfos {
		if finfo.ModTime().After(since) {
			if strings.HasPrefix(finfo.Name(), prefix) {
				go watchSub(filepath.Join(dir, finfo.Name(), subDir))
				if finfo.ModTime().After(maxTime) {
					maxTime = finfo.ModTime()
				}
			}
		}
	}
	return maxTime
}

func main() {
	checkTime := time.Now()
	for {
		checkTime = watchRoot(watchDir, checkTime)
		<-time.After(period)
	}
}
