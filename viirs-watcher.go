package main

import (
	"encoding/json"
	"errors"
	"github.com/fsnotify/fsnotify"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

var (
	IdMissmatch    = errors.New("Merging of states with different Ids is not allowed.")
	UnexpectedName = errors.New("Filename does not satisfy the expected pattern.")
	DetectFailed   = errors.New("Detect binary launch failed.")
	FitFailed      = errors.New("Fit binary launch failed.")
)

var (
	//bands = []string{"DNB", "SVM07", "SVM08", "SVM10", "SVM12", "SVM13", "SVM14", "SVM15", "SVM16"}
	version       = "v2.1"
	defaultPath   = "."
	defaultDetect = "vnf_detect"
	defaultFit    = "vnf_fit"
	m10           = "SVM10"
	gmtco         = "GMTCO"
	iicmo         = "IICMO"
)

type Config struct {
	WatchDir     string
	OutputDir    string
	DetectBinary string
	FitBinary    string
	ReduceBinary string
	RequireBands []string
	RequireGMTCO bool
	RequireIICMO bool
}

func (c *Config) Constrain() {
	for _, s := range c.RequireBands {
		if m10 == s {
			return
		}
	}
	c.RequireBands = append(c.RequireBands, m10)
	if "" == c.OutputDir {
		c.OutputDir = defaultPath
	}
	if "" == c.WatchDir {
		c.WatchDir = defaultPath
	}
	if "" == c.DetectBinary {
		c.DetectBinary = defaultDetect
	}
	if "" == c.FitBinary {
		c.FitBinary = defaultFit
	}
}

func (c *Config) Done(s *State) bool {
	if c.RequireGMTCO != s.GMTCO {
		return false
	}
	if c.RequireIICMO != s.IICMO {
		return false
	}
	index := make(map[string]struct{})
	for _, b := range s.Bands {
		index[b] = struct{}{}
	}
	for _, b := range c.RequireBands {
		if _, ok := index[b]; !ok {
			return false
		}
	}
	return true
}

func (c *Config) Process(s *State) error {
	detfile := filepath.Join(c.OutputDir, strings.Join([]string{"VNFD", s.Id, version}, "_")) + ".csv"
	detect := exec.Command(c.DetectBinary, s.M10File, "-output", detfile, "-cloud", "0")
	err := detect.Run()
	if nil != err {
		return DetectFailed
	}
	fitfile := filepath.Join(c.OutputDir, strings.Join([]string{"VNFL", s.Id, version}, "_")) + ".csv"
	fit := exec.Command(c.FitBinary, detfile, "-output", fitfile, "-plot", "1", "-map", "1", "-localmax", "1", "-size", "100", "-font", "10")
	err = fit.Run()
	if nil != err {
		return FitFailed
	}
	return nil
}

type State struct {
	Id      string
	M10File string
	Bands   []string
	GMTCO   bool
	IICMO   bool
}

func NewState(fp string) (*State, error) {
	var s State
	name := filepath.Base(fp)
	parts := strings.Split(name, "_")
	if len(parts) < 6 {
		return nil, UnexpectedName
	}
	s.Id = strings.Join(parts[1:5], "_")
	if strings.HasPrefix(parts[0], "SV") {
		s.Bands = append(s.Bands, parts[0])
	}
	if parts[0] == m10 {
		s.M10File = fp
	}
	return &s, nil
}

func (s *State) Merge(s2 *State) error {
	if s.Id != s2.Id {
		return IdMissmatch
	}
	s.Bands = append(s.Bands, s2.Bands...)
	if s.M10File == "" {
		s.M10File = s2.M10File
	}
	s.GMTCO = s.GMTCO || s2.GMTCO
	s.IICMO = s.IICMO || s2.IICMO
	return nil
}

func work(cfg Config, files <-chan string) {
	awaiting := make(map[string]*State)
	for f := range files {
		s, err := NewState(f)
		if nil != err {
			log.Printf("State creation fail: %s for %s\n", err.Error(), f)
			continue
		}
		if s2, ok := awaiting[s.Id]; ok {
			s.Merge(s2)
		}
		awaiting[s.Id] = s
		if !cfg.Done(s) {
			continue
		}
		delete(awaiting, s.Id)
		if err = cfg.Process(s); nil != err {
			log.Printf("Processing failed: %s for %s\n", err.Error(), s.Id)
		}
	}
	log.Printf("Notifications channel closed. All done. \n")
}

func main() {
	var cfg Config
	if len(os.Args) >= 2 {
		fcfg, err := os.Open(os.Args[1])
		if nil != err {
			log.Panicln("Failed to open config.")
		}
		dec := json.NewDecoder(fcfg)
		if err = dec.Decode(&cfg); nil != err {
			log.Panicln("Failed to parse config.")
		}
	}
	cfg.Constrain()
	notifications := make(chan string)
	watcher, err := fsnotify.NewWatcher()
	if nil != err {
		log.Panicf("Failed to start watcher: %s\n", err.Error())
	}
	if err = watcher.Add(cfg.WatchDir); nil != err {
		log.Panicf("Failed to start watching directory: %s\n", err.Error())
	}
	go work(cfg, notifications)
	for {
		select {
		case event := <-watcher.Events:
			if event.Op&fsnotify.Write == fsnotify.Write && event.Name != cfg.WatchDir {
				notifications <- event.Name
			}
		case e := <-watcher.Errors:
			log.Printf("Watcher error: %s", e.Error())
		}
	}
}
