package main

import (
	"encoding/json"
	"errors"
	"golang.org/x/exp/inotify"
	"log"
	"os"
	"os/exec"
	"os/signal"
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
	version       = "v2.1"
	defaultPath   = "."
	defaultDetect = "viirs_detect"
	defaultFit    = "viirs_fit"
	m10           = "SVM10"
)

type Config struct {
	WatchDir     string
	OutputDir    string
	DetectBinary string
	FitBinary    string
	ReduceBinary string
	RequireFiles []string
}

func (c *Config) Constrain() {
	for _, s := range c.RequireFiles {
		if m10 == s {
			return
		}
	}
	c.RequireFiles = append(c.RequireFiles, m10)
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
	index := make(map[string]struct{})
	for _, b := range s.Files {
		index[b] = struct{}{}
	}
	for _, b := range c.RequireFiles {
		if _, ok := index[b]; !ok {
			log.Printf("State %s incomplete at least %s left.", s.Id, b)
			return false
		}
	}
	return true
}

func (c *Config) Process(s *State) error {
	detfile := filepath.Join(c.OutputDir, strings.Join([]string{"VNFD", s.Id, version}, "_")) + ".csv"
	detect := exec.Command(c.DetectBinary, s.M10File, "-output", detfile, "-cloud", "0")
	out, err := detect.Output()
	if nil != err {
		log.Printf("DEBUG Detect output: %s\n", string(out))
		return DetectFailed
	}
	fitfile := filepath.Join(c.OutputDir, strings.Join([]string{"VNFL", s.Id, version}, "_")) + ".csv"
	fit := exec.Command(c.FitBinary, detfile, "-output", fitfile, "-plot", "1", "-map", "1", "-localmax", "1", "-size", "100", "-font", "10")
	out, err = fit.Output()
	if nil != err {
		log.Printf("DEBUG Fit output: %s\n", string(out))
		return FitFailed
	}
	return nil
}

type State struct {
	Id      string
	M10File string
	Files   []string
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
		s.Files = append(s.Files, parts[0])
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
	s.Files = append(s.Files, s2.Files...)
	if s.M10File == "" {
		s.M10File = s2.M10File
	}
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
		log.Printf("Granule %s ready for processing.\n", s.Id)
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
		defer fcfg.Close()
		if nil != err {
			log.Panicln("Failed to open config.")
		}
		dec := json.NewDecoder(fcfg)
		if err = dec.Decode(&cfg); nil != err {
			log.Panicln("Failed to parse config.")
		}
		fcfg.Close()
	}
	cfg.Constrain()
	notifications := make(chan string)
	watcher, err := inotify.NewWatcher()
	defer watcher.Close()
	if nil != err {
		log.Panicf("Failed to start watcher: %s\n", err.Error())
	}
	if err = watcher.AddWatch(cfg.WatchDir, inotify.IN_CLOSE_WRITE); nil != err {
		log.Panicf("Failed to start watching directory: %s\n", err.Error())
	}
	go work(cfg, notifications)
	log.Printf("Watching: %s\n", cfg.WatchDir)
	go func() {
	EVENT_LOOP:
		for {
			select {
			case event, ok := <-watcher.Event:
				if !ok {
					break EVENT_LOOP
				}
				if event.Mask&inotify.IN_CLOSE_WRITE == inotify.IN_CLOSE_WRITE && event.Name != cfg.WatchDir {
					notifications <- event.Name
				}
			case e, ok := <-watcher.Error:
				if !ok {
					break EVENT_LOOP
				}
				log.Printf("Watcher error: %s", e.Error())
			}
		}
	}()
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}
