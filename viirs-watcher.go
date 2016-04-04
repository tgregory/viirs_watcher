package main

import (
	"encoding/json"
	"errors"
	"golang.org/x/exp/inotify"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var (
	IdMissmatch      = errors.New("Merging of states with different Ids is not allowed.")
	UnexpectedName   = errors.New("Filename does not satisfy the expected pattern.")
	DetectFailed     = errors.New("Detect binary launch failed.")
	FitFailed        = errors.New("Fit binary launch failed.")
	NightCheckFailed = errors.New("Failed to check if night data provided.")
	NoNightData      = errors.New("File provided does not contain nighttime data.")
)

const DefaultPeriod = 30 * time.Second

var (
	version       = "v2.1"
	defaultPath   = "."
	defaultDetect = "viirs_detect"
	defaultFit    = "viirs_fit"
	m10           = "SVM10"
)

const (
	Inotify = "inotify"
	Timed   = "timed"
)

type Config struct {
	Watcher struct {
		Type        string
		Period      string
		WatchDir    string
		SubWatchDir string
	}
	OutputDir    string
	DetectBinary string
	FitBinary    string
	H5DumpBinary string
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
	if "" == c.Watcher.Type {
		c.Watcher.Type = "inotify"
	}
	if "" == c.Watcher.WatchDir {
		c.Watcher.WatchDir = defaultPath
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

func (c *Config) hasNight(s *State) (bool, error) {
	h5dump := exec.Command(c.H5DumpBinary, "-x", "-A", s.M10File)
	out, err := h5dump.Output()
	if nil != err {
		log.Printf("H5Dump failed: %s\n", err.Error())
		return true, NightCheckFailed
	}
	if strings.Contains(string(out), "Descending_Indicator") {
		return true, nil
	}
	return false, nil
}

func (c *Config) Process(s *State) error {
	if night, err := c.hasNight(s); nil == err && !night {
		return NoNightData
	}
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
	s.Files = append(s.Files, parts[0])
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

type Notification struct {
	File      string
	OnProcess func()
}

func work(cfg Config, notifications <-chan Notification) {
	awaiting := make(map[string]*State)
	for notif := range notifications {
		f := notif.File
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
		notif.OnProcess()
	}
	log.Printf("Notifications channel closed. All done. \n")
}

func workLayout(cfg Config, dirs <-chan string) {
	var watchers []*LayoutWatcher
	notifications := make(chan Notification)
	go work(cfg, notifications)
	for d := range dirs {
		period, err := time.ParseDuration(cfg.Watcher.Period)
		if nil != err {
			log.Printf("Failed to parse period, failing back to default %s\n", DefaultPeriod)
			period = DefaultPeriod
		}
		lw := NewLayoutWatcher(period)
		watchers = append(watchers, lw)
		lw.AddWatch(filepath.Join(d, cfg.Watcher.SubWatchDir))
	FILE_LOOP:
		for {
			select {
			case file, ok := <-lw.Event():
				if !ok {
					break FILE_LOOP
				}
				go func() {
					finfo, err := os.Stat(file)
					if nil != err {
						log.Println(err)
						return
					}
					osz := finfo.Size()
					<-time.After(10 * time.Second)
					finfo, err = os.Stat(file)
					if nil != err {
						log.Println(err)
						return
					}
					if finfo.Size() == osz {
						notifications <- Notification{file, lw.Close}
					}
				}()
			case err, ok := <-lw.Error():
				if !ok {
					break FILE_LOOP
				}
				log.Println(err.Error())
			}
		}
	}
	for _, w := range watchers {
		w.Close()
	}
}

type LayoutWatcher struct {
	period time.Duration
	paths  string
	event  chan string
	err    chan error
	wg     sync.WaitGroup
	done   map[string]chan struct{}
}

func NewLayoutWatcher(period time.Duration) *LayoutWatcher {
	return &LayoutWatcher{
		period: period,
		event:  make(chan string),
		err:    make(chan error),
		done:   make(map[string]chan struct{}),
	}
}

func (lw *LayoutWatcher) Event() <-chan string {
	return lw.event
}

func (lw *LayoutWatcher) Error() <-chan error {
	return lw.err
}

func (lw *LayoutWatcher) Close() {
	for k := range lw.done {
		close(lw.done[k])
	}
	lw.wg.Wait()
}

func (lw *LayoutWatcher) AddWatch(path string) {
	lw.wg.Add(1)
	done := make(chan struct{})
	if _, ok := lw.done[path]; ok {
		return // prevent double watch
	}
	lw.done[path] = done
	var currentCheck time.Time
	lastCheck := time.Now()
	go func() {
	WATCH_LOOP:
		for {
			func() {
				currentCheck = time.Now()
				finfos, err := ioutil.ReadDir(path)
				if nil != err {
					lw.err <- err
					return
				}

				for _, finfo := range finfos {
					if finfo.ModTime().After(lastCheck) {
						lw.event <- filepath.Join(path, finfo.Name())
					}
				}

				lastCheck = currentCheck
			}()
			notif := time.After(lw.period)
			select {
			case <-notif:
				continue WATCH_LOOP
			case <-done:
				break WATCH_LOOP
			}
		}
		lw.wg.Done()
	}()
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
			log.Printf("Config parse error: %s\n", err.Error())
			log.Panicln("Failed to parse config.")
		}
		fcfg.Close()
	}
	cfg.Constrain()
	switch strings.ToLower(cfg.Watcher.Type) {
	case Inotify:
		watcher, err := inotify.NewWatcher()
		defer watcher.Close()
		if nil != err {
			log.Panicf("Failed to start watcher: %s\n", err.Error())
		}
		if err = watcher.AddWatch(cfg.Watcher.WatchDir, inotify.IN_CLOSE_WRITE); nil != err {
			log.Panicf("Failed to start watching directory: %s\n", err.Error())
		}
		notifications := make(chan Notification)
		go work(cfg, notifications)
		log.Printf("Watching: %s\n", cfg.Watcher.WatchDir)
		go func() {
		EVENT_LOOP:
			for {
				select {
				case event, ok := <-watcher.Event:
					if !ok {
						break EVENT_LOOP
					}
					if event.Mask&inotify.IN_CLOSE_WRITE == inotify.IN_CLOSE_WRITE && event.Name != cfg.Watcher.WatchDir {
						notifications <- Notification{event.Name, func() {}}
					}
				case e, ok := <-watcher.Error:
					if !ok {
						break EVENT_LOOP
					}
					log.Printf("Watcher error: %s", e.Error())
				}
			}
		}()
	case Timed:
		period, err := time.ParseDuration(cfg.Watcher.Period)
		if nil != err {
			log.Printf("Failed to parse period, failing back to default %s\n", DefaultPeriod)
			period = DefaultPeriod
		}
		watcher := NewLayoutWatcher(period)
		defer watcher.Close()
		watcher.AddWatch(cfg.Watcher.WatchDir)
		go workLayout(cfg, watcher.Event())
		go func() {
		EVENT_LOOP:
			for {
				e, ok := <-watcher.Error()
				if !ok {
					break EVENT_LOOP
				}
				log.Printf("Watcher error: %s", e.Error())
			}
		}()
	}
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}
