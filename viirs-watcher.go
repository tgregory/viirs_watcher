package main

import (
	"encoding/json"
	"errors"
	"os"
	"regexp"
)

var (
	ErrBadRegexp             = errors.New("The pattern regexp is malformed and fails to compile.")
	ErrNoGroup               = errors.New("Regular expression does not provide the required capture group.")
	ErrEmptyId               = errors.New("Failed to extract id from string.")
	ErrDuplicateType         = errors.New("Duplicate file types are forbidden.")
	ErrUnexpectedType        = errors.New("Received notification of unexpected type.")
	ErrDuplicateNotification = errors.New("Data file of the same name created twice.")
)

const idName = "id"

type Pattern struct {
	Pattern  *regexp.Regexp
	subIndex int
}

/* json.Marshaller interface */
func (p *Pattern) MarshalJSON() ([]byte, error) {
	s := ""
	if nil != p && nil != p.Pattern {
		s = p.Pattern.String()
	}
	return json.Marshal(s)
}

/* json.Unmarshaller interface */
func (p *Pattern) UnmarshalJSON(bytes []byte) error {
	var s string
	err := json.Unmarshal(bytes, &s)
	if nil != err {
		return err
	}
	p.Pattern, err = regexp.Compile(s)
	if nil != err {
		return ErrBadRegexp
	}
	for i, sub := range p.Pattern.SubexpNames() {
		if sub == idName {
			p.subIndex = i
			return nil
		}
	}
	return ErrNoGroup
}

func (p *Pattern) GetId(s string) (string, error) {
	id := ""
	matches := p.Pattern.FindAllStringSubmatch(s, -1)
	for _, match := range matches {
		id += match[p.subIndex]
	}
	if "" == id {
		return "", ErrEmptyId
	}
	return id, nil
}

type DataFile struct {
	Name    string
	Path    string
	Pattern Pattern
}

func (df *DataFile) UniqueName() string {
	s := df.Name
	if "" == s {
		s = df.Pattern.Pattern.String()
	}
	return s
}

type Config struct {
	CommonPath     string
	ProcessingPath string
	Files          []DataFile
}

type Data struct {
	Id       string
	Type     string
	Filename string
	Filepath string
}

type Pack struct {
	received int
	expected int
	index    map[string]bool
	Data     []Data
}

func NewPack(files []DataFile) (*Pack, error) {
	var p Pack
	p.index = make(map[string]bool)
	for _, df := range files {
		name := df.UniqueName()
		_, ok := p.index[name]
		if ok {
			return nil, ErrDuplicateType
		}
		p.index[name] = false
	}
	p.expected = len(p.index)
	return &p, nil
}

func (p *Pack) Full() bool {
	return p.expected == p.received
}

func (p *Pack) Notify(d Data) error {
	v, ok := p.index[d.Type]
	if !ok {
		return ErrUnexpectedType
	}
	if v {
		return ErrDuplicateNotification
	}
	p.index[d.Type] = true
	p.Data = append(p.Data, d)
	return nil
}

func work(cfg Config, data <-chan Data) {
	var err error
	packs := make(map[string]*Pack)
	for d := range data {
		p, ok := packs[d.Id]
		if !ok {
			p, err = NewPack(cfg.Files)
			if nil != err {
				// !TODO log
			}
			packs[d.Id] = p
		}
		p.Notify(d)
		if !p.Full() {
			continue
		}

	}
}

func main() {
}
