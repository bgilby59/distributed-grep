package mapreduce

import (
	"sync"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type safeMapStringInt struct {
	m  map[string]int
	mu sync.Mutex
}

func (sm *safeMapStringInt) make() error {
	sm.m = make(map[string]int)
	return nil
}

func (sm *safeMapStringInt) set(k string, v int) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.m[k] = v
	return nil
}

func (sm *safeMapStringInt) get(k string) (int, bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	res, ok := sm.m[k]
	return res, ok
}

type safeInt struct {
	val int
	mu  sync.Mutex
}

func (si *safeInt) set(new_val int) error {
	si.mu.Lock()
	defer si.mu.Unlock()
	si.val = new_val
	return nil
}

func (si *safeInt) change(new_val int) bool { // returns error if same value
	si.mu.Lock()
	defer si.mu.Unlock()
	if si.val == new_val {
		return false
	}
	si.val = new_val
	return true
}

func (si *safeInt) get_and_increment() int { // returns error if same value
	si.mu.Lock()
	defer si.mu.Unlock()
	res := si.val
	si.val += 1
	return res
}

func (si *safeInt) get() int {
	si.mu.Lock()
	defer si.mu.Unlock()
	return si.val
}

type safeTime struct {
	val time.Time
	mu  sync.Mutex
}

func (t *safeTime) set(new_val time.Time) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.val = new_val
	return nil
}

func (t *safeTime) get() time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.val
}

type safeString struct {
	val string
	mu  sync.Mutex
}

func (s *safeString) set(new_val string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.val = new_val
	return nil
}

func (s *safeString) get() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.val
}

type safeStringSlice struct {
	slice []safeString
	mu    sync.Mutex
}

func (s *safeStringSlice) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.slice)
}

func (s *safeStringSlice) append(new_ele string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.slice = append(s.slice, safeString{val: new_ele})
	return nil
}

func (s *safeStringSlice) get(index int) string { // this type is append-only so no need to lock
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.slice[index].get()
}

// type State safeInt
type ID int

const (
	Unassigned int = iota
	InProgress int = iota
	Completed  int = iota
)

type MapData struct {
	state     safeInt
	timestamp safeTime
	file      safeString
}

type ReduceData struct {
	state      safeInt
	timestamp  safeTime
	task_files safeStringSlice
	files      safeInt
}
