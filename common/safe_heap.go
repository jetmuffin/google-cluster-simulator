package common

import (
	"container/heap"
	"sort"
	"sync"
)

// An Item is something we manage in a priority queue.
type Item struct {
	Key      interface{} //The unique key of the item.
	Value    interface{} // The value of the item; arbitrary.
	Index    int         // The index of the item in the heap.
}

type JobSlice struct {
	items    []*Job
	itemsMap map[interface{}]*Job
}

func cmp(i, j *Job) bool {
	if i.taskQueue.Len() == 0 {
		return false
	} else if j.taskQueue.Len() == 0 {
		return true
	} else if i.Share == j.Share {
		if i.taskQueue.Len() == j.taskQueue.Len() {
			return i.SubmitTime < j.SubmitTime
		} else {
			return i.taskQueue.Len() > j.taskQueue.Len()
		}
	} else {
		return i.Share < j.Share
	}
}

func (s JobSlice) Len() int { return len(s.items) }

func (s JobSlice) Less(i, j int) bool {
	return cmp(s.items[i], s.items[j])
}

func (s JobSlice) Swap(i, j int) {
	s.items[i], s.items[j] = s.items[j], s.items[i]
	s.items[i].index = i
	s.items[j].index = j
	if s.itemsMap != nil {
		s.itemsMap[s.items[i].JobID] = s.items[i]
		s.itemsMap[s.items[j].JobID] = s.items[j]
	}
}

func (s *JobSlice) Push(x interface{}) {
	n := len(s.items)
	item := x.(*Job)
	item.index = n
	s.items = append(s.items, item)
	s.itemsMap[item.JobID] = item
}

func (s *JobSlice) Pop() interface{} {
	old := s.items
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	delete(s.itemsMap, item.JobID)
	s.items = old[0: n-1]
	return item
}

// update modifies the priority and value of an Item in the queue.
func (s *JobSlice) update(key interface{}, value *Job) {
	if item, ok := s.itemsMap[key]; ok {
		s.itemsMap[key] = value
		heap.Fix(s, item.index)
	}
}

// delete function delete key value pairs in the map
func (s *JobSlice) remove(key interface{}) {
	if item, ok := s.itemsMap[key]; ok {
		delete(s.itemsMap, key)
		heap.Remove(s, item.index)
	}
}

func (s *JobSlice) itemByKey(key interface{}) *Job {
	if item, found := s.itemsMap[key]; found {
		return item
	}
	return nil
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue struct {
	slice   JobSlice
	maxSize int
	mutex   sync.RWMutex
}

func (pq *PriorityQueue) Init(maxSize int) {
	pq.slice.items = make([]*Job, 0, pq.maxSize)
	pq.slice.itemsMap = make(map[interface{}]*Job)
	pq.maxSize = maxSize
}

func (pq PriorityQueue) Len() int {
	pq.mutex.RLock()
	size := pq.slice.Len()
	pq.mutex.RUnlock()
	return size
}

func (pq *PriorityQueue) minItem() *Job {
	sz := pq.slice.Len()
	if sz == 0 {
		return nil
	}
	return pq.slice.items[0]
}

func (pq *PriorityQueue) MinItem() *Job {
	pq.mutex.RLock()
	defer pq.mutex.RUnlock()
	return pq.minItem()
}

func (pq *PriorityQueue) PopItem() *Job {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()
	return heap.Pop(&(pq.slice)).(*Job)
}

func (pq *PriorityQueue) PushItem(key interface{}, value *Job) (bPushed bool) {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()
	size := pq.slice.Len()
	item := pq.slice.itemByKey(key)
	if size > 0 && item != nil {
		pq.slice.update(item, value)
		return true
	}
	if pq.maxSize <= 0 || size < pq.maxSize {
		heap.Push(&(pq.slice), value)
		return true
	}
	min := pq.minItem()
	if !cmp(min, value) {
		return false
	}
	heap.Pop(&(pq.slice))
	heap.Push(&(pq.slice), item)
	return true
}

func (pq *PriorityQueue) GetItem(key interface{}) *Job {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()

	return pq.slice.itemByKey(key)
}

func (pq *PriorityQueue) UpdateItem(key interface{}, value *Job) {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()

	pq.slice.update(key, value)
}

func (pq *PriorityQueue) RemoveItem(key interface{}) {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()

	size := pq.slice.Len()
	item := pq.slice.itemByKey(key)
	if size > 0 && item != nil {
		pq.slice.remove(key)
	}
}

func (pq PriorityQueue) GetQueue() []interface{} {
	items := pq.GetQueueItems()
	values := make([]interface{}, len(items))
	for i := 0; i < len(items); i++ {
		values[i] = items[i]
	}
	return values
}

func (pq PriorityQueue) GetQueueItems() []*Job {
	size := pq.Len()
	if size == 0 {
		return []*Job{}
	}
	s := JobSlice{}
	s.items = make([]*Job, size)
	pq.mutex.RLock()
	for i := 0; i < size; i++ {
		s.items[i] = pq.slice.items[i]
	}
	pq.mutex.RUnlock()
	sort.Sort(sort.Reverse(s))
	return s.items
}
