package container

import "sync"

type ConcurrentSet[T comparable] struct {
	mu  sync.RWMutex
	set Set[T]
}

func NewConcurrentSet[T comparable]() ConcurrentSet[T] {
	return ConcurrentSet[T]{
		set: NewSet[T](),
	}
}

// Add adds element k to the set and returns whether the element was
// new, and the new length of the set.
func (cset *ConcurrentSet[T]) Add(k T) (bool, int) {
	cset.mu.Lock()
	defer cset.mu.Unlock()

	_, ok := cset.set[k]
	cset.set[k] = struct{}{}
	return !ok, len(cset.set)
}

func (cset *ConcurrentSet[T]) Has(k T) bool {
	cset.mu.RLock()
	defer cset.mu.RUnlock()

	_, ok := cset.set[k]
	return ok
}

// Delete deletes element k and returns the new length of the set.
func (cset *ConcurrentSet[T]) Delete(k T) int {
	cset.mu.Lock()
	defer cset.mu.Unlock()

	delete(cset.set, k)
	return len(cset.set)
}

func (cset *ConcurrentSet[T]) Copy() Set[T] {
	cset.mu.RLock()
	defer cset.mu.RUnlock()
	return CopyMap(cset.set)
}

type Set[T comparable] map[T]struct{}

func NewSet[T comparable]() Set[T] {
	return Set[T]{}
}

func (set Set[T]) Add(k T) bool {
	_, ok := set[k]
	set[k] = struct{}{}
	return !ok
}

func (set Set[T]) Has(k T) bool {
	_, ok := set[k]
	return ok
}

func (set Set[T]) Delete(k T) {
	delete(set, k)
}

func CopyMap[M interface{ ~map[K]V }, K comparable, V any](m M) M {
	out := make(M, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
