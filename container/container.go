package container

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
