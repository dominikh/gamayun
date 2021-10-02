package mymath

type Ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr |
		~float32 | ~float64 |
		~string
}

func Min[T Ordered](a, b T) T {
	if a <= b {
		return a
	} else {
		return b
	}
}

func Max[T Ordered](a, b T) T {
	if a >= b {
		return a
	} else {
		return b
	}
}
