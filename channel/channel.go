package channel

func TrySend[T any](ch chan<- T, el T) bool {
	select {
	case ch <- el:
		return true
	default:
		return false
	}
}

func TryRecv[T any](ch <-chan T) (T, bool) {
	select {
	case el := <-ch:
		return el, true
	default:
		var zero T
		return zero, false
	}
}

func Collect[T any](ch <-chan T, buf []T) (n int) {
	for n < len(buf) {
		select {
		case buf[n] = <-ch:
			n++
		default:
			return n
		}
	}
	return n
}
