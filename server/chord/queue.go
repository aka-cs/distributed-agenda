package chord

type Queue[T any] struct {
	first    *QueueNode[T]
	last     *QueueNode[T]
	size     int
	capacity int
}

type QueueNode[T any] struct {
	value *T
	prev  *QueueNode[T]
	next  *QueueNode[T]
}

func NewQueue[T any](capacity int) *Queue[T] {
	return &Queue[T]{first: nil, last: nil, size: 0, capacity: capacity}
}

func (queue *Queue[T]) PushBeg(value *T) {
	if queue.size == queue.capacity || value == nil {
		return
	}

	queue.size++

	if queue.size == 1 {
		queue.first = &QueueNode[T]{value: value, prev: nil, next: nil}
		queue.last = queue.first
		return
	}

	queue.first.prev = &QueueNode[T]{value: value, prev: nil, next: queue.first}
	queue.first = queue.first.prev
	return
}

func (queue *Queue[T]) PopBeg() *T {
	if queue.size == 0 {
		return nil
	}

	value := queue.first.value
	queue.first = queue.first.next
	queue.size--
	if queue.size != 0 {
		queue.first.prev = nil
	}

	if queue.size < 2 {
		queue.last = queue.first
	}

	return value
}

func (queue *Queue[T]) PushBack(value *T) {
	if queue.size == queue.capacity || value == nil {
		return
	}

	queue.size++

	if queue.size == 1 {
		queue.first = &QueueNode[T]{value: value, prev: nil, next: nil}
		queue.last = queue.first
		return
	}

	queue.last.next = &QueueNode[T]{value: value, prev: queue.last, next: nil}
	queue.last = queue.last.next
	return
}

func (queue *Queue[T]) PopBack() *T {
	if queue.size == 0 {
		return nil
	}

	value := queue.last.value
	queue.last = queue.last.prev
	queue.size--
	if queue.size != 0 {
		queue.last.next = nil
	}

	if queue.size < 2 {
		queue.first = queue.last
	}

	return value
}

func (queue *Queue[T]) Remove(node *QueueNode[T]) {
	if node == queue.first {
		queue.PopBeg()
	} else if node == queue.last {
		queue.PopBack()
	} else {
		node.prev.next = node.next
		node.next.prev = node.prev
		queue.size--
	}
}

func (queue *Queue[T]) Fulfilled() bool {
	return queue.size == queue.capacity
}

func (queue *Queue[T]) Back() *T {
	if queue.size == 0 {
		return nil
	}
	return queue.last.value
}

func (queue *Queue[T]) Beg() *T {
	if queue.size == 0 {
		return nil
	}
	return queue.first.value
}
