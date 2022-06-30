package chord

import log "github.com/sirupsen/logrus"

type Queue[T any] struct {
	first    *QueueNode[T]
	last     *QueueNode[T]
	size     int
	capacity int
}

type QueueNode[T any] struct {
	value  *T
	prev   *QueueNode[T]
	next   *QueueNode[T]
	inside bool
}

func NewQueue[T any](capacity int) *Queue[T] {
	return &Queue[T]{first: nil, last: nil, size: 0, capacity: capacity}
}

func (queue *Queue[T]) PushBeg(value *T) {
	if queue.size == queue.capacity {
		queue.PopBack()
	}

	queue.size++

	if queue.size == 1 {
		queue.first = &QueueNode[T]{value: value, prev: nil, next: nil, inside: true}
		queue.last = queue.first
		return
	}

	queue.first.prev = &QueueNode[T]{value: value, prev: nil, next: queue.first, inside: true}
	queue.first = queue.first.prev
	return
}

func (queue *Queue[T]) PopBeg() *T {
	if queue.size == 0 {
		log.Error("Cannot pop: queue empty.\n")
		return nil
	}

	node := queue.first
	queue.first = node.next
	node.inside = false
	node.next = nil
	queue.size--

	if queue.size > 0 {
		queue.first.prev = nil
	}

	if queue.size < 2 {
		queue.last = queue.first
	}

	return node.value
}

func (queue *Queue[T]) PushBack(value *T) {
	if queue.size == queue.capacity {
		queue.PopBeg()
	}

	queue.size++

	if queue.size == 1 {
		queue.first = &QueueNode[T]{value: value, prev: nil, next: nil, inside: true}
		queue.last = queue.first
		return
	}

	queue.last.next = &QueueNode[T]{value: value, prev: queue.last, next: nil, inside: true}
	queue.last = queue.last.next
	return
}

func (queue *Queue[T]) PopBack() *T {
	if queue.size == 0 {
		log.Error("Cannot pop: queue empty.\n")
		return nil
	}

	node := queue.last
	queue.last = node.prev
	node.inside = false
	node.prev = nil
	queue.size--

	if queue.size > 0 {
		queue.last.next = nil
	}

	if queue.size < 2 {
		queue.first = queue.last
	}

	return node.value
}

func (queue *Queue[T]) Remove(node *QueueNode[T]) {
	if node.inside {
		if node.prev == nil {
			queue.PopBeg()
		} else if node.next == nil {
			queue.PopBack()
		} else {
			node.prev.next = node.next
			node.next.prev = node.prev
			node.next = nil
			node.prev = nil
			queue.size--
		}
	}
}

func (queue *Queue[T]) Empty() bool {
	return queue.size == 0
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
