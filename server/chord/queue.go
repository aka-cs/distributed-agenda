package chord

import (
	"errors"
)

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

func (queue *Queue[T]) PushBeg(value *T) error {
	if queue.size == queue.capacity {
		return errors.New("queue filled")
	}

	queue.size++

	if queue.size == 1 {
		queue.first = &QueueNode[T]{value: value, prev: nil, next: nil}
		queue.last = queue.first
		return nil
	}

	queue.first.prev = &QueueNode[T]{value: value, prev: nil, next: queue.first}
	queue.first = queue.first.prev
	return nil
}

func (queue *Queue[T]) PopBeg() (*T, error) {
	if queue.size == 0 {
		return nil, errors.New("queue empty")
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

	return value, nil
}

func (queue *Queue[T]) PushBack(value *T) error {
	if queue.size == queue.capacity {
		return errors.New("queue filled")
	}

	queue.size++

	if queue.size == 1 {
		queue.first = &QueueNode[T]{value: value, prev: nil, next: nil}
		queue.last = queue.first
		return nil
	}

	queue.last.next = &QueueNode[T]{value: value, prev: queue.last, next: nil}
	queue.last = queue.last.next
	return nil
}

func (queue *Queue[T]) PopBack() (*T, error) {
	if queue.size == 0 {
		return nil, errors.New("queue empty")
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

	return value, nil
}

func (queue *Queue[T]) Remove(node *QueueNode[T]) error {
	if node == queue.first {
		_, err := queue.PopBeg()
		if err != nil {
			return err
		}
	}
	if node == queue.last {
		_, err := queue.PopBack()
		if err != nil {
			return err
		}
	}

	node.prev.next = node.next
	node.next.prev = node.prev
	queue.size--
	return nil
}
