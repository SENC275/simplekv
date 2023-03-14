package simplekv

import (
	"log"
	"sync"
)

type treeNode struct {
	KV    *Entry
	Left  *treeNode
	Right *treeNode
}

type Tree struct {
	root   *treeNode
	count  int
	rWLock *sync.RWMutex
}

func NewTree() *Tree {
	return &Tree{
		rWLock: &sync.RWMutex{},
	}
}

func (tree *Tree) Init() {
	tree.rWLock = &sync.RWMutex{}
}

func (tree *Tree) GetCount() int {
	return tree.count
}

func (tree *Tree) Search(key string) (*Entry, error) {
	tree.rWLock.RLock()
	defer tree.rWLock.RUnlock()

	if tree == nil {
		log.Fatal("The tree is empty")
	}

	currentNode := tree.root

	for currentNode != nil {
		if key == currentNode.KV.Key {
			return currentNode.KV, nil
		}
		if key < currentNode.KV.Key {
			currentNode = currentNode.Left
		} else {
			currentNode = currentNode.Right
		}
	}

	return nil, nil
}

func (tree *Tree) Set(key string, value string) {
	tree.rWLock.Lock()
	defer tree.rWLock.Unlock()

	if tree == nil {
		log.Fatal("The tree is empty")
	}

	newNode := &treeNode{
		KV: NewEntry(key, value, PUT),
	}

	currentNode := tree.root

	if currentNode == nil {
		tree.root = newNode
		tree.count++
		return
	}

	for currentNode != nil {
		if key == currentNode.KV.Key {
			currentNode.KV.Value = value
			return
		}

		if key < currentNode.KV.Key {
			if currentNode.Left == nil {
				currentNode.Left = newNode
				tree.count++
			}
			currentNode = currentNode.Left
		} else {
			if currentNode.Right == nil {
				currentNode.Right = newNode
				tree.count++
				return
			}
			currentNode = currentNode.Right
		}
	}
	return
}

func (tree *Tree) GetEntries() []*Entry {
	tree.rWLock.RLock()
	defer tree.rWLock.RUnlock()

	stack := InitStack(tree.count / 2)
	entries := make([]*Entry, 0)

	currentNode := tree.root

	for {
		if currentNode != nil {
			stack.Push(currentNode)
			currentNode = currentNode.Left
		} else {
			popNode, ok := stack.Pop()
			if ok == false {
				break
			}
			entries = append(entries, popNode.KV)
			currentNode = popNode.Right
		}
	}
	return entries
}
