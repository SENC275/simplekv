package simplekv

import (
	"fmt"
	"os"
	"sync"
)

const CACHE_LIMIT = 100

type SimpleKV struct {
	indexes    map[string]int64
	memoryTree *Tree
	dbFile     *DBFile
	dbPath     string
	mu         sync.RWMutex
}

func Open(dbPath string) (*SimpleKV, error) {
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		if err := os.MkdirAll(dbPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	dbFile, err := NewDBFile(dbPath)
	if err != nil {
		return nil, err
	}

	newMemoryTree := NewTree()
	db := &SimpleKV{
		indexes:    make(map[string]int64),
		memoryTree: newMemoryTree,
		dbPath:     dbPath,
		dbFile:     dbFile,
	}

	return db, nil
}

func (db *SimpleKV) Put(key, value string) {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.memoryTree.Set(key, value)
	db.CheckAndWriteEntries()
}

func (db *SimpleKV) Get(key string) (string, error) {
	if len(key) == 0 {
		return "", nil
	}
	db.mu.RLock()
	defer db.mu.RUnlock()

	entry, _ := db.memoryTree.Search(key)
	fmt.Println(entry)
	if entry != nil {
		return entry.Value, nil
	} else {
		value, err := db.dbFile.Search(key)
		if err == nil {
			return value, nil
		}
	}

	return "", nil
}

func (db *SimpleKV) WriteAllEntries() {
	entries := db.memoryTree.GetEntries()
	for _, entry := range entries {
		err := db.dbFile.Write(entry)
		if err != nil {
			return
		}
	}
	db.memoryTree = NewTree()
}

func (db *SimpleKV) CheckAndWriteEntries() {
	if db.memoryTree.count >= CACHE_LIMIT {
		db.WriteAllEntries()
	}
}
