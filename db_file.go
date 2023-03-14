package simplekv

import (
	"io"
	"os"
)

const FileName = "simplekv.data"
const MergeFileName = "simplekv.data.merge"

type DBFile struct {
	File   *os.File
	Offset int64
}

func newFile(fileName string) (*DBFile, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	stat, err := os.Stat(fileName)
	if err != nil {
		return nil, err
	}
	return &DBFile{Offset: stat.Size(), File: file}, nil
}

func NewDBFile(path string) (*DBFile, error) {
	fileName := path + string(os.PathSeparator) + FileName
	return newFile(fileName)
}

func NewMergeDBFile(path string) (*DBFile, error) {
	fileName := path + string(os.PathSeparator) + MergeFileName
	return newFile(fileName)
}

func (df *DBFile) Write(e *Entry) error {
	enc, err := e.Encode()
	if err != nil {
		return err
	}
	_, err = df.File.WriteAt(enc, df.Offset)
	df.Offset += e.GetEntrySize()
	return nil
}

func (df *DBFile) Read(offset int64) (*Entry, error) {
	var e *Entry
	var err error

	buf := make([]byte, entryHeaderSize)
	if _, err = df.File.ReadAt(buf, offset); err != nil {
		return nil, err
	}
	if e, err = Decode(buf); err != nil {
		return nil, err
	}

	offset += entryHeaderSize
	if e.KeySize > 0 {
		key := make([]byte, e.KeySize)
		if _, err = df.File.ReadAt(key, offset); err != nil {
			return nil, err
		}
		e.Key = string(key)
	}

	offset += int64(e.KeySize)
	if e.ValueSize > 0 {
		value := make([]byte, e.ValueSize)
		if _, err = df.File.ReadAt(value, offset); err != nil {
			return nil, err
		}
		e.Value = string(value)
	}
	return e, nil
}

func (df *DBFile) Search(key string) (string, error) {
	var offset int64

	for {
		e, err := df.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return "", err
		}
		if e.Key == key {
			return e.Value, nil
		}
		offset += e.GetEntrySize()
	}
	return "", nil
}
