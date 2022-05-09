package main

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sort"
	"sync"
)

var (
	ErrCrc         = errors.New("crc mismatch")
	ErrKeyNotFound = errors.New("key not found")
)

type Entry struct {
	key    string
	offset int64
}

type DB struct {
	path   string
	file   *os.File
	lock   sync.Mutex
	offset int64
	index  []*Entry
}

func NewDB(path string) (*DB, error) {
	db := &DB{path: path}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0766)
	if err != nil {
		return nil, err
	}
	db.file = file
	stat, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	db.offset = stat.Size()
	db.index = []*Entry{}

	return db, db.load()
}

func (db *DB) load() error {
	var offsets = map[string]int64{}
	var offset int64
	for {
		key, _, size, err := db.Read(offset, false)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		offsets[key] = offset
		offset += size
	}
	for k, o := range offsets {
		db.index = append(db.index, &Entry{key: k, offset: o})
	}
	sort.Slice(db.index, func(i, j int) bool {
		return db.index[i].key < db.index[j].key
	})
	return nil
}

func (db *DB) Read(offset int64, loadValue bool) (string, string, int64, error) {
	buf := make([]byte, 12)
	if _, err := db.file.ReadAt(buf, offset); err != nil {
		return "", "", 0, err
	}
	crc := binary.BigEndian.Uint32(buf[0:4])
	keysize := binary.BigEndian.Uint32(buf[4:8])
	valuesize := binary.BigEndian.Uint32(buf[8:12])

	offset += 12
	key := make([]byte, keysize)
	if _, err := db.file.ReadAt(key, offset); err != nil {
		return "", "", 0, err
	}

	offset += int64(keysize)
	var value []byte
	if loadValue {
		value = make([]byte, valuesize)
		if _, err := db.file.ReadAt(value, offset); err != nil {
			return "", "", 0, err
		}
		crcbuf := make([]byte, 8+keysize+valuesize)
		copy(crcbuf[0:4], buf[4:8])
		copy(crcbuf[4:8], buf[8:12])
		copy(crcbuf[8:8+keysize], key)
		copy(crcbuf[8+keysize:8+keysize+valuesize], value)
		if crc != crc32.ChecksumIEEE(crcbuf) {
			return "", "", 0, ErrCrc
		}
	}

	return string(key), string(value), 12 + int64(keysize+valuesize), nil
}

func (db *DB) entry(key string) *Entry {
	i := sort.Search(len(db.index), func(i int) bool {
		return db.index[i].key >= key
	})
	if i < len(db.index) && db.index[i].key == key {
		return db.index[i]
	} else {
		return nil
	}
}

func (db *DB) Get(key string) (string, error) {
	db.lock.Lock()
	defer db.lock.Unlock()
	entry := db.entry(key)
	if entry == nil {
		return "", ErrKeyNotFound
	}
	_, v, _, err := db.Read(entry.offset, true)
	return v, err
}

func (db *DB) Put(key string, value string) error {
	keyb := []byte(key)
	valueb := []byte(value)
	keysize := uint32(len(keyb))
	valuesize := uint32(len(valueb))
	buf := make([]byte, 12+keysize+valuesize)
	binary.BigEndian.PutUint32(buf[4:8], keysize)
	binary.BigEndian.PutUint32(buf[8:12], valuesize)
	copy(buf[12:12+keysize], keyb)
	copy(buf[12+keysize:12+keysize+valuesize], valueb)
	crc := crc32.ChecksumIEEE(buf[4:])
	binary.BigEndian.PutUint32(buf[0:4], crc)

	db.lock.Lock()
	defer db.lock.Unlock()

	offset := db.offset
	_, err := db.file.WriteAt(buf, offset)
	if err != nil {
		return err
	}
	if err := db.file.Sync(); err != nil {
		return err
	}
	db.offset += int64(len(buf))

	entry := db.entry(key)
	if entry != nil {
		entry.offset = offset
	} else {
		i := sort.Search(len(db.index), func(i int) bool {
			return db.index[i].key >= key
		})
		if i >= len(db.index) {
			db.index = append(db.index, &Entry{key: key, offset: offset})
		} else {
			last := len(db.index) - 1
			db.index = append(db.index, db.index[last])
			copy(db.index[i+1:], db.index[i:last])
			db.index[i] = &Entry{key: key, offset: offset}
		}
	}
	return nil
}

func (db *DB) Close() error {
	if err := db.file.Sync(); err != nil {
		return err
	}
	return db.file.Close()
}
