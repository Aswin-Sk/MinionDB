// sstable.go
package SSTables

import (
	"encoding/binary"
	"os"
	"sort"
)

const Tombstone = "__deleted__"

type SSTable struct {
	Path string
}

func WriteSSTable(path string, data map[string][]byte) error {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, k := range keys {
		v := data[k]
		var klen, vlen [4]byte
		binary.LittleEndian.PutUint32(klen[:], uint32(len(k)))
		binary.LittleEndian.PutUint32(vlen[:], uint32(len(v)))
		f.Write(klen[:])
		f.Write(vlen[:])
		f.Write([]byte(k))
		f.Write(v)
	}
	return nil
}

func ReadSSTable(path string, key string) ([]byte, bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, false, err
	}
	defer f.Close()

	var klenBuf, vlenBuf [4]byte
	for {
		_, err := f.Read(klenBuf[:])
		if err != nil {
			break
		}
		_, _ = f.Read(vlenBuf[:])
		klen := binary.LittleEndian.Uint32(klenBuf[:])
		vlen := binary.LittleEndian.Uint32(vlenBuf[:])

		kb := make([]byte, klen)
		_, err = f.Read(kb)
		if err != nil {
			break
		}
		vb := make([]byte, vlen)
		_, err = f.Read(vb)
		if err != nil {
			break
		}
		if string(kb) == key {
			return vb, true, nil
		}
	}
	return nil, false, nil
}

func MergeSSTables(outPath string, sst1, sst2 string) error {
	f1, err := os.Open(sst1)
	if err != nil {
		return err
	}
	defer f1.Close()

	f2, err := os.Open(sst2)
	if err != nil {
		return err
	}
	defer f2.Close()

	data := make(map[string][]byte)

	readAll := func(f *os.File) error {
		var klenBuf, vlenBuf [4]byte
		for {
			_, err := f.Read(klenBuf[:])
			if err != nil {
				break
			}
			_, _ = f.Read(vlenBuf[:])
			klen := binary.LittleEndian.Uint32(klenBuf[:])
			vlen := binary.LittleEndian.Uint32(vlenBuf[:])

			kb := make([]byte, klen)
			_, err = f.Read(kb)
			if err != nil {
				return err
			}
			vb := make([]byte, vlen)
			_, err = f.Read(vb)
			if err != nil {
				return err
			}

			data[string(kb)] = vb
		}
		return nil
	}

	if err := readAll(f1); err != nil {
		return err
	}
	if err := readAll(f2); err != nil {
		return err
	}

	for k, v := range data {
		if string(v) == Tombstone {
			delete(data, k)
		}
	}
	return WriteSSTable(outPath, data)
}
