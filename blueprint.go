package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	BlueprintVersion = 1
	ChunkSize = 256 * 1024
)

type Blueprint struct {
	Version int `json:"version"`
	FileName string `json:"file_name"`
	FileSize int64 `json:"file_size"`
	ChunkSize uint32 `json:"chunk_size"`
	Hashes []string `json:"hashes"`    
	InfoHash string `json:"info_hash"` 
}

func (bp *Blueprint) TotalChunks() uint32 { 
	return uint32(len(bp.Hashes)) 
}

func (bp *Blueprint) ComputeInfoHash() string {
	h := sha256.New()

	h.Write([]byte("P2Pv1"))
	_ = binary.Write(h, binary.BigEndian, uint32(bp.ChunkSize))
	_ = binary.Write(h, binary.BigEndian, uint64(bp.FileSize))
	_ = binary.Write(h, binary.BigEndian, uint32(len(bp.Hashes)))

	for _, hexHash := range bp.Hashes {
		raw, err := hex.DecodeString(hexHash)
		if err != nil {
			return ""
		}
		h.Write(raw)
	}

	return hex.EncodeToString(h.Sum(nil))
}

func (bp *Blueprint) Stub() FileStub {
	return FileStub{
		InfoHash: bp.InfoHash,
		FileName: bp.FileName,
		FileSize: bp.FileSize,
	}
}


func CreateBlueprint(path string) (*Blueprint, error) {
	f, err := os.Open(path) 
	if err != nil {         
		return nil, err
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.IsDir() {
		return nil, fmt.Errorf("%s is a directory, not a file", path)
	}

	bp := &Blueprint{
		Version:   BlueprintVersion,
		FileName:  filepath.Base(path), 
		FileSize:  st.Size(),
		ChunkSize: ChunkSize,
	}

	buf := make([]byte, ChunkSize)

	for {
		n, err := io.ReadFull(f, buf)

		if n > 0 {
			sum := sha256.Sum256(buf[:n])
			bp.Hashes = append(bp.Hashes, hex.EncodeToString(sum[:]))
		}

		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break 
		}
		if err != nil {
			return nil, err
		}
	}

	bp.InfoHash = bp.ComputeInfoHash()
	return bp, nil
}
