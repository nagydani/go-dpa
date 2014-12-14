/*
The blockhash package implements a hash tree based fixed block size distributed
data storage.
The block hash of a byte array is defined as follows:

- if size is no more than BlockSize, it is stored in a single block
  blockhash = sha256(uint64(size) + data)

- if size is more than BlockSize*BlockHashCount^l, but no more than BlockSize*
  BlockHashCount^(l+1), the data vector is split into slices of BlockSize*
  BlockHashCount^l length (except the last one).
  blockhash = sha256(uint64(size) + blockhash(slice0) + blockhash(slice1) + ...)
*/

package blockhash

import (
	"crypto/sha256"
	"encoding/binary"
)

const HashSize = 32
const BlockSize = 4096
const BlockHashCount = BlockSize / HashSize

type HashType []byte

/*
The layered (memory, disk, distributed) storage model provides two channels, one
for storing and one for retrieving blocks. The layers are chained so that every
layer can store blocks and try to retrieve them if the previous layer did not
succeed.
*/

type bhtStorage struct {
	store_chn    chan *bhtStoreReq
	retrieve_chn chan *bhtRetrieveReq
	chain        *bhtStorage
}

type bhtNode struct {
	data []byte
	size uint64 // denotes the size of data represented by the whole subtree
}

type bhtStoreReq struct {
	bhtNode
	hash HashType
}

type bhtRetrieveRes struct {
	bhtNode
	req_id int
}

type bhtRetrieveReq struct {
	hash       HashType
	req_id     int
	result_chn chan *bhtRetrieveRes
}

func (h HashType) bits(i, j uint) uint {

	ii := i >> 3
	jj := i & 7
	if ii >= HashSize {
		return 0
	}

	if jj+j <= 8 {
		return uint((h[ii] >> jj) & ((1 << j) - 1))
	}

	res := uint(h[ii] >> jj)
	jj = 8 - jj
	j -= jj
	for j != 0 {
		ii++
		if j < 8 {
			res += uint(h[ii]&((1<<j)-1)) << jj
			return res
		}
		res += uint(h[ii]) << jj
		jj += 8
		j -= 8
	}
	return res
}

func (h HashType) isEqual(h2 HashType) bool {

	for i := range h {
		if h[i] != h2[i] {
			return false
		}
	}
	return true

}

func (s *bhtStorage) Init() {

	s.store_chn = make(chan *bhtStoreReq, 1000)
	s.retrieve_chn = make(chan *bhtRetrieveReq, 1000)

}

// get the root hash of any data vector and store the blocks of the tree if store != nil

func GetBHTroot(data []byte, store chan<- *bhtStoreReq) HashType {

	size := len(data)
	var block []byte

	if size <= BlockSize {
		block = make([]byte, size)
		copy(block[:], data[:])
	} else {
		SubtreeCount := (size + BlockSize - 1) / BlockSize
		SubtreeSize := BlockSize

		for SubtreeCount > BlockHashCount {
			SubtreeCount = (SubtreeCount-1)/BlockHashCount + 1
			SubtreeSize *= BlockHashCount
		}

		block = make([]byte, SubtreeCount*HashSize)

		ptr := 0
		hptr := 0
		for i := 0; i < SubtreeCount; i++ {
			ptr2 := ptr + SubtreeSize
			if ptr2 > size {
				ptr2 = size
			}
			hash := GetBHTroot(data[ptr:ptr2], store)
			copy(block[hptr:hptr+HashSize], hash[:])
			ptr = ptr2
			hptr += HashSize
		}

	}

	hashfn := sha256.New()
	//binary.LittleEndian.PutUint16(b, uint16(i))
	//fmt.Printf("%d\n", size)
	binary.Write(hashfn, binary.LittleEndian, uint64(size))
	hashfn.Write(block)
	hash := hashfn.Sum(nil)

	if store != nil {
		req := new(bhtStoreReq)
		req.data = block
		req.size = uint64(size)
		req.hash = hash
		store <- req
	}

	return hash

}

const MaxReceiveSize = 100000000

// recursive function to retrieve a subtree

func getBHTblock(res *bhtRetrieveRes, data []byte, bsize int, retrv chan<- *bhtRetrieveReq, done chan<- bool) bool {

	if bsize < BlockSize {
		if res.size != uint64(len(data)) {
			if done != nil {
				done <- false
			}
			return false
		}
		copy(data[:], res.data[:])
		if done != nil {
			done <- true
		}
		return true
	}

	size := len(data)
	bcnt := (size + bsize - 1) / bsize

	if len(res.data) != bcnt*HashSize {
		if done != nil {
			done <- false
		}
		return false
	}

	chn := make(chan *bhtRetrieveRes, bcnt)
	sdone := make(chan bool, bcnt)

	for i := 0; i < bcnt; i++ {

		a := i * bsize
		b := a + bsize
		if b > size {
			b = size
		}

		hash := HashType(res.data[i*HashSize : (i+1)*HashSize])
		req := new(bhtRetrieveReq)
		req.hash = hash
		req.req_id = i
		req.result_chn = chn
		retrv <- req

	}

	for j := 0; j < bcnt; j++ {

		res := <-chn

		i := res.req_id
		a := i * bsize
		b := a + bsize
		if b > size {
			b = size
		}

		if int(res.size) != b-a {
			if done != nil {
				done <- false
			}
			return false
		}

		if bsize == BlockSize {
			getBHTblock(res, data[a:b], 0, retrv, sdone)
		} else {
			go getBHTblock(res, data[a:b], bsize/BlockHashCount, retrv, sdone)
		}
	}

	dd := true
	for j := 0; j < bcnt; j++ {
		if !<-sdone {
			dd = false
			break
		}
	}

	if done != nil {
		done <- dd
	}
	return dd

}

// retrieve a data vector of a given block hash from the given storage

func GetBHTdata(hash HashType, retrv chan<- *bhtRetrieveReq) []byte {

	chn := make(chan *bhtRetrieveRes)

	req := new(bhtRetrieveReq)
	req.hash = hash
	req.req_id = 0
	req.result_chn = chn

	retrv <- req
	res := <-chn

	if (res.size == 0) || (res.size > MaxReceiveSize) {
		return nil
	}

	data := make([]byte, res.size)

	bsize := uint64(0)
	if res.size > BlockSize {
		bsize = uint64(BlockSize)
		for bsize*BlockHashCount < res.size {
			bsize *= BlockHashCount
		}
	}

	if !getBHTblock(res, data, int(bsize), retrv, nil) {
		return nil
	}

	return data

}
