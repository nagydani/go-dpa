// disk storage layer for the package blockhash
// inefficient work-in-progress version

package blockhash

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
)

const DiskTreeLevels = 2
const DiskTreeLW = 8

type dpaDiskStorage struct {
	dpaStorage
}

func diskStorePathName(hash HashType) (string, string) {

	path := "DPAstore/"
	for i := 0; i < DiskTreeLevels; i++ {
		path += fmt.Sprintf("%02x/", byte(hash.bits(uint(i*DiskTreeLW), uint(DiskTreeLW))))
	}

	name := path + fmt.Sprintf("%064x", hash)

	return path, name

}

func (s *dpaDiskStorage) add(entry *dpaStoreReq) {

	path, name := diskStorePathName(entry.hash)

	_, err := os.Stat(name)
	if os.IsNotExist(err) {

		_, err = os.Stat(path)
		if os.IsNotExist(err) {
			err = os.MkdirAll(path, 0)
			/*			if err != nil {
						fmt.Print("MkdirAll: ")
						fmt.Println(err)
					}*/
		}

		data := make([]byte, len(entry.data)+8)
		binary.LittleEndian.PutUint64(data[0:8], uint64(entry.size))
		copy(data[8:], entry.data[:])

		err = ioutil.WriteFile(name, data, 0)
		/*		if err != nil {
				fmt.Print("WriteFile: ")
				fmt.Println(err)
			}*/
	}
}

func (s *dpaDiskStorage) find(hash HashType) (entry *dpaStoreReq) {

	_, name := diskStorePathName(hash)

	data, err := ioutil.ReadFile(name)
	if err != nil {
		return nil
	}

	hashfn := sha256.New()
	hashfn.Write(data)
	hh := HashType(hashfn.Sum(nil))

	if !hh.isEqual(hash) {
		//		fmt.Printf("Find: hash of stored block is %064x; expected %064x\n", hh, hash)
		return nil
	}

	entry = new(dpaStoreReq)
	entry.hash = hash
	entry.data = make([]byte, len(data)-8)
	copy(entry.data[:], data[8:])
	entry.size = int64(binary.LittleEndian.Uint64(data[0:8]))

	return entry

}

func (s *dpaDiskStorage) process_store(req *dpaStoreReq) {

	s.add(req)

	if s.chain != nil {
		s.chain.store_chn <- req
	}

}

func (s *dpaDiskStorage) process_retrieve(req *dpaRetrieveReq) {

	entry := s.find(req.hash)
	if entry == nil {
		if s.chain != nil {
			s.chain.retrieve_chn <- req
			return
		}
	}

	res := new(dpaRetrieveRes)
	if entry != nil {
		res.dpaNode = entry.dpaNode
	}
	res.req_id = req.req_id
	req.result_chn <- res

}

func (s *dpaDiskStorage) Init(ch *dpaStorage) {

	s.dpaStorage.Init()
	s.chain = ch

}

func (s *dpaDiskStorage) Run() {

	for {
		bb := true
		for bb {
			select {
			case store := <-s.store_chn:
				s.process_store(store)
			default:
				bb = false
			}
		}
		select {
		case store := <-s.store_chn:
			s.process_store(store)
		case retrv := <-s.retrieve_chn:
			s.process_retrieve(retrv)
		}
	}

}
