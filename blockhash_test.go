// test bench for the package blockhash

package blockhash

import (
	"math"
	"math/rand"
	"testing"
)

func maketest(l int) []byte {

	r := rand.New(rand.NewSource(int64(l)))

	test := make([]byte, l)
	for i := 0; i < l; i++ {
		test[i] = byte(r.Intn(256))
	}

	return test
}

func cmptest(a, b []byte) bool {

	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

const testcnt = 80

func testlen(i int) int {
	return int(0.5 + math.Exp2(3.0+float64(i)/5))
}

func TestBlockHashStorage(t *testing.T) {

	t.Logf("Creating DiskStorage...")

	diskstore := new(bhtDiskStorage)
	diskstore.Init(nil)
	go diskstore.Run()

	t.Logf("Creating MemStorage...")

	memstore := new(bhtMemStorage)
	memstore.Init(&diskstore.bhtStorage)
	go memstore.Run()

	t.Logf("Storing test vectors...")

	test := make([][]byte, testcnt)
	hash := make([]HashType, testcnt)
	for i := 0; i < testcnt; i++ {
		test[i] = maketest(testlen(i))
		//t.Logf("Test[%d] = %x", i, test[i])
		hash[i] = GetBHTroot(test[i], memstore.store_chn)
		//t.Logf("Hash[%d] = %x", i, hash[i])
	}

	t.Logf("Retrieving test vectors...")

	for i := 0; i < testcnt; i++ {
		tt := GetBHTdata(hash[i], memstore.retrieve_chn)
		//t.Logf("Retrieved[%d] = %x", i, tt)
		if cmptest(test[i], tt) {
			t.Logf("Test case %d passed (test vector length %d)", i, len(tt))
		} else {
			t.Errorf("Test case %d failed", i)
		}
	}

}
