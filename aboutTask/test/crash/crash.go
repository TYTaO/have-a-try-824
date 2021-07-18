package crash

import (
	crand "crypto/rand"
	myRpc "have-try-6.824/aboutTask/rpc"
	"math/big"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

func maybeCrash() {
	max := big.NewInt(1000)
	rr, _ := crand.Int(crand.Reader, max)
	if rr.Int64() < 0 { // 330
		// crash!
		os.Exit(1)
	} else if rr.Int64() < 660 {
		// delay for a while.
		maxms := big.NewInt(10 * 1000)
		ms, _ := crand.Int(crand.Reader, maxms)
		time.Sleep(time.Duration(ms.Int64()) * time.Millisecond)
	}
}

func Map(filename string, contents string) []myRpc.KeyValue {
	maybeCrash()

	kva := []myRpc.KeyValue{}
	kva = append(kva, myRpc.KeyValue{"a", filename})
	kva = append(kva, myRpc.KeyValue{"b", strconv.Itoa(len(filename))})
	kva = append(kva, myRpc.KeyValue{"c", strconv.Itoa(len(contents))})
	kva = append(kva, myRpc.KeyValue{"d", "xyzzy"})
	return kva
}

func Reduce(key string, values []string) string {
	maybeCrash()

	// sort values to ensure deterministic output.
	vv := make([]string, len(values))
	copy(vv, values)
	sort.Strings(vv)

	val := strings.Join(vv, " ")
	return val
}
