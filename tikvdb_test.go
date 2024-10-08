package db

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

var pdAddr = []string{"127.0.0.1:2379"}

//var pdAddr = []string{"192.168.0.166:2379"}

func TestTikvDBNewTikvDB(t *testing.T) {
	name := fmt.Sprintf("testname%x", randStr(12))
	dir := fmt.Sprintf("testdir%x", randStr(12))

	// Test we can't open the db twice for writing
	wr1, err := NewTikvDBWithOpts(name, dir, pdAddr)
	require.Nil(t, err)
	_, err = NewTikvDBWithOpts(name, dir, pdAddr)
	require.NotNil(t, err)
	wr1.Close() // Close the db to release the lock
}

func BenchmarkTikvDBRandomReadsWrites(b *testing.B) {
	name := fmt.Sprintf("testname%x", randStr(12))
	dir := fmt.Sprintf("testdir%x", randStr(12))

	db, err := NewTikvDBWithOpts(name, dir, pdAddr)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	benchmarkRandomReadsWrites(b, db)
}

// 迭代器测试
func TestTikvIter(t *testing.T) {
	//name := fmt.Sprintf("testname%x", randStr(12))
	//dir := fmt.Sprintf("testdir%x", randStr(12))

	db, err := NewTikvDBWithOpts("app", "test", pdAddr)
	if err != nil {
		t.Fatal(err)
	}
	//defer db.Close()

	result, err := db.Get([]byte("s/k:capability/n~"))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(result))

	num := int64(10)
	var keys [][]byte

	for i := int64(0); i < num; i++ {
		bytes := int642Bytes(i)
		err := db.Set(bytes, bytes)
		if err != nil {
			// require.NoError() is very expensive (according to profiler), so check manually
			t.Fatal(t, err)
		}
		keys = append(keys, bytes)
	}

	for _, key := range keys {
		get, err := db.Get(key)
		require.NoError(t, err)
		fmt.Printf("[%X]:\t[%X]\n", key, get)
	}

	//iter, err := db.Iterator([]byte(" D:\\work\\tianzhou-v1\\data/application/s/1"), []byte(" D:\\work\\tianzhou-v1\\data/application/s/100000"))
	//var rootKeyFormat = keyformat.NewKeyFormat('r', 8) // r<version>
	/*txn, err := db.txn.Begin()
	if err != nil {
		panic(err)
	}
	defer txn.Commit(context.Background())*/
	//source, err := txn.IterReverse([]byte("D:\\work\\tianzhou-v1\\data/application/r"))
	//value, err := db.Get([]byte("D:\\work\\tianzhou-v1\\data/application/s/k:acc/r"))
	//var version int64
	//rootKeyFormat.Scan(value, &version)
	//fmt.Println(fmt.Sprintf("key: %v, data: %v\n", string(iter.Key()), string(iter.Value())))
	//fmt.Println(",value:", version)
	itr, err := db.Iterator([]byte("s/k:capability/n"), []byte("s/k:capability/s"))
	if err != nil {
		panic(err)
	}
	for ; itr.Valid(); itr.Next() {
		//var version int64
		//rootKeyFormat.Scan(itr.Key(), &version)
		//fmt.Println(fmt.Sprintf("key: %v, data: %v\n", string(iter.Key()), string(iter.Value())))
		fmt.Println("key:", string(itr.Key()), ",value:", string(itr.Value()))
	}
	itr.Close()
	//iter, err := db.Iterator([]byte("s/k:feegrant/r"), []byte("s/k:feegrant"))
	//require.NoError(t, err)
	//count := 0
	//for ; iter.Valid(); iter.Next() {
	//	//fmt.Println(fmt.Sprintf("key: %v, data: %v\n", string(iter.Key()), string(iter.Value())))
	//	fmt.Println("key:", string(iter.Key()))
	//	fmt.Println(count)
	//	count++
	//}
	//iter.Close()
	//require.EqualValues(t, num, count)
}

// 迭代器批量数据自动测试
func BenchmarkTikvDBRangeScans1M(b *testing.B) {
	name := fmt.Sprintf("testname%x", randStr(12))
	dir := fmt.Sprintf("testdir%x", randStr(12))

	db, err := NewTikvDBWithOpts(name, dir, pdAddr)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	benchmarkRangeScans(b, db, int64(10001))
}

// 反迭代器测试
func TestTikvReverseIter(t *testing.T) {
	name := fmt.Sprintf("testname%x", randStr(12))
	dir := fmt.Sprintf("testdir%x", randStr(12))

	db, err := NewTikvDBWithOpts(name, dir, pdAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	num := int64(10)
	var keys [][]byte

	for i := int64(0); i < num; i++ {
		bytes := int642Bytes(i)
		err := db.Set(bytes, bytes)
		if err != nil {
			// require.NoError() is very expensive (according to profiler), so check manually
			t.Fatal(t, err)
		}
		keys = append(keys, bytes)
	}

	for _, key := range keys {
		get, err := db.Get(key)
		require.NoError(t, err)
		fmt.Printf("[%X]:\t[%X]\n", key, get)
	}

	iter, err := db.ReverseIterator(int642Bytes(2), int642Bytes(6))
	require.NoError(t, err)
	count := 0
	for ; iter.Valid(); iter.Next() {
		fmt.Printf("key: %v, data: %v\n", string(iter.Key()), iter.Value())
		count++
	}
	iter.Close()
	//require.EqualValues(t, num, count)
}
