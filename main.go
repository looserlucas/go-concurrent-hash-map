package main

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"sync"
	"sync/atomic"
)

var ShardNumber = 64
var BufferNumber = 16

type hashMap []*hashMapComponent

type hashMapComponent struct {
	items       map[string]*atomic.Value
	writeOpChan chan *writeOp
	sync.RWMutex
}

func CreateHashMap() hashMap {
	hm := make(hashMap, ShardNumber)
	for i := 0; i < ShardNumber; i++ {
		hm[i] = &hashMapComponent{items: make(map[string]*atomic.Value), writeOpChan: make(chan *writeOp, BufferNumber)}
	}
	return hm
}

type writeOp struct {
	shardID  uint32
	isDelete bool
	data     interface{}
	key      string
	confirm  chan interface{}
}

func (hs hashMap) sendWriteRequestToChannel(key string, data interface{}) interface{} {
	shardID := getHash(key) % uint32(ShardNumber)
	write := &writeOp{data: data, key: key, shardID: shardID, isDelete: false, confirm: make(chan interface{}, 1)}

	hs[shardID].writeOpChan <- write
	log := <-write.confirm
	return log
}

func (hs hashMap) sendDeleteRequestToChannel(key string) interface{} {
	shardID := getHash(key) % uint32(ShardNumber)
	write := &writeOp{key: key, shardID: shardID, isDelete: true, confirm: make(chan interface{}, 1)}

	hs[shardID].writeOpChan <- write
	log := <-write.confirm
	return log
}

func (hs hashMap) SetKey(key string, data interface{}) bool {
	log := hs.sendWriteRequestToChannel(key, data)
	fmt.Println(log)
	return true
}
func (hs hashMap) DeleteKey(key string) bool {
	log := hs.sendDeleteRequestToChannel(key)
	fmt.Println(log)
	return true
}

func (hs hashMap) GetShardID(key string) uint32 {
	h := getHash(key)
	return h % uint32(ShardNumber)
}

func (hs hashMap) GetKey(key string) (interface{}, bool) {
	ok := false
	sid := hs.GetShardID(key)
	if !hs.Has(key) {
		return nil, false
	}
	hs[sid].RLock()
	val := hs[sid].items[key].Load()
	hs[sid].RUnlock()
	if val != nil {
		ok = true
	}
	return val, ok
}

func (hs hashMap) listenToWriteAndDelete(shardID int) {
	for {
		select {
		case request := <-hs[shardID].writeOpChan:
			if request.isDelete {
				ok := hs.Has(request.key)
				if !ok {
					request.confirm <- false
				} else {
					hs[shardID].Lock()
					delete(hs[shardID].items, request.key)
					hs[shardID].Unlock()
					request.confirm <- true
				}
			} else {
				if hs.Has(request.key) {
					// update key
					hs[shardID].items[request.key].Store(request.data)
					request.confirm <- request
				} else {
					// create new key
					var tg atomic.Value
					tg.Store(request.data)
					/*
						unsafePointerOld := unsafe.Pointer(request.item)
						unsafePointerNew := unsafe.Pointer(a)
						//CompareAndSwap
						atomic.CompareAndSwapPointer(&unsafePointerOld, unsafePointerOld, unsafePointerNew)
					*/
					//					runtime.LockOSThread()
					hs[shardID].Lock()
					hs[shardID].items[request.key] = &tg
					//					runtime.UnlockOSThread()
					hs[shardID].Unlock()
					request.confirm <- request
				}
			}
		}
	}
}

func (hs hashMap) Has(key string) bool {
	shardID := hs.GetShardID(key)
	hs[shardID].RLock()
	ok := hs[shardID].items[key] != nil
	hs[shardID].RUnlock()
	return ok
	//return hs[hs.GetShardID(key)].items[key] != nil
}

//getHash hash string to uint32
func getHash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

type MapObject struct {
	Key   string
	Value interface{}
}

func (hs hashMap) Iter() <-chan *MapObject {
	chans := getItemsFromShards(hs)
	ch := make(chan *MapObject)
	go manyToOne(chans, ch)
	return ch
}

func (hs hashMap) IterBuffered() <-chan *MapObject {
	chans := getItemsFromShards(hs)
	total := 0
	for _, c := range chans {
		total = total + cap(c)
	}
	ch := make(chan *MapObject, total)
	go manyToOne(chans, ch)
	return ch
}

func getItemsFromShards(hs hashMap) (chans []chan *MapObject) {
	chans = make([]chan *MapObject, ShardNumber)
	wg := sync.WaitGroup{}
	wg.Add(ShardNumber)

	for i, shard := range hs {
		go func(i int, shard *hashMapComponent) {
			chans[i] = make(chan *MapObject, len(shard.items))
			wg.Done()

			//concurrently streaming data to chan[i]
			for key := range shard.items {
				chans[i] <- &MapObject{Key: key, Value: shard.items[key].Load()}
			}

			close(chans[i])
		}(i, shard)
	}
	wg.Wait()
	return chans
}

func manyToOne(inputChans []chan *MapObject, outputChan chan *MapObject) {
	wg := sync.WaitGroup{}
	wg.Add(len(inputChans))
	//put all input from input channels to one output channel
	for _, ch := range inputChans {
		go func(ch chan *MapObject) {
			for obj := range ch {
				outputChan <- obj
			}
			wg.Done()
		}(ch)
	}
	wg.Wait()
	close(outputChan)
}

func main() {
	hm := CreateHashMap()
	for i := 0; i < ShardNumber; i++ {
		go hm.listenToWriteAndDelete(i)
	}
	/*
		for i := 0; i < 10; i++ {
			a := strconv.Itoa(i)
			hm.SetKey(a, a)
		}
	*/
	var wg1 *sync.WaitGroup
	wg1 = &sync.WaitGroup{}
	wg1.Add(3500)
	for i := 0; i < 1000; i++ {
		go func(i int) {
			//index := rand.Intn(1000)
			//a := strconv.Itoa(index)
			a := strconv.Itoa(i)
			hm.SetKey(a, a)
			wg1.Done()
		}(i)
	}
	for i := 0; i < 500; i++ {
		go func(i int) {
			//index := rand.Intn(1000)
			//a := strconv.Itoa(index)
			a := strconv.Itoa(i)
			//b := strconv.Itoa(i + 1)
			//hm.SetKey(a, b)
			hm.DeleteKey(a)
			wg1.Done()
		}(i)
	}

	for i := 0; i < 2000; i++ {
		go func(i int) {
			//index := rand.Intn(1000)
			//a := strconv.Itoa(index)
			a := strconv.Itoa(i)
			//b := strconv.Itoa(i + 1)
			//hm.SetKey(a, b)
			fmt.Println(hm.GetKey(a))
			wg1.Done()
		}(i)
	}

	wg1.Wait()

	count := 0
	for _ = range hm.Iter() {
		count++
	}
	fmt.Println("count: ")
	fmt.Print(count)

}
