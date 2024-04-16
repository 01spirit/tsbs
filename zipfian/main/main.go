package main

import (
	"github.com/timescale/tsbs/zipfian/counter"
	"github.com/timescale/tsbs/zipfian/distributionGenerator"
	"math/rand"
	"time"
)

func main() {
	zipfian := distributionGenerator.NewZipfianWithItems(100, distributionGenerator.ZipfianConstant)
	zipfian = distributionGenerator.NewZipfianWithRange(15, 100, distributionGenerator.ZipfianConstant)
	rz := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < 10; i++ {
		println(zipfian.Next(rz))
	}

	cntr := counter.NewCounter(100)
	latest := distributionGenerator.NewSkewedLatest(cntr)
	rl := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < 10; i++ {
		println(latest.Next(rl))
	}

}
