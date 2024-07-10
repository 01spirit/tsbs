package influx

import (
	"fmt"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/zipfian/distributionGenerator"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestQueryTime(t *testing.T) {

	// 加入两个分布，用于生成随机时间范围
	zipfian := distributionGenerator.NewZipfianWithItems(10, distributionGenerator.ZipfianConstant)

	zipNums := make([]int64, 0)
	rz := rand.New(rand.NewSource(time.Now().UnixNano()))

	items := float64(0)

	var mu sync.Mutex
	random := func() {
		mu.Lock()

		zipNum := zipfian.Next(rz)
		zipNums = append(zipNums, zipNum)

		fmt.Printf("zipnum:\t%d\n", zipNum)

		timeDuration := utils.ZipFianTimeDuration[zipNum].Seconds()
		if zipNum < 5 {
			items += timeDuration / time.Hour.Seconds() * (60 / 5)
		} else {
			items += timeDuration / time.Hour.Seconds() * (60 / 15)
		}

		mu.Unlock()
	}

	var wg sync.WaitGroup
	for i := 0; i < 200000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			random()
		}()
	}

	// 等待所有goroutine完成
	wg.Wait()

	meanItems := items / 200000

	fmt.Printf("mean items : %f\n", meanItems)

}
