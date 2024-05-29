package common

import (
	"fmt"
	"math/rand"
	"reflect"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	internalutils "github.com/timescale/tsbs/internal/utils"
)

const (
	errMoreItemsThanScale = "cannot get random permutation with more items than scale"
)

// Core is the common component of all generators for all systems
type Core struct {
	// Interval is the entire time range of the dataset
	Interval *internalutils.TimeInterval

	// Scale is the cardinality of the dataset in terms of devices/hosts
	Scale int
}

// NewCore returns a new Core for the given time range and cardinality
func NewCore(start, end time.Time, scale int) (*Core, error) {
	ti, err := internalutils.NewTimeInterval(start, end)
	if err != nil {
		return nil, err
	}

	return &Core{Interval: ti, Scale: scale}, nil
}

// PanicUnimplementedQuery generates a panic for the provided query distributionGenerator.
func PanicUnimplementedQuery(dg utils.QueryGenerator) {
	panic(fmt.Sprintf("database (%v) does not implement query", reflect.TypeOf(dg)))
}

// GetRandomSubsetPerm returns a subset of numItems of a permutation of numbers from 0 to totalNumbers,
// e.g., 5 items out of 30. This is an alternative to rand.Perm and then taking a sub-slice,
// which used up a lot more memory and slowed down query generation significantly.
// The subset of the permutation should have no duplicates and thus, can not be longer that original set
// Ex.: 12, 7, 25 for numItems=3 and totalItems=30 (3 out of 30)
func GetRandomSubsetPerm(numItems int, totalItems int) ([]int, error) {
	if numItems > totalItems {
		// Cannot make a subset longer than the original set
		return nil, fmt.Errorf(errMoreItemsThanScale)
	}

	seen := map[int]bool{}
	res := make([]int, numItems)
	for i := 0; i < numItems; i++ {
		for {
			n := rand.Intn(totalItems)
			// Keep iterating until a previously unseen int is found
			if !seen[n] {
				seen[n] = true
				res[i] = n
				break
			}
		}
	}
	return res, nil
}

func GetContinuousRandomSubset() ([]int, error) {
	// 要取的结果数量
	//resNum := (rand.Intn(4) + 1) * 10 // 生成 [0,4) 的一个整数｛0,1,2,3｝，加一变成 ｛1,2,3,4｝，乘十变成｛10,20,30，40｝
	resNum := 10
	//resNum := 50
	// 结果数组
	result := make([]int, resNum)

	// 随机取某区间内的该数量的整数
	switch resNum {
	case 10:
		//section := rand.Intn(10) * 10 // 生成[0,10) 的一个整数，乘 10 变成{0,10,20....90}
		section := rand.Intn(19) * 5 // 生成[0,20) 的一个整数，乘 5 变成{0,5,15....90}
		fmt.Printf("result number: %d\tsection number: %d\n", resNum, section)
		for i := 0; i < resNum; i++ {
			result[i] = section
			section++
		}
		break
	case 20:
		section := rand.Intn(5) * 20 // 生成[0,5) 的一个整数{0,1,2,3,4}，乘二十变成{0,20,40,60，80}
		fmt.Printf("result number: %d\tsection number: %d\n", resNum, section)
		for i := 0; i < resNum; i++ {
			result[i] = section
			section++
		}
		break
	case 30:
		section := rand.Intn(3) * 30 // 生成[0,3) 的一个整数｛0,1,2｝，乘三十变成{0,30,60}
		fmt.Printf("result number: %d\tsection number: %d\n", resNum, section)
		for i := 0; i < resNum; i++ {
			result[i] = section
			section++
		}
		break
	case 40:
		section := rand.Intn(2) * 40 // 生成[0,2) 的一个整数{0,1}，乘十变成{0,40}
		fmt.Printf("result number: %d\tsection number: %d\n", resNum, section)
		for i := 0; i < resNum; i++ {
			result[i] = section
			section++
		}
		break
	default:
		// todo 生成 50 个
		section := rand.Intn(19) * 5 * 5 // 生成[0,20) 的一个整数，乘 25 变成{0,25,50....450}
		fmt.Printf("result number: %d\tsection number: %d\n", resNum, section)
		for i := 0; i < resNum; i++ {
			result[i] = section
			section++
		}
		break
	}

	return result, nil
}
