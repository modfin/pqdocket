package want

import (
	"fmt"
)

type Counter struct {
	parallelism          int
	parallelismMinByFunc map[string]int
	wantNumGeneral       int
	wantNumByFunc        map[string]int
}

func NewCounter(parallelism int, parallelismMinByFunc map[string]int) (*Counter, error) {

	wantNumByFunc := make(map[string]int)
	wantNumGeneral := parallelism
	for funcName, minParallelism := range parallelismMinByFunc {
		wantNumByFunc[funcName] = minParallelism
		wantNumGeneral--
	}
	if wantNumGeneral < 1 {
		return nil, fmt.Errorf("not enough parallelism for all functions, due to bad WithFuncMinParallelism config")
	}
	return &Counter{
		parallelism:          parallelism,
		parallelismMinByFunc: parallelismMinByFunc,
		wantNumGeneral:       wantNumGeneral,
		wantNumByFunc:        wantNumByFunc,
	}, nil
}

func (c *Counter) Total() int {
	return c.wantNumGeneral + c.WantByFuncNameTotal()
}

func (c *Counter) WantByFuncNameTotal() int {
	num := 0
	for _, v := range c.wantNumByFunc {
		num += v
	}
	return num
}

func (c *Counter) General() int {
	return c.wantNumGeneral
}

func (c *Counter) WantByFuncName() ([]string, []int64) {
	var funcNames []string
	var counts []int64
	for funcName, count := range c.wantNumByFunc {
		funcNames = append(funcNames, funcName)
		counts = append(counts, int64(count))
	}
	return funcNames, counts
}

func (c *Counter) Increment(funcName string) {
	if current, ok := c.wantNumByFunc[funcName]; ok && current < c.parallelismMinByFunc[funcName] {
		c.wantNumByFunc[funcName]++
		return
	}
	c.wantNumGeneral++
}

func (c *Counter) Decrement(funcName string) {
	if current, ok := c.wantNumByFunc[funcName]; ok && current > 0 {
		c.wantNumByFunc[funcName]--
		return
	}
	c.wantNumGeneral--
}
