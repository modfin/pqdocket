package want

import (
	"fmt"
	"slices"
	"strings"
)

type Counter struct {
	parallelism       int
	parallelismGroups []ParallelismGroup
	wantNumGeneral    int
	wantNumByGroup    []ParallelismGroup
}

type ParallelismGroup struct {
	Functions []string
	Count     int
}

func NewCounter(parallelism int, parallelismGroups []ParallelismGroup) (*Counter, error) {

	var wantNumByFunc []ParallelismGroup
	wantNumGeneral := parallelism
	for _, g := range parallelismGroups {
		if slices.ContainsFunc(g.Functions, func(s string) bool {
			return strings.Contains(s, ",")
		}) {
			return nil, fmt.Errorf("functions names cannot contain commas")
		}
		wantNumByFunc = append(wantNumByFunc, g)
		wantNumGeneral -= g.Count
	}
	if wantNumGeneral < 1 {
		return nil, fmt.Errorf("not enough parallelism for all functions, due to bad WithDedicatedParallelismGroup config")
	}
	return &Counter{
		parallelism:       parallelism,
		parallelismGroups: parallelismGroups,
		wantNumGeneral:    wantNumGeneral,
		wantNumByGroup:    wantNumByFunc,
	}, nil
}

func (c *Counter) Total() int {
	return c.wantNumGeneral + c.WantByGroupsTotal()
}

func (c *Counter) WantByGroupsTotal() int {
	num := 0
	for _, v := range c.wantNumByGroup {
		num += v.Count
	}
	return num
}

func (c *Counter) FunctionsHandledByGroups() map[string]bool {
	funcs := make(map[string]bool)
	for _, v := range c.wantNumByGroup {
		for _, f := range v.Functions {
			funcs[f] = true
		}
	}
	return funcs
}

func (c *Counter) General() int {
	return c.wantNumGeneral
}

func (c *Counter) WantByGroup() ([]string, []int64) {
	var groupFuncNames []string
	var counts []int64
	for _, g := range c.wantNumByGroup {
		groupFuncNames = append(groupFuncNames, strings.Join(g.Functions, ","))
		counts = append(counts, int64(g.Count))
	}
	return groupFuncNames, counts
}

func (c *Counter) Increment(funcName string) {
	for i := range c.wantNumByGroup {
		if slices.Contains(c.wantNumByGroup[i].Functions, funcName) {
			c.wantNumByGroup[i].Count++
			if c.wantNumByGroup[i].Count > c.parallelismGroups[i].Count { // TODO remove
				panic("shouldn't happen, count should never exceed group parallelism")
			}
			return
		}
	}

	c.wantNumGeneral++
	if c.wantNumGeneral > c.parallelism { // TODO remove
		panic("general: shouldn't happen, count should never exceed parallelism")
	}
}

func (c *Counter) Decrement(funcName string) {
	for i := range c.wantNumByGroup {
		if slices.Contains(c.wantNumByGroup[i].Functions, funcName) {
			c.wantNumByGroup[i].Count--
			if c.wantNumByGroup[i].Count < 0 { // TODO remove
				fmt.Println(c.wantNumByGroup[i].Count, c.parallelismGroups[i].Count)
				panic("group: shouldn't happen, count should never be lower than 0")
			}
			return
		}
	}
	c.wantNumGeneral--
	if c.wantNumGeneral < 0 { // TODO remove
		panic("general: shouldn't happen, count should never go below 0")
	}
}
