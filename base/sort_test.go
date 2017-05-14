package base

import (
	"testing"
	"sort"
	"reflect"
	"fmt"
)

type GameDownloadItem struct {
	GameID        int // 游戏ID
	DownloadTimes int // 下载次数
}

func (self GameDownloadItem) String() string {
	return fmt.Sprintf("<Item(%d, %d)>", self.GameID, self.DownloadTimes)
}

type GameDownloadSlice []*GameDownloadItem

func (p GameDownloadSlice) Len() int {
	return len(p)
}

func (p GameDownloadSlice) Swap(i int, j int) {
	p[i], p[j] = p[j], p[i]
}

// 根据游戏下载量 降序 排列
func (p GameDownloadSlice) Less(i int, j int) bool {
	return p[i].DownloadTimes > p[j].DownloadTimes
}



func TestSort(t *testing.T) {
	jobs := make([]*Job, 0, 10)

	job := NewJob(Job {
		Share: 0.1,
		JobID: 1,
	})
	jobs = append(jobs, job)

	sort.Sort(JobShareSort(jobs))

	if !reflect.DeepEqual(jobs[0], job) {
		t.Error("sort by share error")
	}
}