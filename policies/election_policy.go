package policies

import (
	"sync/atomic"

	"github.com/lwightmoon/galeraclient/utils"
)

type ElectionPolicy interface {
	ChooseNode(nodes utils.Slice) (string, error)
}

//选取第一个node
type MasterPolicy struct {
}

//找不到node
type NoNodeError struct {
}

func (ne *NoNodeError) Error() string {
	return "no node"
}

func (mp *MasterPolicy) ChooseNode(nodes utils.Slice) (string, error) {
	if nodes.IsEmpty() {
		return "", &NoNodeError{}
	}
	node := nodes.Get(0)
	if node == nil {
		return "", &NoNodeError{}
	}
	nodeStr, ok := node.(string)
	if ok {
		return nodeStr, nil
	}
	return "", &NoNodeError{}
}

//轮训
type RoundRobinPolicy struct {
	nextNodeIndex int32
}

func (rp *RoundRobinPolicy) ChooseNode(nodes utils.Slice) (string, error) {
	rp.getNextIndex()
	size := nodes.Size()
	if size == 0 {
		return "", &NoNodeError{}
	}
	nodeIndex := rp.nextNodeIndex % int32(size)
	node := nodes.Get(int(nodeIndex))
	if node == nil {
		return "", &NoNodeError{}
	}
	nodeStr := node.(string)
	return nodeStr, nil
}

func (rp *RoundRobinPolicy) getNextIndex() {
	atomic.AddInt32(&rp.nextNodeIndex, 1)
	if rp.nextNodeIndex > 1000000 {
		atomic.StoreInt32(&rp.nextNodeIndex, 0)
	}
}
