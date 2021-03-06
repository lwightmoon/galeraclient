package policies

import (
	"fmt"
	"testing"

	"github.com/lwightmoon/galeraclient/utils"
)

func TestRoundRobin(t *testing.T) {
	roundRobin := &RoundRobinPolicy{}
	nodes := utils.NewSafeSlice()
	nodes.Apppend("node1")
	nodes.Apppend("node2")
	nodes.Apppend("node3")
	for i := 0; i < 1; i++ {
		routineName := fmt.Sprintf("routine:%d", i)
		go func() {
			for j := 0; j < 5; j++ {
				node, _ := roundRobin.ChooseNode(nodes)
				t.Logf("%s,%s", routineName, node)
			}
		}()
	}
}

func TestMaster(t *testing.T) {
	policy := &MasterPolicy{}
	nodes := utils.NewSafeSlice()
	nodes.Apppend("node1")
	nodes.Apppend("node2")
	nodes.Apppend("node3")
	node, _ := policy.ChooseNode(nodes)
	if node != "node1" {
		t.Error("choose error")
	}
}
