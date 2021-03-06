package galeraclient

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/lwightmoon/galeraclient/policies"

	"log"

	"github.com/go-xorm/xorm"
	"github.com/lwightmoon/galeraclient/settings"
	"github.com/lwightmoon/galeraclient/utils"
)

type Client struct {
	nodes       *utils.SafeMap
	activeNodes utils.Slice
	downedNodes utils.Slice

	dbSetting   *settings.DBSetting   //db配置
	xormSetting *settings.XormSetting //xorm 配置

	clientSetting *settings.ClientSetting //客户端配置

	discoversetting *settings.DiscoverSetting //节点探测配置

	discoverRunning int32 //探测是否在运行
}

func newClient(clientSetting *settings.ClientSetting,
	dbSetting *settings.DBSetting, xormSetting *settings.XormSetting, discoverSetting *settings.DiscoverSetting) *Client {
	client := new(Client)
	client.clientSetting = clientSetting
	client.dbSetting = dbSetting
	client.xormSetting = xormSetting
	client.discoversetting = discoverSetting
	client.nodes = utils.NewSafeMap()
	client.activeNodes = utils.NewSafeSlice()
	client.downedNodes = utils.NewSafeSlice()
	client.registNodes()
	client.startDiscovery(client.discoversetting.Period)
	return client
}

/*
获取engine
*/
func (client *Client) GetEngine() *xorm.Engine {
	elecPolicy := client.clientSetting.ElectionPolicy
	if elecPolicy == nil {
		elecPolicy = client.clientSetting.DefaultElectionPolicy
	}
	node := client.selectNode(elecPolicy)
	if node != nil {
		engine, err := node.getEngine()
		if err != nil {
			client.discovery()
			return nil
		}
		return engine
	}
	log.Println("get no node")
	return nil
}

/*
GetSession 获取session
*/
func (client *Client) GetSession() *xorm.Session {
	elecPolicy := client.clientSetting.ElectionPolicy
	if elecPolicy == nil {
		elecPolicy = client.clientSetting.DefaultElectionPolicy
	}
	node := client.selectNode(elecPolicy)
	if node != nil {
		session, err := node.getSession()
		if err != nil {
			log.Printf("get session err:%v", err)
			client.discovery()
			return nil
		}
		return session
	}
	return nil
}

/*
GetSession 获取session
*/
func (client *Client) GetSessionWithLevel(level interface{}) *xorm.Session {
	elecPolicy := client.clientSetting.ElectionPolicy
	if elecPolicy == nil {
		elecPolicy = client.clientSetting.DefaultElectionPolicy
	}
	node := client.selectNode(elecPolicy)
	if node != nil {
		session, err := node.getSessionWithConsistencyLevel(level)
		if err != nil {
			log.Printf("get session err:%v", err)
			client.discovery()
			return nil
		}
		return session
	}
	return nil
}

/*
获取session 指定隔离级别
*/
func (client *Client) GetSessionWithConsistencyLevel(level interface{}) *xorm.Session {
	elecPolicy := client.clientSetting.ElectionPolicy
	if elecPolicy == nil {
		elecPolicy = client.clientSetting.DefaultElectionPolicy
	}
	node := client.selectNode(elecPolicy)
	if node != nil {
		session, err := node.getSessionWithConsistencyLevel(level)
		if err != nil {
			client.discovery()
			return nil
		}
		return session
	}
	return nil
}
func (client *Client) selectNode(elecPolicy policies.ElectionPolicy) *Node {
	node, err := client.getActiveNode(1, elecPolicy)
	if err != nil {
		log.Printf("can not get active node err:%v", err)
		return nil
	}
	log.Printf("get node nodeName:%s", node.NodeName)
	return node
}
func (client *Client) getActiveNode(retry int, elecPolicy policies.ElectionPolicy) (*Node, error) {
	if client.activeNodes.IsEmpty() {
		return nil, errors.New("there is no active node")
	}
	if retry <= client.clientSetting.RetriesToGetConn {
		nodeName, err := elecPolicy.ChooseNode(client.activeNodes)
		log.Printf("seled nodeName:%s", nodeName)
		if err != nil {
			log.Printf("sel err:%v", err)
			retry++
			client.getActiveNode(retry, elecPolicy)
		}
		node, exist := client.nodes.Get(nodeName)
		if !exist {
			retry++
			client.getActiveNode(retry, elecPolicy)
		} else {
			nodeReal, isNode := node.(*Node)
			if isNode {
				return nodeReal, nil
			}
			log.Println("node type is err")
		}
		log.Println("thers is no active nodes")

	}
	return nil, errors.New("no node found")
}

func (client *Client) startDiscovery(period time.Duration) {
	if !client.clientSetting.TestMode {
		log.Println("not test mode start Discovery")
		tickerCh := time.NewTicker(client.discoversetting.Period).C
		go func() {
			for {
				client.discovery()
				<-tickerCh
			}
		}()
	} else {
		log.Println("test mode don't discovery")
	}
}

//注册节点
func (client *Client) registNodes() {
	nodes := client.clientSetting.Nodes
	for _, node := range nodes {
		client.registNode(node)
	}
}

func (client *Client) registNode(nodeName string) {
	pxcNode, err := newPxcNode(nodeName, client.dbSetting, client.xormSetting,
		client.xormSetting, client.clientSetting.TestMode)
	if err != nil {
		log.Printf("crate node fail err:%v", err)
		return
	}
	client.nodes.Set(nodeName, pxcNode)
	err = client.discover(nodeName)
	if err != nil {
		client.down(nodeName, "fail in connection err "+err.Error())
	}

}

/*
检测所有节点
*/
func (client *Client) discovery() {
	if !atomic.CompareAndSwapInt32(&client.discoverRunning, 0, 1) {
		return
	}
	if client.nodes.IsEmpty() {
		log.Println("Reinitializing from nodes")
		client.registNodes()
	}
	log.Println("discovering pxc cluster")
	client.discoverActiveNodes()
	client.testDownedNodes()

	atomic.CompareAndSwapInt32(&client.discoverRunning, 1, 0)
}

func (client *Client) testDownedNodes() {
	nodeCh := client.downedNodes.Iter()
	for nodeItem := range nodeCh {
		nodeName := nodeItem.Value
		nodeNameStr, ok := nodeName.(string)
		if ok {
			err := client.discover(nodeNameStr)
			if err != nil {
				client.down(nodeNameStr, "fail to connect")
				continue
			}
			node, existNode := client.nodes.Get(nodeNameStr)
			if existNode {
				nodeReal, isNode := node.(*Node)
				if isNode {
					if !(nodeReal.status.IsDonor() && client.discoversetting.IgnoreDonor) && nodeReal.status.IsPrimary() {
						client.activate(nodeNameStr)
					}
				}
			}
		}
	}
}

//测试
func (client *Client) discoverActiveNodes() {
	nodeCh := client.activeNodes.Iter()
	for nodeItem := range nodeCh {
		nodeName := nodeItem.Value
		nodeNameStr, ok := nodeName.(string)
		if ok {
			err := client.discover(nodeNameStr)
			if err != nil {
				client.down(nodeNameStr, "fail to connect")
			}
		}
	}
}

//check node if is active
func (client *Client) discover(nodeName string) error {
	log.Printf("discovering %s", nodeName)
	var status *Status
	var err error
	status, err = client.refreshStatus(nodeName)
	if err != nil {
		log.Printf("could not refresh node:%s so remove it err:%v", nodeName, err)
		client.removeNode(nodeName)
		return err
	}
	if !status.IsPrimary() {
		log.Printf("On discover node:%s is not primary", nodeName)
		client.down(nodeName, "non Primary")
		return nil
	}
	if !status.IsSynced() && (client.discoversetting.IgnoreDonor || !status.IsDonor()) {
		client.down(nodeName, "state not ready "+status.State())
		return nil
	}
	discoverNodes := status.GetClusterNodes()
	log.Printf("cluster nodes:%v", discoverNodes)
	//remove unexist node
	if !containsStr(discoverNodes, nodeName) {
		log.Printf("discovered nodes does not contains %s", nodeName)
		client.removeNode(nodeName)
	} else {
		if !client.isActive(nodeName) && !(status.IsDonor() && client.discoversetting.IgnoreDonor) {
			log.Printf("will discover a discovered node:%s", nodeName)
			client.activate(nodeName)
		}
	}
	for _, discoverNode := range discoverNodes {
		if client.IsNewNodeOnCluster(discoverNode) {
			log.Printf("found new node:%s", discoverNode)
			client.registNode(discoverNode)
		}
	}
	log.Printf("discover------------ node:%s end", nodeName)
	return nil
}

func containsStr(list []string, value string) bool {
	for _, v := range list {
		if v == value {
			return true
		}
	}
	return false
}

//更新节点状态
func (client *Client) refreshStatus(nodeName string) (*Status, error) {
	if client.clientSetting.TestMode {
		return newTestStatus(nodeName), nil
	}
	node, exist := client.nodes.Get(nodeName)
	if exist {
		nodeReal, ok := node.(*Node)
		if ok {
			ctx, cancle := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancle()
			err := nodeReal.refreshStatus(ctx)
			if err != nil {
				log.Printf("client refresh status err:%v", err)
				return nil, err
			}
			status := nodeReal.GetStatus()
			return status, nil
		}

	}
	return nil, errors.New("no node in nodes")
}

//是否新加入集群
func (client *Client) IsNewNodeOnCluster(nodeName string) bool {
	return !client.nodes.Contains(nodeName)
}

//删除节点
func (client *Client) removeNode(nodeName string) {
	client.activeNodes.Remove(nodeName)
	client.downedNodes.Remove(nodeName)
	client.shutDown(nodeName)
	client.nodes.Del(nodeName)
	log.Printf("remove node:%s", nodeName)
}

//close engine and status engin
func (client *Client) shutDown(nodeName string) {
	log.Printf("shut down node:%s", nodeName)
	node, _ := client.nodes.Get(nodeName)
	nodeReal, ok := node.(*Node)
	if ok {
		nodeReal.shutDown()
	}
}

//remove from activity
func (client *Client) down(nodeName, cause string) {
	log.Printf("mark node:%s as down due to %s", nodeName, cause)
	client.activeNodes.Remove(nodeName)
	if !client.downedNodes.Contains(nodeName) {
		client.downedNodes.Apppend(nodeName)
	}
}

//close engine
func (client *Client) closeNode(nodeName string) {
	node, _ := client.nodes.Get(nodeName)
	nodeReal, ok := node.(*Node)
	if ok {
		nodeReal.OnDown()
	}
}

func (client *Client) isActive(nodeName string) bool {
	return client.activeNodes.Contains(nodeName)
}
func (client *Client) activate(nodeName string) {
	if !client.isActive(nodeName) {
		log.Printf("active node:%s", nodeName)
		node, ok := client.nodes.Get(nodeName)
		if ok {
			nodeReal, isNode := node.(*Node)
			if isNode {
				nodeReal.OnActive()
			}
		}
		client.activeNodes.Apppend(nodeName)
		client.downedNodes.Remove(nodeName)
	}
}
