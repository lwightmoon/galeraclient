package settings

import (
	"strings"
	"time"

	"github.com/lwightmoon/galeraclient/policies"
)

//xorm 配置
type XormSetting struct {
	MaxConnPerHost     int
	MaxConnIdlePerHost int
	ConnTimeout        int
	ShowSQL            bool
	ConsistencyLevel   interface{}
}

func NewXormSetting(maxConn, maxConnIdle int, timeout int,
	showSQL bool, consistencyLevel string) *XormSetting {
	return &XormSetting{
		maxConn,
		maxConnIdle,
		timeout,
		showSQL,
		consistencyLevel,
	}
}

//db配置
type DBSetting struct {
	Database string
	User     string
	Pwd      string
}

func NewDBSetting(database, user, pwd string) *DBSetting {
	return &DBSetting{database, user, pwd}
}

//client 配置

type ClientSetting struct {
	Nodes                 []string
	RetriesToGetConn      int
	ElectionPolicy        policies.ElectionPolicy
	DefaultElectionPolicy policies.ElectionPolicy
	TestMode              bool
}

func NewClientSetting(nodeStr string, retryCnt int,
	policy policies.ElectionPolicy, testMode bool) *ClientSetting {
	nodes := strings.Split(nodeStr, ",")
	if len(nodes) == 0 {
		return nil
	}
	defaultPolicy := &policies.MasterPolicy{}
	return &ClientSetting{
		Nodes:            nodes,
		RetriesToGetConn: retryCnt,
		// ConnTimeout:           connTimeout,
		ElectionPolicy:        policy,
		DefaultElectionPolicy: defaultPolicy,
		TestMode:              testMode,
	}
}

type DiscoverSetting struct {
	Period time.Duration
	//When this flag is true, donor nodes are marked as down, so you will not get connections from donor nodes.
	IgnoreDonor bool
}
