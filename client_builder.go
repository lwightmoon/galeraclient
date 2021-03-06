package galeraclient

import (
	"time"

	"github.com/lwightmoon/galeraclient/policies"
	"github.com/lwightmoon/galeraclient/settings"
)

type ClientBuilder struct {
	testMode           bool
	nodes              string
	discoverPeriod     time.Duration
	database           string
	user               string
	pwd                string
	maxConnPerHost     int
	maxConnIdlePerHost int
	connTimeout        int
	showSQL            bool
	policy             policies.ElectionPolicy
	ignoreDonor        bool
	consistencyLevel   interface{}
}

func (builder *ClientBuilder) TestMode(testMode bool) *ClientBuilder {
	builder.testMode = testMode
	return builder
}

func (builder *ClientBuilder) Nodes(nodes string) *ClientBuilder {
	builder.nodes = nodes
	return builder
}

// DiscoverPeriod 探测可用节点时间间隔
func (builder *ClientBuilder) DiscoverPeriod(period time.Duration) *ClientBuilder {
	builder.discoverPeriod = period
	return builder
}
func (builder *ClientBuilder) Database(database string) *ClientBuilder {
	builder.database = database
	return builder
}
func (builder *ClientBuilder) User(user string) *ClientBuilder {
	builder.user = user
	return builder
}
func (builder *ClientBuilder) Pwd(pwd string) *ClientBuilder {
	builder.pwd = pwd
	return builder
}
func (builder *ClientBuilder) MaxConnPerHost(connPerHost int) *ClientBuilder {
	builder.maxConnPerHost = connPerHost
	return builder
}
func (builder *ClientBuilder) MaxConnIdlePerHost(connIdlePerHost int) *ClientBuilder {
	builder.maxConnIdlePerHost = connIdlePerHost
	return builder
}
func (builder *ClientBuilder) ConnTimeout(timeout int) *ClientBuilder {
	builder.connTimeout = timeout
	return builder
}
func (builder *ClientBuilder) ShowSQL(showSQL bool) *ClientBuilder {
	builder.showSQL = showSQL
	return builder
}
func (builder *ClientBuilder) ElectionPolicy(policy policies.ElectionPolicy) *ClientBuilder {
	builder.policy = policy
	return builder
}
func (builder *ClientBuilder) IgnoreDonor(isIgnore bool) *ClientBuilder {
	builder.ignoreDonor = isIgnore
	return builder
}
func (builder *ClientBuilder) ConsistencyLevel(consistencyLevel interface{}) *ClientBuilder {
	builder.consistencyLevel = consistencyLevel
	return builder
}
func (builder *ClientBuilder) Build() *Client {
	xormSetting := &settings.XormSetting{
		MaxConnIdlePerHost: builder.maxConnIdlePerHost,
		MaxConnPerHost:     builder.maxConnPerHost,
		ShowSQL:            builder.showSQL,
		ConsistencyLevel:   builder.consistencyLevel,
		ConnTimeout:        builder.connTimeout,
	}
	dbSetting := &settings.DBSetting{
		Database: builder.database,
		User:     builder.user,
		Pwd:      builder.pwd,
	}
	clientSetting := settings.NewClientSetting(builder.nodes, 3,
		builder.policy, builder.testMode)
	discoverSetting := &settings.DiscoverSetting{
		Period:      builder.discoverPeriod,
		IgnoreDonor: builder.ignoreDonor,
	}
	client := newClient(clientSetting, dbSetting, xormSetting, discoverSetting)
	return client
}
