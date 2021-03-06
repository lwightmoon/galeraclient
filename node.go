package galeraclient

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"log"

	_ "github.com/go-sql-driver/mysql" //mysql 驱动
	"github.com/go-xorm/xorm"
	"github.com/lwightmoon/galeraclient/consistency"
	"github.com/lwightmoon/galeraclient/settings"
)

const queryStatus = "SHOW STATUS where variable_name LIKE 'wsrep_%' or variable_name like 'Threads_connected'"
const queryGlobalVariables = "SHOW GLOBAL VARIABLES WHERE variable_name in ('wsrep_sync_wait', 'wsrep_causal_reads')"
const defaultTimeout = 2

type Node struct {
	NodeName      string
	dbSetting     *settings.DBSetting
	xormSetting   *settings.XormSetting
	statusSetting *settings.XormSetting
	engine        *xorm.Engine
	statusEngine  *xorm.Engine
	testMode      bool
	status        *Status
}

func newXormEngine(node string, dbSetting *settings.DBSetting, xormSetting *settings.XormSetting) *xorm.Engine {
	connStr := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8", dbSetting.User, dbSetting.Pwd, node, dbSetting.Database)
	engine, err := xorm.NewEngine("mysql", connStr)
	if err != nil {
		log.Printf("create mysql engin fail err:%v", err)
		return nil
	}
	engine.SetMaxOpenConns(xormSetting.MaxConnPerHost)
	engine.SetMaxIdleConns(xormSetting.MaxConnIdlePerHost)
	engine.ShowSQL(xormSetting.ShowSQL)
	return engine
}
func newPxcNode(node string, dbSetting *settings.DBSetting,
	xormSetting *settings.XormSetting, statusSetting *settings.XormSetting, testMode bool) (*Node, error) {
	var statusEngien *xorm.Engine
	if !testMode {
		statusEngien = newXormEngine(node, dbSetting, statusSetting)
	}
	engine := newXormEngine(node, dbSetting, xormSetting)
	if engine == nil {
		return nil, errors.New("create engine fail")
	}
	var nodeStatus *Status
	if testMode {
		nodeStatus = &Status{}
	}
	pxcNode := &Node{
		NodeName:      node,
		dbSetting:     dbSetting,
		xormSetting:   xormSetting,
		statusSetting: statusSetting,
		statusEngine:  statusEngien,
		engine:        engine,
		status:        nodeStatus,
		testMode:      testMode,
	}
	return pxcNode, nil
}

type sessonAndErr struct {
	session *xorm.Session
	err     error
}

func (node *Node) getSession() (*xorm.Session, error) {
	errCh := make(chan sessonAndErr)
	var timeOut int
	if node.xormSetting.ConnTimeout == 0 {
		timeOut = defaultTimeout
	} else {
		timeOut = node.xormSetting.ConnTimeout
	}
	ctx, cancle := context.WithTimeout(context.Background(), time.Duration(timeOut)*time.Second)
	defer cancle()
	go func() {
		session := node.engine.NewSession()
		err := session.Ping()
		if err != nil {
			errCh <- sessonAndErr{session, err}
			return
		}
		if node.xormSetting.ConsistencyLevel != "" {
			errConsist := consistency.Support.Set(session, node.xormSetting.ConsistencyLevel, node.status.SupportSyncWait())
			if errConsist != nil {
				log.Printf("set session consistlevle fail err:%v", errConsist)
			}
			errCh <- sessonAndErr{session, err}
			return
		}
		errCh <- sessonAndErr{session, nil}

	}()
	select {
	case sAndErr := <-errCh:
		return sAndErr.session, sAndErr.err
	case <-ctx.Done():
		return nil, errors.New("get session Timeout")
	}
}

func (node *Node) getSessionWithConsistencyLevel(level interface{}) (*xorm.Session, error) {
	if level == "" {
		return nil, errors.New("level isEmpty")
	}
	session := node.engine.NewSession()
	err := session.Ping()
	if err != nil {
		return nil, err
	}
	err = consistency.Support.Set(session, level, node.status.SupportSyncWait())
	if err != nil {
		log.Printf("set consistency level fail")
	}
	return session, nil
}

/*
获取*xorm.Engine
*/
type engineAndErr struct {
	engine *xorm.Engine
	err    error
}

func (node *Node) getEngine() (*xorm.Engine, error) {
	errCh := make(chan engineAndErr)
	var timeOut int
	if node.xormSetting.ConnTimeout == 0 {
		timeOut = defaultTimeout
	} else {
		timeOut = node.xormSetting.ConnTimeout
	}
	ctx, cancle := context.WithTimeout(context.Background(), time.Duration(timeOut)*time.Second)
	defer cancle()
	go func() {
		engine := node.engine
		err := engine.Ping()
		if err != nil {
			errCh <- engineAndErr{engine, err}
			return
		}

		errCh <- engineAndErr{engine, nil}

	}()
	select {
	case engineAndErr := <-errCh:
		return engineAndErr.engine, engineAndErr.err
	case <-ctx.Done():
		return nil, errors.New("get engine Timeout")
	}
}

func (node *Node) refreshStatus(ctx context.Context) error {
	engine := node.statusEngine
	session := engine.NewSession()
	defer session.Close()
	errCh := make(chan error)
	go func() {
		status := newPxcStatus()
		err := querySatus(session, queryGlobalVariables, status)
		err = querySatus(session, queryStatus, status)
		if err == nil {
			node.status = status
		}
		errCh <- err
	}()
	var err error
	select {
	case <-ctx.Done():
		return errors.New("query status time out")
	case err = <-errCh:
		return err
	}
}

func querySatus(session *xorm.Session, query string, status *Status) error {

	statusTemp, err := session.Query(query)
	if err != nil {
		log.Printf("query global status err:%v", err)
		return err
	}
	for _, row := range statusTemp {
		k, _ := row["Variable_name"]
		v, _ := row["Value"]
		kstr := string(k)
		vstr := string(v)
		status.statusMap[kstr] = string(vstr)
	}
	return nil
}

const showSyncwait = "show variables like 'wsrep_sync_wait'"

func QueryStatusForTest(session *xorm.Session) int {
	statusTemp, err := session.Query(showSyncwait)
	if err != nil {
		log.Printf("query global status err:%v", err)
		return -1
	}
	for _, row := range statusTemp {
		v, _ := row["Value"]
		vstr := string(v)
		res, _ := strconv.Atoi(vstr)
		return res
	}
	return -2
}
func (node *Node) GetStatus() *Status {
	if node.status == nil {
		ctx, cancle := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancle()
		err := node.refreshStatus(ctx)
		if err != nil {
			log.Printf("referesh status err:%v", err)
		}
	}
	return node.status
}
func (node *Node) shutDown() {
	node.OnDown()
	if node.statusEngine != nil {
		node.statusEngine.Close()
	}
}
func (node *Node) OnActive() {
	node.engine = newXormEngine(node.NodeName, node.dbSetting, node.xormSetting)
}
func (node *Node) OnDown() {
	if node.engine != nil {
		node.engine.Close()
		node.engine = nil
	}
}
