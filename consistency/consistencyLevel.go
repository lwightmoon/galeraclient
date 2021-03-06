package consistency

import (
	"log"

	"github.com/go-xorm/xorm"
)

const (
	CausalReadsOFF = "OFF"
	CausalReadsON  = "ON"
)
const (
	SyncOFF = iota
	SyncReads
	SyncUpdateDelete
	SyncReadUpdateDelete
	SyncInsertReplace
)
const (
	setSessionSyncWait    = "SET SESSION wsrep_sync_wait = ?"
	setSessionCausalReads = "SET SESSION wsrep_causal_reads = ?"
)

var Support *ConsistencySupport

func init() {
	Support = &ConsistencySupport{}
}

type ConsistencySupport struct {
}

func (cs *ConsistencySupport) Set(session *xorm.Session, level interface{}, supportsSyncWait bool) error {
	var err error
	if supportsSyncWait {
		levelInt, ok := level.(int)
		if ok {
			_, err = session.Exec(setSessionSyncWait, levelInt)
		} else {
			log.Print("level type err")
		}

	} else {
		levelStr, ok := level.(string)
		if ok {
			if levelStr == "" {
				levelStr = CausalReadsOFF
			}
			_, err = session.Exec(setSessionCausalReads, levelStr)
		} else {
			log.Print("level type err")
		}
	}
	return err
}
