package galeraclient

import (
	"strings"
)

const (
	inCommingAddr = "wsrep_incoming_addresses"
	primary       = "Primary"

	syncWait    = "wsrep_sync_wait"
	causalReads = "wsrep_causal_reads"

	clusterStatus = "wsrep_cluster_status"
	statusDonor   = "Donor/Desynced"
	statusSynced  = "Synced"
	stateVariable = "wsrep_local_state_comment"

	threadsConnected = "Threads_connected"
)

type Status struct {
	statusMap map[string]string
}

func newPxcStatus() *Status {
	return &Status{
		statusMap: make(map[string]string, 0),
	}
}
func newTestStatus(nodeName string) *Status {
	status := newPxcStatus()
	status.statusMap[clusterStatus] = primary
	status.statusMap[stateVariable] = statusSynced
	status.statusMap[inCommingAddr] = nodeName
	return status
}
func exist(params map[string]string, key string) bool {
	_, exist := params[key]
	return exist
}
func (status *Status) GetThreadConnected() string {
	v, _ := status.statusMap[threadsConnected]
	return v
}
func (status *Status) GetClusterNodes() []string {
	nodeStr, _ := status.statusMap[inCommingAddr]
	nodes := strings.Split(nodeStr, ",")
	return nodes
}
func (status *Status) IsPrimary() bool {
	v, _ := status.statusMap[clusterStatus]
	return v == primary
}

func (status *Status) IsSynced() bool {
	v, _ := status.statusMap[stateVariable]
	return v == statusSynced
}
func (status *Status) IsDonor() bool {
	v, _ := status.statusMap[stateVariable]
	return v == statusDonor
}
func (status *Status) State() string {
	v, _ := status.statusMap[stateVariable]
	return v
}
func (status *Status) SupportSyncWait() bool {
	return exist(status.statusMap, syncWait)
}

func (status *Status) GetGlobalConsistencyLevel() string {
	if status.SupportSyncWait() {
		v, _ := status.statusMap[syncWait]
		return v
	}
	v, _ := status.statusMap[causalReads]
	return v
}
