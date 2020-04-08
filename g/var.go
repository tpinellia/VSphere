package g

import (
	"context"
	"net/rpc"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/vim25/mo"
)

const (
	//VERSION version
	VERSION = "1.1.0"
	//LOGFILE log file
	LOGFILE = "./var/vsphere.log"
)

//Log global logger
var Log = logrus.New()

//OldModTime global config 's mTime
var OldModTime int64

//OldExtendTime extend config 's mTime
var OldExtendTime int64

//LocalIP the vc ip
var LocalIP string

//HbsClient hbs client
var HbsClient *SingleConnRPCClient

//SimpleRPCResponse rpc
type SimpleRPCResponse struct {
	Code int `json:"code"`
}

//SingleConnRPCClient rpc client
type SingleConnRPCClient struct {
	sync.Mutex
	rpcClient *rpc.Client
	RPCServer string
	Timeout   time.Duration
}

//AgentReportRequest agent report format
type AgentReportRequest struct {
	Hostname      string
	IP            string
	AgentVersion  string
	PluginVersion string
}

//MetricValue metric format
type MetricValue struct {
	Endpoint  string      `json:"endpoint"`
	Metric    string      `json:"metric"`
	Value     interface{} `json:"value"`
	Step      int64       `json:"step"`
	Type      string      `json:"counterType"`
	Tags      string      `json:"tags"`
	Timestamp int64       `json:"timestamp"`
}

//VFuncsAndInterval vc mapper format
type VFuncsAndInterval struct {
	Fs       []func(ctx context.Context, c *govmomi.Client) []*MetricValue
	Interval int
}

//EFuncsAndInterval esxi mapper format
type EFuncsAndInterval struct {
	Fs       []func(ctx context.Context, c *govmomi.Client, esxi mo.HostSystem, dsWURL *[]DatastoreWithURL) []*MetricValue
	Interval int
}

//DatastoreWithURL datastore with url
type DatastoreWithURL struct {
	Datastore string
	URL       string
}

//TransferResponse transfer return format
type TransferResponse struct {
	Message string
	Total   int
	Invalid int
	Latency int64
}

//MetricPerf vsphere performance
type MetricPerf struct {
	Metric   string `json:"metric"`
	Value    int64  `json:"value"`
	Instance string `json:"instance"`
}
