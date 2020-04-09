package g

import (
	"context"

	"strings"

	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/view"

	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/vim25/mo"
)

//EsxiList get esxi list
func EsxiList(ctx context.Context, c *govmomi.Client) []mo.HostSystem {
	m := view.NewManager(c.Client)
	var esxiList []mo.HostSystem
	v, err := m.CreateContainerView(ctx, c.ServiceContent.RootFolder, []string{"HostSystem"}, true)
	if err != nil {
		Log.Errorln("[esxi.go]", err)
	}
	defer v.Destroy(ctx)

	err = v.Retrieve(ctx, []string{"HostSystem"}, []string{"summary", "datastore"}, &esxiList)
	if err != nil {
		Log.Errorln("[esxi.go]", err)
	}
	return esxiList
}

//esxiAlive power status
func esxiPower(ctx context.Context, c *govmomi.Client, esxi mo.HostSystem, dsWURL *[]DatastoreWithURL) []*MetricValue {
	/*
		1.0: poweredOff
		2.0: poweredOn
		3.0: standBy
		4.0: unknown
	*/
	switch esxi.Summary.Runtime.PowerState {
	case "poweredOff":
		return []*MetricValue{GaugeValue("agent.power", 1.0)}
	case "poweredOn":
		return []*MetricValue{GaugeValue("agent.power", 2.0)}
	case "standBy":
		return []*MetricValue{GaugeValue("agent.power", 3.0)}
	case "unknown":
		return []*MetricValue{GaugeValue("agent.power", 4.0)}
	default:
		return []*MetricValue{GaugeValue("agent.power", 4.0)}
	}
}

//esxiStatus The Status enumeration defines a general "health" value for a managed entity.
func esxiStatus(ctx context.Context, c *govmomi.Client, esxi mo.HostSystem, dsWURL *[]DatastoreWithURL) []*MetricValue {
	/*
		1.0: gray,The status is unknown.
		2.0: green,The entity is OK.
		3.0: red,The entity definitely has a problem.
		4.0: yellow,The entity might have a problem.
	*/
	switch esxi.Summary.OverallStatus {
	case "gray":
		return []*MetricValue{GaugeValue("agent.status", 1.0)}
	case "green":
		return []*MetricValue{GaugeValue("agent.status", 2.0)}
	case "red":
		return []*MetricValue{GaugeValue("agent.status", 3.0)}
	case "yellow":
		return []*MetricValue{GaugeValue("agent.status", 4.0)}
	default:
		return []*MetricValue{GaugeValue("agent.status", 1.0)}
	}
}

//esxiUptime uptime
func esxiUptime(ctx context.Context, c *govmomi.Client, esxi mo.HostSystem, dsWURL *[]DatastoreWithURL) []*MetricValue {
	return []*MetricValue{GaugeValue("agent.uptime", esxi.Summary.QuickStats.Uptime)}
}

//esxiCPU cpu metrics
func esxiCPU(ctx context.Context, c *govmomi.Client, esxi mo.HostSystem, dsWURL *[]DatastoreWithURL) []*MetricValue {
	var total = int64(esxi.Summary.Hardware.CpuMhz) * int64(esxi.Summary.Hardware.NumCpuCores)
	totalCPU := GaugeValue("cpu.total", total*1000*1000)
	useCPU := GaugeValue("cpu.usage.average", int64(esxi.Summary.QuickStats.OverallCpuUsage)*1000*1000)
	usePercentCPU := GaugeValue("cpu.busy", float64(esxi.Summary.QuickStats.OverallCpuUsage)/float64(total)*100)
	freeCPU := GaugeValue("cpu.free.average", (int64(total)-int64(esxi.Summary.QuickStats.OverallCpuUsage))*1000*1000)
	return []*MetricValue{totalCPU, freeCPU, useCPU, usePercentCPU}
}

//esxiMem mem metrics
func esxiMem(ctx context.Context, c *govmomi.Client, esxi mo.HostSystem, dsWURL *[]DatastoreWithURL) []*MetricValue {
	var total = esxi.Summary.Hardware.MemorySize
	var free = int64(esxi.Summary.Hardware.MemorySize) - (int64(esxi.Summary.QuickStats.OverallMemoryUsage) * 1024 * 1024)
	totalMem := GaugeValue("mem.memtotal", total)
	useMem := GaugeValue("mem.memused", int64(esxi.Summary.QuickStats.OverallMemoryUsage)*1024*1024)
	freeMem := GaugeValue("mem.memfree", free)
	freeMemPer := GaugeValue("mem.memfree.percent", float64(free)/float64(total)*100)
	usedMemPer := GaugeValue("mem.memused.percent", float64(total-free)/float64(total)*100)
	return []*MetricValue{totalMem, useMem, freeMem, freeMemPer, usedMemPer}
}

//esxiNet net metrics
func esxiNet(ctx context.Context, c *govmomi.Client, esxi mo.HostSystem, dsWURL *[]DatastoreWithURL) []*MetricValue {
	var netPerf []*MetricValue
	counterNameID := CoID()
	var EsxiNetExtend = []string{"net.bytesRx.average", "net.bytesTx.average"}
	extendID := CounterIDByName(counterNameID, EsxiNetExtend)
	for _, k := range extendID {
		metricPerf, err := Performance(ctx, c, esxi.Self, k)
		if err == nil {
			for _, each := range metricPerf {
				var tags string
				if each.Instance != "" {
					tags = "dev=" + each.Instance
				}
				netPerf = append(netPerf, GaugeValue(each.Metric, each.Value, tags))
			}
		}
	}
	return netPerf
}

//esxiDatastore datastore metrics
func esxiDatastore(ctx context.Context, c *govmomi.Client, esxi mo.HostSystem, dsWURL *[]DatastoreWithURL) []*MetricValue {
	var datastorePerf []*MetricValue
	counterNameID := CoID()
	var EsxiDatastoreExtend = []string{"datastore.totalWriteLatency.average", "datastore.totalReadLatency.average"}
	extendID := CounterIDByName(counterNameID, EsxiDatastoreExtend)
	for _, k := range extendID {
		metricPerf, err := Performance(ctx, c, esxi.Self, k)
		if err == nil {
			for _, each := range metricPerf {
				var tags string
				if each.Instance != "" {
					for _, eachDs := range *dsWURL {
						if strings.Index(eachDs.URL, each.Instance) != -1 {
							tags = "dev=" + eachDs.Datastore
							break
						}
					}
				}
				if each.Instance != "" && tags == "" {
					tags = "dev=" + each.Instance
				}
				datastorePerf = append(datastorePerf, GaugeValue(each.Metric, each.Value, tags))
			}
		}
	}
	return datastorePerf
}

//esxiDisk disk metrics
func esxiDisk(ctx context.Context, c *govmomi.Client, esxi mo.HostSystem, dsWURL *[]DatastoreWithURL) []*MetricValue {
	var diskPerf []*MetricValue
	pc := property.DefaultCollector(c.Client)
	dss := []mo.Datastore{}
	err := pc.Retrieve(ctx, esxi.Datastore, []string{"summary"}, &dss)
	if err != nil {
		Log.Warnln("[esxi.go] get host datastore info err:", err)
	}
	var (
		freeAll  int64
		totalAll int64
		usedAll  int64
	)
	for _, ds := range dss {
		var tags = "datastore=" + ds.Summary.Name
		var free = ds.Summary.FreeSpace
		var total = ds.Summary.Capacity
		var used = total - free
		freeAll += free
		totalAll += total
		usedAll += used
		var freePercent = float64(free) / float64(total) * 100
		var usedPercent = float64(used) / float64(total) * 100
		diskPerf = append(diskPerf, GaugeValue("df.bytes.free", free, tags))
		diskPerf = append(diskPerf, GaugeValue("df.bytes.total", total, tags))
		diskPerf = append(diskPerf, GaugeValue("df.bytes.used", used, tags))
		diskPerf = append(diskPerf, GaugeValue("df.bytes.free.percent", freePercent, tags))
		diskPerf = append(diskPerf, GaugeValue("df.bytes.used.Percent", usedPercent, tags))
	}
	freeAllPercent := float64(freeAll) / float64(totalAll) * 100
	usedAllPercent := float64(usedAll) / float64(totalAll) * 100
	diskPerf = append(diskPerf, GaugeValue("df.statistics.total", totalAll))
	diskPerf = append(diskPerf, GaugeValue("df.statistics.used", usedAll))
	diskPerf = append(diskPerf, GaugeValue("df.statistics.used.percent", usedAllPercent))
	diskPerf = append(diskPerf, GaugeValue("df.statistics.free", freeAll))
	diskPerf = append(diskPerf, GaugeValue("df.statistics.free.percent", freeAllPercent))
	return diskPerf
}

//EsxiMappers Esxi's mapper object
func EsxiMappers() []EFuncsAndInterval {
	interval := Config().Transfer.Interval
	mappers := []EFuncsAndInterval{
		{
			Fs: []func(ctx context.Context, c *govmomi.Client, esxi mo.HostSystem, dsWURL *[]DatastoreWithURL) []*MetricValue{
				esxiPower,
				esxiStatus,
				esxiUptime,
				esxiCPU,
				esxiMem,
				esxiNet,
				esxiDatastore,
				esxiDisk,
			},
			Interval: interval,
		},
	}
	return mappers
}
