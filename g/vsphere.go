package g

import (
	"context"
	"fmt"

	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/view"
	"github.com/vmware/govmomi/vim25/mo"
)

func datastores(ctx context.Context, c *govmomi.Client) []mo.Datastore {
	m := view.NewManager(c.Client)
	v, err := m.CreateContainerView(ctx, c.ServiceContent.RootFolder, []string{"Datastore"}, true)
	if err != nil {
		Log.Warnln("[vsphere.go]", err)
	}
	defer v.Destroy(ctx)

	var dss []mo.Datastore
	err = v.Retrieve(ctx, []string{"Datastore"}, []string{"summary"}, &dss)
	if err != nil {
		Log.Warnln("[vsphere.go]", err)
	}

	if le := len(dss); le != 0 {
		return dss
	}
	return nil
}

//DatastoreMetrics datastore metrics
func DatastoreMetrics(ctx context.Context, c *govmomi.Client) (L []*MetricValue) {
	dss := datastores(ctx, c)
	if dss != nil {
		for _, ds := range dss {
			tags := fmt.Sprintf("ds=%s,fstype=%s", ds.Summary.Name, ds.Summary.Type)
			freePercent := float64(ds.Summary.FreeSpace) / float64(ds.Summary.Capacity) * 100
			L = append(L, GaugeValue("datastore.bits.total", ds.Summary.Capacity, tags))
			L = append(L, GaugeValue("datastore.bits.free", ds.Summary.FreeSpace, tags))
			L = append(L, GaugeValue("datastore.free.percent", freePercent, tags))
		}
	}
	return
}

//VsphereMappers vsphere's mappers object
func VsphereMappers() []VFuncsAndInterval {
	interval := Config().Transfer.Interval
	mappers := []VFuncsAndInterval{
		{
			Fs: []func(ctx context.Context, c *govmomi.Client) []*MetricValue{
				DatastoreMetrics,
				AgentMetrics,
			},
			Interval: interval,
		},
	}
	return mappers
}
