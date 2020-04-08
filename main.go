package main

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"os"
	"runtime"
	"time"

	"vsphere/cron"
	"vsphere/g"

	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/vim25/soap"
)

func main() {
	cfg := flag.String("c", "cfg.json", "configuration file")
	version := flag.Bool("v", false, "show version")

	flag.Parse()
	if *version {
		fmt.Println(g.VERSION)
		os.Exit(0)
	}

	g.InitLog("info")
	g.ParseConfig(*cfg)
	g.ParseExtendConfig(g.Config().Extend)
	go g.ReloadConfig(*cfg)
	go g.ReloadExtendConfig(g.Config().Extend)

	if !g.Config().Transfer.N9eMode {
		g.InitRPCClients()
	}

	t := time.NewTicker(time.Duration(g.Config().Transfer.Interval) * time.Second)
	defer t.Stop()
	for {
		for _, vc := range g.Config().Vsphere {
			go vcCollect(vc)
		}
		g.Log.Debugln("[main.go] the number of goroutine:", runtime.NumGoroutine())
		<-t.C
	}

}

func vcCollect(vc *g.VsphereConfig) {
	ctx := context.Background()
	u, err := soap.ParseURL(vc.Addr)
	if err != nil {
		g.Log.Errorln("[main.go]", err)
	}
	u.User = url.UserPassword(vc.User, vc.Pwd)
	c, err := govmomi.NewClient(ctx, u, true)

	if err != nil {
		g.Log.Warnln("[main.go] the vc", vc.Addr, "connection error:", err)
		return
	}

	if c.IsVC() {
		g.Log.Infoln("[main.go] the vc", vc.Addr, "connection successful!")
	}

	cron.Collect(ctx, c, vc)
	defer c.Logout(ctx)
}
