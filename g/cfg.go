package g

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"time"
)

//HeartbeatConfig hbs configuration
type HeartbeatConfig struct {
	Enabled bool   `json:"enabled"`
	Addr    string `json:"addr"`
	Timeout int    `json:"timeout"`
}

//TransferConfig transfer configuration
type TransferConfig struct {
	Enabled  bool     `json:"enabled"`
	N9eMode  bool     `json:"n9eMode"`
	Addr     []string `json:"addrs"`
	Interval int      `json:"interval"`
	Timeout  int      `json:"timeout"`
}

//VsphereConfig vsphere configuration
type VsphereConfig struct {
	Hostname     string `json:"hostname"`
	IP           string `json:"ip"`
	Addr         string `json:"addr"`
	User         string `json:"user"`
	Pwd          string `json:"pwd"`
	Port         int    `json:"port"`
	Split        bool   `json:"split"`
	EndpointHead string `json:"endpointhead"`
	MetricHead   string `json:"metrichead"`
	Extend       bool   `json:"extend"`
}

//GlobalConfig global configuration
type GlobalConfig struct {
	Debug     bool             `json:"debug"`
	Extend    string           `json:"extend"`
	Heartbeat *HeartbeatConfig `json:"heartbeat"`
	Transfer  *TransferConfig  `json:"transfer"`
	Vsphere   []*VsphereConfig `json:"vsphere"`
}

//ExtendConfig extend configuration
type ExtendConfig struct {
	Hbr            []string `json:"hbr"`
	Rescpu         []string `json:"rescpu"`
	StoragePath    []string `json:"storagePath"`
	StorageAdapter []string `json:"storageAdapter"`
	Power          []string `json:"power"`
	Sys            []string `json:"sys"`
	Net            []string `json:"net"`
	Disk           []string `json:"disk"`
	CPU            []string `json:"cpu"`
	Datastore      []string `json:"datastore"`
	Mem            []string `json:"mem"`
}

var (
	config *GlobalConfig
	extend *ExtendConfig
	lock   = new(sync.RWMutex)
	//ConfigFile global config file
	ConfigFile string
)

//Config get global config
func Config() *GlobalConfig {
	lock.RLock()
	defer lock.RUnlock()
	return config
}

//Extend get extend config
func Extend() *ExtendConfig {
	lock.RLock()
	defer lock.RUnlock()
	return extend
}

//ParseConfig parse global config
func ParseConfig(cfg string) {
	if cfg == "" {
		Log.Fatalln("[cfg.go] use -c to specify configuration file")
	}

	fileInfo, err := os.Stat(cfg)
	if err != nil {
		if os.IsNotExist(err) {
			Log.Fatalln("[cfg.go] config file: ", cfg, " is not existent.Maybe you need `mv cfg.example.json cfg.json`")
		}
	}

	OldModTime = fileInfo.ModTime().Unix()

	b, err := ioutil.ReadFile(cfg)
	if err != nil {
		Log.Fatalln("[cfg.go] read config file:", cfg, "fail:", err)
	}
	configContent := strings.TrimSpace(string(b))
	var c GlobalConfig
	err = json.Unmarshal([]byte(configContent), &c)

	if err != nil {
		Log.Fatalln("[cfg.go] parse config file:", cfg, "fail:", err)
	}

	lock.Lock()
	defer lock.Unlock()

	config = &c
	if config.Debug {
		InitLog("debug")
	} else {
		InitLog("info")
	}
	Log.Info("[cfg.go] read global config file: ", cfg, " successfully!")
}

//ReloadConfig monitor and reload global config
func ReloadConfig(cfg string) {
	t := time.NewTicker(time.Second * 5)
	defer t.Stop()
	for {
		<-t.C
		f, _ := os.Open(cfg)
		fileInfo, _ := f.Stat()
		curModTime := fileInfo.ModTime().Unix()
		if curModTime > OldModTime {
			Log.Infoln("[cfg.go] reload global config...")
			ParseConfig(cfg)
		}
	}
}

//ParseExtendConfig parse extend config
func ParseExtendConfig(cfg string) {
	if cfg == "" {
		Log.Fatalln("[cfg.go] extend config file:", cfg, "is not existent.")
	}

	fileInfo, err := os.Stat(cfg)
	if err != nil {
		if os.IsNotExist(err) {
			Log.Fatalln("[cfg.go] extend config file: ", cfg, " is not existent.")
		}
	}

	OldExtendTime = fileInfo.ModTime().Unix()

	b, err := ioutil.ReadFile(cfg)
	if err != nil {
		Log.Fatalln("[cfg.go] read extend config file:", cfg, "fail:", err)
	}
	configContent := strings.TrimSpace(string(b))
	var c ExtendConfig
	err = json.Unmarshal([]byte(configContent), &c)

	if err != nil {
		Log.Fatalln("[cfg.go] parse extend config file:", cfg, "fail:", err)
	}

	lock.Lock()
	defer lock.Unlock()

	extend = &c
	Log.Info("[cfg.go] read extend config file: ", cfg, " successfully!")
}

//ReloadExtendConfig monitor and reload extend config
func ReloadExtendConfig(cfg string) {
	t := time.NewTicker(time.Second * 5)
	defer t.Stop()
	for {
		<-t.C
		f, _ := os.Open(cfg)
		fileInfo, _ := f.Stat()
		curExtendTime := fileInfo.ModTime().Unix()
		if curExtendTime > OldExtendTime {
			Log.Infoln("[cfg.go] reload extend config...")
			ParseExtendConfig(cfg)
		}
	}
}
