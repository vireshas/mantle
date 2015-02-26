package mantle

import (
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/vireshas/minimal_vitess_pool/pools"
	"strings"
	"time"
)

var MemcachePoolSize = 10
var DefaultMemcacheIpAndHost = []string{"localhost:11211"}

func CreateMemcacheConnection(Instance interface{}) (pools.Resource, error) {
	mcInstance := Instance.(*Memcache)
	hostNPorts := mcInstance.Settings.HostAndPorts
	servers := strings.Join(hostNPorts, ",")
	fmt.Println("connecting to ", servers)
	cli := memcache.New(servers)
	return &MemConn{cli}, nil
}

type MemConn struct {
	*memcache.Client
}

func (m *MemConn) Close() {
}

type Memcache struct {
	Settings PoolSettings
	pool     *ResourcePool
}

func (m *Memcache) GetClient() *MemConn {
	connection, err := m.pool.GetConn(m.Settings.Timeout)
	if err != nil {
		panic(err)
	}
	return connection.(*MemConn)
}

func (m *Memcache) PutClient(c *MemConn) {
	m.pool.PutConn(c)
}

func (m *Memcache) SetDefaults() {
	if len(m.Settings.HostAndPorts) == 0 {
		m.Settings.HostAndPorts = DefaultMemcacheIpAndHost
	}
	if m.Settings.Capacity == 0 {
		m.Settings.Capacity = MemcachePoolSize
	}
	if m.Settings.MaxCapacity == 0 {
		m.Settings.MaxCapacity = MemcachePoolSize
	}
	m.Settings.Timeout = time.Minute
	m.pool = NewPool(CreateMemcacheConnection, m, m.Settings)
}

func (m *Memcache) Configure(settings PoolSettings) {
	m.Settings = settings
	m.SetDefaults()
}

func (m *Memcache) Execute(cmd string, args ...interface{}) (interface{}, error) {
	return "inside GEt", nil
}

func (m *Memcache) Delete(keys ...interface{}) int {
	mc := m.GetClient()
	for _, key := range keys {
		skey := key.(string)
		err := mc.Delete(skey)
		if err != nil {
			fmt.Println("error deleting key:", err)
		}
	}
	m.PutClient(mc)
	return 1
}

func (m *Memcache) Get(key string) string {
	mc := m.GetClient()
	it, erm := mc.Get(key)
	m.PutClient(mc)
	if erm != nil {
		errMsg := fmt.Sprintf("Error in getting key %s: %s", key, erm)
		fmt.Println(errMsg)
		return ""
	}
	return string(it.Value)
}

func (m *Memcache) Set(key string, value interface{}) bool {
	svalue := value.(string)
	mc := m.GetClient()
	erm := mc.Set(&memcache.Item{Key: key, Value: []byte(svalue)})
	m.PutClient(mc)
	if erm != nil {
		fmt.Println(erm)
		return false
	}
	return true
}

func (m *Memcache) MSet(keyValMap map[string]interface{}) bool {
	return false
}

func (m *Memcache) MGet(keys ...interface{}) []string {
	skeys := make([]string, len(keys))
	for _, key := range keys {
		skeys = append(skeys, key.(string))
	}
	mc := m.GetClient()
	items, err := mc.GetMulti(skeys)
	m.PutClient(mc)
	if err != nil {
		return []string{}
	}
	arr := make([]string, 10)
	for _, key := range skeys {
		if item, ok := items[key]; ok {
			arr = append(arr, string(item.Value))
		} else {
			arr = append(arr, "")
		}
	}
	return arr
}

func (m *Memcache) Expire(key string, duration int) bool {
	mc := m.GetClient()
	err := mc.Touch(key, int32(duration))
	m.PutClient(mc)
	if err != nil {
		return false
	}
	return true
}

func (m *Memcache) Setex(key string, duration int, val interface{}) bool {
	mc := m.GetClient()
	defer m.PutClient(mc)
	sval := val.(string)
	erm := mc.Set(&memcache.Item{Key: key, Value: []byte(sval)})
	if erm != nil {
		return false
	} else {
		err := mc.Touch(key, int32(duration))

		if err != nil {
			return false
		}
	}
	return true
}
