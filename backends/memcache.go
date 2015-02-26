package mantle

import (
	"errors"
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

func (m *Memcache) Delete(key string) int {
	mc := m.GetClient()
	err := mc.Delete(key)
	m.PutClient(mc)
	return err
}

func (m *Memcache) Get(key string) (string, error) {
	mc := m.GetClient()
	it, erm := mc.Get(key)
	m.PutClient(mc)
	if erm != nil {
		errMsg := fmt.Sprintf("Error in getting key %s: %s", key, erm)
		return nil, errors.New(errMsg)
	}
	return string(it.Value), nil
}

func (m *Memcache) Set(key string, value string) (bool, error) {
	mc := m.GetClient()
	erm := mc.Set(&memcache.Item{Key: key, Value: []byte(value)})
	m.PutClient(mc)
	if erm != nil {
		return false, errors.New()
	}
	return true, nil
}

func (m *Memcache) MGet(keys []string) ([]string, error) {
	mc := m.GetClient()
	items, err := mc.GetMulti(keys)
	m.PutClient(mc)
	if err != nil {
		return []string{}, err
	}
	arr := make([]string{})
	for _, key := range keys {
		if item, ok := items[key]; ok {
			arr = append(arr, string(item.Value))
		} else {
			arr = append(arr, "")
		}
	}
	return arr, nil
}

func (m *Memcache) Expire(key string, duration int) error {
	mc := m.GetClient()
	err := mc.Touch(key, duration)
	m.PutClient(mc)
	return err
}

func (m *Memcache) SetEx(key string, duration int, val string) error {
	mc := m.GetClient()
	erm := mc.Set(&memcache.Item{Key: key, Value: []byte(val)})
	if erm != nil {
		return fmt.Sprintf("Failed to set key: %s", erm)
	} else {
		err := mc.Touch(key, duration)
		m.PutClient(mc)
		return err
	}
}
