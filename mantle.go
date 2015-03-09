package mantle

import (
	set "github.com/deckarep/golang-set"
	"github.com/goibibo/mantle/backends"
)

//only strings are supported
type Mantle interface {
	Get(key string) (string, error)
	Set(key string, value interface{}) (bool, error)
	Delete(keys ...interface{}) (int, error)
	Setex(key string, duration int, value interface{}) (bool, error)
	MGet(keys ...interface{}) ([]string, error)
	MSet(keyValMap map[string]interface{}) (bool, error)
	Expire(key string, duration int) (bool, error)
	Execute(cmd string, args ...interface{}) (interface{}, error)

	//Set methods used by cassandra and redis
	Smembers(key string) (set.Set, error)
	SAdd(key string, value interface{}) (bool, error)
	SRem(key string, value string) (bool, error)
}

type MantleSQL interface {
	Select(key string) ([]map[string]interface{}, error)
}

//helper func
func redisConns(settings mantle.PoolSettings) *mantle.Redis {
	redis := &mantle.Redis{}
	redis.Configure(settings)
	return redis
}

func mySQLConns(settings mantle.PoolSettings) *mantle.MySQL {
	mySQL := &mantle.MySQL{}
	mySQL.Configure(settings)
	return mySQL
}

func memcacheConns(settings mantle.PoolSettings) *mantle.Memcache {
	redis := &mantle.Memcache{}
	redis.Configure(settings)
	return redis
}

//generic pool settings
func getSettings(o *Orm) mantle.PoolSettings {
	return mantle.PoolSettings{
		HostAndPorts: o.HostAndPorts,
		Capacity:     o.Capacity,
		MaxCapacity:  o.Capacity,
		Options:      o.Options}

}

//This struct is exported
type Orm struct {
	//redis|memcache|cassandra
	Driver string
	//arrays of ip:port,ip:port
	HostAndPorts []string
	//pool size
	Capacity int
	//any other options thats needed for creating a connection
	Options map[string]string
}

//mantle  wrapper for NoSQL DBs.
func (o *Orm) New() Mantle {
	settings := getSettings(o)
	if o.Driver == "memcache" {
		return memcacheConns(settings)
	} else {
		return redisConns(settings)
	}
}

//mantle  wrapper for SQL DBs.
func (o *Orm) NewSQL() MantleSQL {
	settings := getSettings(o)
	if o.Driver == "mysql" {
		return mySQLConns(settings)
	} else {
		return nil
	}
}

//override mantle and get a redis client
func (o *Orm) GetRedisConn() (*mantle.RedisConn, error) {
	settings := getSettings(o)
	redisPool := redisConns(settings)
	return redisPool.GetClient()
}
