package mantle

import (
	"github.com/garyburd/redigo/redis"
	"github.com/vireshas/minimal_vitess_pool/pools"
	set "github.com/deckarep/golang-set"
	"strconv"
	"strings"
	"time"
)

//cant make these guys const as []string is not allowed in consts

//default pool size
var RedisPoolSize = 10

//default host:port to connect
var DefaultRedisConfig = []string{"localhost:6379"}

//This method creates a redis connection
//CreateRedisConnection is passed as a callback to pools
//Instance:
//  This is a reference to a struct redis instance
//  Connect needs some params like db, hostAndPorts
//  These params are read from this instance rederence
func CreateRedisConnection(Instance interface{}) (pools.Resource, error) {
	//converting interface Redis struct type
	redisInstance := Instance.(*Redis)
	//this is a string of type "localhost:6379"
	hostNPorts := redisInstance.Settings.HostAndPorts
	//select db after dialing
	db := redisInstance.db

	//panic is more than 1 ip is given
	if len(hostNPorts) > 1 {
		panic("we can only connect to 1 server at the moment")
	}

	hostNPort := strings.Split(hostNPorts[0], ":")
	//dial host and port
	cli, err := redis.Dial("tcp", hostNPort[0]+":"+hostNPort[1])
	if err != nil {
		panic(err)
	}

	//select a redis db
	_, err = cli.Do("SELECT", db)
	if err != nil {
		panic(err)
	}

	//typecast to RedisConn
	return &RedisConn{cli}, nil
}

// Wrapping redigo redis connection
//   Pool expects a Object which defines
//   Close() and doesn't return anything, but
//   redigo.Redis#Close() returns error, hence this wrapper
//   around redis.Conn
type RedisConn struct {
	redis.Conn
}

//Close a redis connection
func (r *RedisConn) Close() {
	_ = r.Conn.Close()
}

//Gets a connection from pool and converts to RedisConn type
//If all the connections are in use, timeout the present request after a minute
func (r *Redis) GetClient() (*RedisConn, error) {
	connection, err := r.pool.GetConn(r.Settings.Timeout)
	if err != nil {
		return nil, err
	}
	return connection.(*RedisConn), nil
}

//Puts a connection back to pool
func (r *Redis) PutClient(c *RedisConn) {
	r.pool.PutConn(c)
}

type Redis struct {
	Settings PoolSettings
	pool     *ResourcePool
	db       int
}

//Add default settings if they are missing
func (r *Redis) SetDefaults() {
	if len(r.Settings.HostAndPorts) == 0 {
		r.Settings.HostAndPorts = DefaultRedisConfig
	}
	//this is poolsize
	if r.Settings.Capacity == 0 {
		r.Settings.Capacity = RedisPoolSize
	}
	//maxcapacity of the pool
	if r.Settings.MaxCapacity == 0 {
		r.Settings.MaxCapacity = RedisPoolSize
	}
	//pool timeout
	r.Settings.Timeout = time.Minute

	//select a particular db in redis
	db, ok := r.Settings.Options["db"]
	if !ok {
		db = "0"
	}
	select_db, err := strconv.Atoi(db)
	if err != nil {
		panic("From Redis: select db is not a valid string")
	}

	r.db = select_db

	//create a pool finally
	r.pool = NewPool(CreateRedisConnection, r, r.Settings)
}

func (r *Redis) Configure(settings PoolSettings) {
	r.Settings = settings
	r.SetDefaults()
}

//Generic method to execute any redis call
//Gets a client from pool, executes a cmd, puts conn back in pool
func (r *Redis) Execute(cmd string, args ...interface{}) (interface{}, error) {
	client, err := r.GetClient()
	if err != nil {
		return nil, err
	}
	defer r.PutClient(client)
	return client.Do(cmd, args...)
}

func (r *Redis) Delete(keys ...interface{}) int {
	value, err := redis.Int(r.Execute("DEL", keys...))
	if err != nil {
		return -1
	}
	return value
}

func (r *Redis) Get(key string) string {
	value, err := redis.String(r.Execute("GET", key))
	if err != nil {
		return ""
	}
	return value
}

func (r *Redis) Set(key string, value interface{}) bool {
	_, err := r.Execute("SET", key, value)
	if err != nil {
		return false
	}
	return true
}

func (r *Redis) MGet(keys ...interface{}) []string {
	values, err := redis.Strings(r.Execute("MGET", keys...))
	if err != nil {
		return []string{}
	}
	return values
}

func (r *Redis) MSet(mapOfKeyVal map[string]interface{}) bool {
	_, err := r.Execute("MSET", redis.Args{}.AddFlat(mapOfKeyVal)...)
	if err != nil {
		return false
	}
	return true
}

func (r *Redis) Expire(key string, duration int) bool {
	_, err := r.Execute("EXPIRE", key, duration)
	if err != nil {
		return false
	}
	return true
}

func (r *Redis) Setex(key string, duration int, val interface{}) bool {
	_, err := r.Execute("SETEX", key, duration, val)
	if err != nil {
		return false
	}
	return true
}

// redis SMEMBERS implementation
func (r *Redis) Smembers(key string) (set.Set, error) {
	s := set.NewSet()
	val, err := redis.Values(r.Execute("SMEMBERS", key))
	if err != nil {
		return s, err
	}
	for _, item := range(val){
		//Redis always stores set members as string; 
		//TODO: Check if we need to handle byte converted  with type switching.
		it := string(item.([]byte))
		s.Add(it)
	}
	return s, nil
}

// redis SADD implementation
func (r *Redis) SAdd(key string, value interface{}) (bool, error) {
	_, err := r.Execute("SADD", key, value)
	if err != nil {
		return false, err
	}
	return true, nil
}

// redis SREM implementation
func (r *Redis) SRem(key string, value string) (bool, error) {
	_, err := r.Execute("SREM", key, value)
	if err != nil {
		return false, err
	}
	return true, nil
}
