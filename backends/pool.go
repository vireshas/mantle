package mantle

import (
	"github.com/goibibo/minimal_vitess_pool"
	"time"
)

//This struct has all the generic settings that are required for creating a pool
type PoolSettings struct {
	HostAndPorts []string
	Capacity     int
	MaxCapacity  int
	Options      map[string]string
	Timeout      time.Duration
}

//Gets a connection from pool
func (rp *ResourcePool) GetConn(to time.Duration) (pools.Resource, error) {
	resource, err := rp.pool.Get(to)
	if err != nil {
		return nil, err
	}
	return resource, nil
}

//Puts a connection back in pool
func (rp *ResourcePool) PutConn(conn pools.Resource) {
	rp.pool.Put(conn)
}

//Wraps pool
type ResourcePool struct {
	pool *pools.ResourcePool
}

//The connect callback that is passed by a backend
type dialAndConnect func(instance interface{}) (pools.Resource, error)

/*
 * Creats an instance of pool
   Params:
     connect: A callback which creates a connection to datastore
     instace: An instance of backend(redis instance)
     settings: configurations needed to create a pool
*/
func NewPool(connect dialAndConnect, instance interface{}, settings PoolSettings) *ResourcePool {
	return &ResourcePool{
		pools.NewResourcePool(newRedisFactory(connect, instance),
			settings.Capacity, settings.MaxCapacity, settings.Timeout)}
}

//Callback that is passed to NewPool is called here
func newRedisFactory(connect dialAndConnect, instance interface{}) pools.Factory {
	return func() (pools.Resource, error) {
		return connect(instance)
	}
}

func (rp *ResourcePool) StatsJSON() string {
	return rp.pool.StatsJSON()
}
