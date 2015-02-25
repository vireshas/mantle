package mantle

import (
	"github.com/go-sql-driver/mysql"
	"github.com/vireshas/minimal_vitess_pool/pools"
	"time"
	"strings"
	"strconv"
)

//cant make these guys const as []string is not allowed in consts

//default pool size
var MySQLPoolSize = 100

//This method creates a MySQL connection
//CreateMySQLConnection is passed as a callback to pools
//Instance:
//  This is a reference to a struct MySQL instance
//  Connect needs some params like db, hostAndPorts
//  These params are read from this instance rederence
func CreateMySQLConnection(Instance interface{}) (pools.Resource, error) {
	
	//converting interface MySQL struct type
	mySQLInstance := Instance.(*MySQL)
	
	//this is a string of type "root:mih123@tcp(127.0.0.1:3306)/test"
	hostNPorts := mySQLInstance.Settings.HostAndPorts
	
	if (hostNPorts == ""){
		panic("From MySQL: host and port not specified")
	}
	
	//select db after dialing
	db := mySQLInstance.db

	//connect to host and port
	cli, err := sql.Open("mysql", hostNPorts)
	
	if err != nil {
		panic(err)
	}

	//typecast to MySQLConn
	return &MySQLConn{cli}, nil
}

// Wrapping MySQL connection
	//sql.db returns a db connection pointer
type MySQLConn struct {
	sql.DB
}

//Close a MySQL connection
func (m *MySQLConn) Close() {
	_ = m.Close()
}

//Gets a connection from pool and converts to MySQLConn type
//If all the connections are in use, timeout the present request after a minute
func (m *MySQL) GetConn() (*MySQLConn, error) {
	connection, err := m.pool.GetConn(m.Settings.Timeout)
	if err != nil {
		return nil, err
	}
	return connection.(*MySQLConn), nil
}

//Puts MySQL connection back to pool
func (m *MySQL) PutConn(c *MySQLConn) {
	m.pool.PutConn(c)
}

type MySQL struct {
	Settings PoolSettings
	pool     *ResourcePool
	db       string
}

func (m *MySQL) Configure(settings PoolSettings) {
	m.Settings = settings
}


//Execute all the methods for a SQL query over here
	//Execute MySQL commands; Also has support for select * from table
	//Gets a client from pool, executes a cmd, puts conn back in pool
func (m *MySQL) Select(query string) (map[string]interface, error) {
	client, err := r.GetClient()
	if err != nil {
		return nil, err
	}
	defer m.PutClient(client)
	rows, err := client.Query(query)
	if err != nil {
		fmt.Println(err)
		panic("From MySQL: Error in executing select query")
	}
	columns, _ := rows.Columns()

	scanArgs := make([]interface{}, len(columns))
	values := make([]interface{}, len(columns))

	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		rows.Scan(scanArgs...)
		record := make(map[string]interface{})
		for i, col := range values {
			if col != nil {
				switch t := col.(type) {
				default:
					panic("From MySQL: Unknown Type in type switching")
				case bool:
					record[columns[i]] = col.(bool)
				case int:
					record[columns[i]] = col.(int)
				case int64:
					record[columns[i]] = col.(int64)
				case float64:
					
					record[columns[i]] = col.(float64)
				case string:
					record[columns[i]] = col.(string)
				case []byte: // -- all cases go HERE!
					record[columns[i]] = string(col.([]byte))
				case time.Time:
					record[columns[i]] = col.(string)
				}
			}
		}
		return record, nil
	}
}
