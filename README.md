Mantle
======

Go wrapper for nosql dbs.

####Get the package:
        go get github.com/goibibo/mantle
        
####Code:
        package main

        import (
                "fmt"
                "github.com/goibibo/mantle"
                "time"
        )

        func main(){
                //sample data
                keyValue := map[string]interface{}{"key1":"val1", "key2":"val2", "key3":"val3"}
                //extra params to be passed to connection
                options := map[string]string{"db":"1"}
                hostNPort := []string{"localhost:6379"}

                orm := mantle.Orm{Driver: "redis", HostAndPorts: hostNPort}

                //selecting a particular db
                orm := mantle.Orm{Driver: "redis", HostAndPorts: hostNPort, Options: options}

                //default "localhost:6379 is used when hostAndPort is not passed"
                //orm := mantle.Orm{Driver: "redis"}

                //this connects to redis at localhost:6379 by default
                //orm := mantle.Orm{}

                connection := orm.New()

                fmt.Println(connection.Set("key", "value2")) //output: true
                fmt.Println(connection.Get("key"))           //value2
                fmt.Println(connection.Delete("key"))        //1
                fmt.Println(connection.Get("key"))           //""

                fmt.Println(connection.MSet(keyValue))       //true
                fmt.Println(connection.MGet("key3", "key2")) //[val3 val2]

                connection.Expire("key", 1)
                time.Sleep(1 * time.Second)
                fmt.Println(connection.Get("key"))           //""

                /*Execute any redis command*/
                connection.Execute("LPUSH", "test", "a")
                connection.Execute("LPUSH", "test", "b")
                connection.Execute("LPUSH", "test", "c")
                values, _ := connection.Execute("LRANGE", "test", 0, -1)
                fmt.Println(values)                          //[[99] [98] [97]]

                connection.Setex("key", 1, "value")
                fmt.Println(connection.Get("key"))           //value
                time.Sleep(1 * time.Second)
                fmt.Println(connection.Get("key"))           //""

		//For MySQL
		//DB query
		query := "select * from flight_controllerdata"

		connections := []string{"root:@tcp(127.0.0.1:3306)/bm"}
		//Create mantle Driver with settings
		orm := mantle.Orm{Driver: "mysql", HostAndPorts: connections}

		// Create a new connection
		conn := orm.NewMySQL()

		//Get query response and print
		response, _ := conn.Select(query)
		fmt.Println(response)


        }
