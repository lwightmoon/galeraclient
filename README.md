# go client for galera cluster

# usage
```
builder := &galeraclient.ClientBuilder{}
	host := "ip:port"
	db := "db"
	user := ""
	pwd := ""
	maxIdleConn := 2
	maxConn := 5
	connTimeout := 1

	builder = builder.Nodes(host).
		DiscoverPeriod(time.Second).Database(db).
		User(user).Pwd(pwd).MaxConnIdlePerHost(maxIdleConn).
		MaxConnPerHost(maxConn).DiscoverPeriod(1 * time.Second).
		ConnTimeout(connTimeout).ShowSQL(true).ElectionPolicy(nil).IgnoreDonor(false).ConsistencyLevel("0")
	client := builder.TestMode(true).Build()
	session := client.GetSession()
	defer session.Close()
	// s.Insert()
	// s.Update()
	// s.Query()

```
