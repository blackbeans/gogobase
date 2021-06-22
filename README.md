Overview
===

gogobase is a golang client of hbase (via thrift)  supports thrift's client pool . 


Install
===

	go get github.com/blackbeans/gogobase

Usage
===

```go

    ctx,cancel := context.WithCancel(context.Background())
    
	defer cancel()
	registryUri := "thrift://xxxxx:9097"
	maxIdleSeconds := 30
	maxPoolSize := 20
    
	//create hbase thriftclient pool
	hbasePool := goh.NewThriftPool(
		ctx,
		registryUri,
		maxPoolSize,
		maxIdleSeconds,
		5*time.Second,
		func(addr string) (*goh.IdleClient, error) {

			hclient, err := goh.NewTcpClient(addr, goh.TBinaryProtocol, false)
			if nil != err {
				log4go.ErrorLog("stdout", "NewTcpClient|FAIL|%v|%s", err, addr)
				panic(err)
			}

			if err = hclient.Open(); nil != err {
				log4go.ErrorLog("stdout", "NewTcpClient.Open|FAIL|%v|%s", err, addr)
				return nil, err
			}

			return &goh.IdleClient{
				Socket: hclient.Trans,
				Client: hclient}, nil
		}, func(c *goh.IdleClient) error {
			
			//do something when the client is going to be closed!
			
			if nil != c.Client {
				c.Client.Close()
				c.Client = nil
			}
			c.Socket = nil
			return nil
		}, func(cli *goh.HClient) bool {
			
			//checking hclient is still alive~  
			
			_, err := cli.Get("table_name", []byte("check"), "cf:status", nil)
			if nil != err {
				return false
			}
			return true
		})

	idleClient,err := hbasePool.Get()
	if nil != err{
		return
	}
    //release the idleClient into the pool
	defer hbasePool.Put(idleClient)

	idleClient.Client.Get

	idleClient.Client.MutateRows

	idleClient.Client.ScannerOpenWithScan

	....

```
	
	




Links
===


* https://github.com/sdming/goh



License
===

Apache License v2.0  