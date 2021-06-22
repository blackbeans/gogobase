package goh

import (
	"container/list"
	"context"
	"errors"
	"github.com/blackbeans/log4go"
	"net"
	"sync"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
)

type Dial func(addr string) (*IdleClient, error)
type ClientClose func(c *IdleClient) error

type ThriftPool struct {
	ctx        context.Context
	Dial       Dial
	Close      ClientClose
	checkAlive func(cli *HClient) bool

	lock          *sync.RWMutex
	idle          list.List
	idleTimeout   time.Duration
	checkInterval time.Duration

	maxConn int
	count   int
	addr    string
	closed  bool
}

type IdleClient struct {
	Socket     thrift.TTransport
	Client     *HClient
	createtime time.Time
}

type idleConn struct {
	c *IdleClient
	t time.Time
}

//是否有效的
func (c *idleConn) Check(duration time.Duration) bool {
	if nowFunc().After(c.t.Add(duration)) || !c.c.Check() {
		return false
	}
	return true
}

var nowFunc = time.Now

//error
var (
	ErrOverMax          = errors.New("Over Max Connections")
	ErrInvalidConn      = errors.New("Connection was broken")
	ErrPoolClosed       = errors.New("Pool has been closed")
	ErrSocketDisconnect = errors.New("Socket Disconnect")
)

func NewThriftPool(
	ctx context.Context,
	addr string,
	maxConn, idleTimeout int,
	checkInterval time.Duration,
	dial Dial,
	closeFunc ClientClose,
	checkAlive func(cli *HClient) bool) *ThriftPool {

	if checkInterval <= 0 {
		checkInterval = 30 * time.Second
	}

	thriftPool := &ThriftPool{
		ctx:           ctx,
		Dial:          dial,
		Close:         closeFunc,
		addr:          addr,
		lock:          &sync.RWMutex{},
		maxConn:       maxConn,
		idleTimeout:   time.Duration(idleTimeout) * time.Second,
		closed:        false,
		count:         0,
		checkInterval: checkInterval,
		checkAlive:    checkAlive,
	}

	go thriftPool.ClearConn()

	return thriftPool
}

func (p *ThriftPool) Get() (*IdleClient, error) {
	p.lock.Lock()
	if p.closed {
		p.lock.Unlock()
		return nil, ErrPoolClosed
	}

	if p.idle.Len() == 0 && p.count >= p.maxConn {
		p.lock.Unlock()
		return nil, ErrOverMax
	}

	//优先寻找空闲的链接
	for ele := p.idle.Front(); nil != ele; ele = p.idle.Front() {
		idle := p.idle.Remove(ele).(*idleConn)

		if !idle.Check(p.idleTimeout) {
			if p.count > 0 {
				p.count -= 1
			}
			//回收
			p.Close(idle.c)
			idle.c = nil
			idle = nil
		} else {
			p.lock.Unlock()
			//检查是否真正存活
			return idle.c, nil
		}
	}

	//没有找到对应的存活链接，那么久直接新建一个
	dial := p.Dial
	p.count += 1
	p.lock.Unlock()

	client, err := dial(p.addr)
	if err != nil {
		p.lock.Lock()
		if p.count > 0 {
			p.count -= 1
		}
		p.lock.Unlock()
		return nil, err
	}
	client.createtime = nowFunc()
	return client, nil

}

func (p *ThriftPool) Put(client *IdleClient) error {
	if client == nil {
		return ErrInvalidConn
	}

	if client.Client == nil {
		return nil
	}

	p.lock.Lock()
	if p.closed {
		p.lock.Unlock()

		err := p.Close(client)
		client = nil
		return err
	}

	if p.count > p.maxConn {
		if p.count > 0 {
			p.count -= 1
		}
		p.lock.Unlock()

		err := p.Close(client)
		client = nil
		return err
	}

	if !client.Check() {
		if p.count > 0 {
			p.count -= 1
		}
		p.lock.Unlock()

		err := p.Close(client)
		client = nil
		return err
	}

	p.idle.PushBack(&idleConn{
		c: client,
		t: nowFunc(),
	})
	p.lock.Unlock()

	return nil
}

func (p *ThriftPool) CloseErrConn(client *IdleClient) {
	if client == nil {
		return
	}

	p.lock.Lock()
	if p.count > 0 {
		p.count -= 1
	}
	p.lock.Unlock()

	p.Close(client)
	client = nil
	return
}

func (p *ThriftPool) CheckTimeout() {
	p.lock.Lock()

	now := nowFunc()
	removeList := list.New()
	closeConns := make([]interface{}, 0, removeList.Len())

	for ele := p.idle.Front(); nil != ele; ele = ele.Next() {
		v := ele.Value.(*idleConn)
		//已经过期或者损坏
		if !v.t.Add(p.idleTimeout).After(nowFunc()) || !p.checkAlive(v.c.Client) {
			//timeout && clear
			removeList.PushBack(ele)
		}
	}

	//删除指定的需要删除的链接
	for e := removeList.Front(); nil != e; e = removeList.Front() {
		remove := p.idle.Remove(removeList.Remove(e).(*list.Element))
		closeConns = append(closeConns, remove)
		if p.count > 0 {
			p.count -= 1
		}
	}

	//清理掉过于空闲的链接
	for ele := p.idle.Front(); nil != ele; ele = p.idle.Front() {
		//检查下空闲列表占比,最多5个空闲
		if p.count <= p.idle.Len()*4 && p.idle.Len() > 4 {
			v := p.idle.Remove(ele).(*idleConn)
			closeConns = append(closeConns, v)
			if p.count > 0 {
				p.count -= 1
			}
		} else {
			break
		}
	}

	p.lock.Unlock()

	log4go.InfoLog("stdout", "ThriftPool|CleanClients|%dms|%d...", time.Now().Sub(now).Milliseconds(), len(closeConns))
	//逐个关闭
	for _, conn := range closeConns {
		//关闭链接
		p.Close(conn.(*idleConn).c) //close send connection
	}
	closeConns = nil
	return
}

func (c *IdleClient) SetConnTimeout(connTimeout uint32) {
	if tsocket, ok := c.Client.Trans.(*thrift.TSocket); ok {
		tsocket.SetTimeout(time.Duration(connTimeout) * time.Second)
	}
}

func (c *IdleClient) LocalAddr() net.Addr {
	if tsocket, ok := c.Client.Trans.(*thrift.TSocket); ok {
		return tsocket.Conn().LocalAddr()
	}
	return nil
}

func (c *IdleClient) RemoteAddr() net.Addr {
	if tsocket, ok := c.Client.Trans.(*thrift.TSocket); ok {
		return tsocket.Conn().RemoteAddr()
	}
	return nil
}

func (c *IdleClient) Check() bool {
	if c.Socket == nil || c.Client == nil || !c.Client.IsAlive() {
		return false
	}
	return c.Socket.IsOpen()
}

func (p *ThriftPool) GetIdleCount() uint32 {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return uint32(p.idle.Len())
}

func (p *ThriftPool) GetConnCount() int {
	return p.count
}

func (p *ThriftPool) ClearConn() {
	for {
		select {
		case <-p.ctx.Done():
			return
		default:

		}

		p.CheckTimeout()
		time.Sleep(p.checkInterval)
	}
}

func (p *ThriftPool) Destroy() {
	p.lock.Lock()
	idle := p.idle
	p.idle.Init()
	p.closed = true
	p.count = 0
	p.lock.Unlock()

	for iter := idle.Front(); iter != nil; iter = iter.Next() {
		p.Close(iter.Value.(*idleConn).c)
	}
}
