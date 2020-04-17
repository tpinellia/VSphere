package g

import (
	"bufio"
	"io"
	"math/rand"
	"net"
	"net/rpc"
	"reflect"
	"time"

	"github.com/didi/nightingale/src/dataobj"

	"github.com/ugorji/go/codec"
)

func N9ePush(items []*MetricValue) {
	var mh codec.MsgpackHandle
	mh.MapType = reflect.TypeOf(map[string]interface{}(nil))

	addrs := Config().Transfer.Addr
	count := len(addrs)
	retry := 0
	for {
		for _, i := range rand.Perm(count) {
			addr := addrs[i]
			conn, err := net.DialTimeout("tcp", addr, time.Millisecond*3000)
			if err != nil {
				Log.Errorln("[push.go] dial transfer err:", err)
				continue
			}

			var bufconn = struct { // bufconn here is a buffered io.ReadWriteCloser
				io.Closer
				*bufio.Reader
				*bufio.Writer
			}{conn, bufio.NewReader(conn), bufio.NewWriter(conn)}

			rpcCodec := codec.MsgpackSpecRpc.ClientCodec(bufconn, &mh)
			client := rpc.NewClientWithCodec(rpcCodec)

			var reply dataobj.TransferResp
			err = client.Call("Transfer.Push", items, &reply)
			client.Close()
			if err != nil {
				Log.Errorln(err)
				continue
			} else {
				if reply.Msg != "ok" {
					Log.Errorln("[push.go] some item push err", reply)
				}
				return
			}
		}
		time.Sleep(time.Millisecond * 500)

		retry += 1
		if retry == 3 {
			retry = 0
			break
		}
	}
}
