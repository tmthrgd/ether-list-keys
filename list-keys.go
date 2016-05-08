package main

import (
	"flag"
	"fmt"

	msgpack "github.com/hashicorp/go-msgpack/codec"
	serf "github.com/hashicorp/serf/client"
)

const listKeysQuery = "list-keys"

func main() {
	conf := &serf.Config{}

	flag.StringVar(&conf.Addr, "addr", "127.0.0.1:7373", "the address to connect to")
	flag.StringVar(&conf.AuthKey, "auth", "", "the RPC auth key")
	flag.DurationVar(&conf.Timeout, "timeout", 0, "the RPC timeout")

	var eventKeyPrefix string
	flag.StringVar(&eventKeyPrefix, "prefix", "ether:", "the serf event prefix")

	flag.Parse()

	rpc, err := serf.ClientFromConfig(conf)
	if err != nil {
		panic(err)
	}

	ackCh := make(chan string)
	respCh := make(chan serf.NodeResponse)

	if err = rpc.Query(&serf.QueryParam{
		RequestAck: true,
		Name:       eventKeyPrefix + listKeysQuery,
		AckCh:      ackCh,
		RespCh:     respCh,
	}); err != nil {
		panic(err)
	}

	fmt.Printf("Query '%s%s' dispatched\n", eventKeyPrefix, listKeysQuery)

	var resps int
	var acks int

	go func() {
		var mh msgpack.MsgpackHandle

		for resp := range respCh {
			resps++

			dec := msgpack.NewDecoderBytes(resp.Payload, &mh)

			var body struct {
				Default []byte
				Keys    [][]byte
			}

			if err := dec.Decode(&body); err != nil {
				panic(err)
			}

			fmt.Printf(`Response from '%s':
	Default: %x
	Keys: %x
	Total Keys: %d
`, resp.From, body.Default, body.Keys, len(body.Keys))
		}
	}()

	for ack := range ackCh {
		acks++

		fmt.Printf("Ack from '%s'\n", ack)
	}

	fmt.Printf("Total Acks: %d\n", acks)
	fmt.Printf("Total Responses: %d\n", resps)
}
