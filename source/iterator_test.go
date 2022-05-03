package source

import (
	"context"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"
	redis "github.com/gomodule/redigo/redis"
	"github.com/rafaeljusto/redigomock"
	"github.com/stretchr/testify/assert"
)

func TestHasNext(t *testing.T) {
	var cdc CDCIterator
	data := []byte("ABCD")
	data2 := sdk.Record{
		Payload: sdk.RawData(data),
	}
	cdc.records = append(cdc.records, data2)
	tests := []struct {
		name     string
		response bool
	}{
		{
			name:     "Has next",
			response: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := cdc.HasNext(context.Background())
			assert.Equal(t, res, tt.response, tt.name)
		})
	}
}
func TestStop(t *testing.T) {
	cdc := CDCIterator{
		quit: make(chan bool, 1),
	}
	test := struct {
		name string
		err  error
	}{
		name: "stop",
		err:  nil,
	}
	t.Run(test.name, func(t *testing.T) {
		err := cdc.Stop()
		if test.err != nil {
			assert.NotNil(t, test.err, err)
		} else {
			assert.Nil(t, err)
		}
	})
}
func TestNext(t *testing.T) {
	var cdc CDCIterator
	data := []byte("ABCD")
	data2 := sdk.Record{
		Position: nil,
		Metadata: nil,
		Key:      nil,
		Payload:  sdk.RawData(data),
	}
	cdc.records = append(cdc.records, data2)
	cdc.records = append(cdc.records, data2)
	tests := []struct {
		name     string
		response sdk.Record
		err      error
	}{
		{
			name:     "test",
			response: data2,
			err:      nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := cdc.Next(context.Background())
			if tt.err != nil {
				assert.NotNil(t, err)
			} else {
				assert.NotNil(t, res)
				assert.Equal(t, res, tt.response)
			}
		})
	}
}
func TestNewCDCIterator(t *testing.T) {
	redisChannel := "subchannel"
	conn := redigomock.NewConn()
	response := CDCIterator{
		channel: redisChannel,
		psc:     redis.PubSubConn{Conn: (conn)},
		records: []sdk.Record(nil),
		quit:    make(chan bool, 1),
	}

	conn.Command("SUBSCRIBE", redisChannel).Expect([]interface{}{
		[]byte("subscribe"),
		[]byte(redisChannel),
		[]byte("1"),
	})
	messages := [][]byte{
		[]byte("value1"),
		[]byte("value2"),
		[]byte("value3"),
		[]byte("finished"),
	}
	for _, message := range messages {
		conn.AddSubscriptionMessage([]interface{}{
			[]byte("message"),
			[]byte(redisChannel),
			message,
		})
	}
	tests := []struct {
		name     string
		response *CDCIterator
		err      error
	}{
		{
			name:     "Success",
			response: &response,
			err:      nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := NewCDCIterator(context.Background(), conn, redisChannel)
			if tt.err != nil {
				assert.NotNil(t, err)
			} else {
				assert.NotNil(t, res)
			}
		})
	}
}
