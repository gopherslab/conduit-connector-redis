package main

import (
	redis "github.com/conduitio/conduit-connector-redis"
	source "github.com/conduitio/conduit-connector-redis/source"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func main() {
	sdk.Serve(redis.Specification, source.NewSource, nil)
}
