# DataLoader
[![GoDoc](https://godoc.org/gopkg.in/nicksrandall/dataloader.v3?status.svg)](https://godoc.org/github.com/nicksrandall/dataloader)
[![Build Status](https://travis-ci.org/nicksrandall/dataloader.svg?branch=master)](https://travis-ci.org/nicksrandall/dataloader)
[![codecov](https://codecov.io/gh/nicksrandall/dataloader/branch/master/graph/badge.svg)](https://codecov.io/gh/nicksrandall/dataloader)

This is an implementation of [Facebook's DataLoader](https://github.com/facebook/dataloader) in Golang.

## Status
This project is a work in progress. Feedback is encouraged.

## Install
`go get -u gopkg.in/nicksrandall/dataloader.v5`

## Usage
```go
// setup batch function
batchFn := func(ctx context.Context, keys []interface{}) []*dataloader.Result {
  var results []*dataloader.Result
  // do some aync work to get data for specified keys
  // append to this list resolved values
  return results
}

// create Loader with an in-memory cache
loader := dataloader.NewBatchedLoader(batchFn)

/**
 * Use loader
 *
 * A thunk is a function returned from a function that is a
 * closure over a value (in this case an interface value and error).
 * When called, it will block until the value is resolved.
 */
thunk := loader.Load(ctx.TODO(), "key1")
result, err := thunk()
if err != nil {
  // handle data error
}

log.Printf("value: %#v", result)
```

## Upgrade from v1 to v2
The only difference between v1 and v2 is that we added use of [context](https://golang.org/pkg/context).

```diff
- loader.Load(key string) Thunk
+ loader.Load(ctx context.Context, key string) Thunk
- loader.LoadMany(keys []string) ThunkMany
+ loader.LoadMany(ctx context.Context, keys []string) ThunkMany
```

```diff
- type BatchFunc func([]string) []*Result
+ type BatchFunc func(context.Context, []string) []*Result
```

## Upgrade from v2 to v3
```diff
// dataloader.Interface as added context.Context to methods
- loader.Prime(key string, value interface{}) Interface
+ loader.Prime(ctx context.Context, key string, value interface{}) Interface
- loader.Clear(key string) Interface
+ loader.Clear(ctx context.Context, key string) Interface
```

```diff
// cache interface as added context.Context to methods
type Cache interface {
-	Get(string) (Thunk, bool)
+	Get(context.Context, string) (Thunk, bool)
-	Set(string, Thunk)
+	Set(context.Context, string, Thunk)
-	Delete(string) bool
+	Delete(context.Context, string) bool
	Clear()
}
```

## Upgrade from v3 to v4
```diff
// dataloader.Interface as now allows interace{} as key rather than string
- loader.Load(context.Context, key string) Thunk
+ loader.Load(ctx context.Context, key interface{}) Thunk
- loader.LoadMany(context.Context, key []string) ThunkMany
+ loader.LoadMany(ctx context.Context, keys []interface{}) ThunkMany
- loader.Prime(context.Context, key string, value interface{}) Interface
+ loader.Prime(ctx context.Context, key interface{}, value interface{}) Interface
- loader.Clear(context.Context, key string) Interface
+ loader.Clear(ctx context.Context, key interface{}) Interface
```

```diff
// cache interface now allows interface{} as key instead of string
type Cache interface {
-	Get(context.Context, string) (Thunk, bool)
+	Get(context.Context, interface{}) (Thunk, bool)
-	Set(context.Context, string, Thunk)
+	Set(context.Context, interface{}, Thunk)
-	Delete(context.Context, string) bool
+	Delete(context.Context, interface{}) bool
	Clear()
}
```

## Upgrade from v4 to v5
```diff
// dataloader.Interface as now allows interace{} as key rather than string
- loader.Load(context.Context, key interface{}) Thunk
+ loader.Load(ctx context.Context, key Key) Thunk
- loader.LoadMany(context.Context, key []interface{}) ThunkMany
+ loader.LoadMany(ctx context.Context, keys Keys) ThunkMany
- loader.Prime(context.Context, key interface{}, value interface{}) Interface
+ loader.Prime(ctx context.Context, key Key, value interface{}) Interface
- loader.Clear(context.Context, key interface{}) Interface
+ loader.Clear(ctx context.Context, key Key) Interface
```

```diff
// cache interface now allows interface{} as key instead of string
type Cache interface {
-	Get(context.Context, interface{}) (Thunk, bool)
+	Get(context.Context, Key) (Thunk, bool)
-	Set(context.Context, interface{}, Thunk)
+	Set(context.Context, Key, Thunk)
-	Delete(context.Context, interface{}) bool
+	Delete(context.Context, Key) bool
	Clear()
}
```

### Don't need/want to use context?
You're welcome to install the v1 version of this library.

## Cache
This implementation contains a very basic cache that is intended only to be used for short lived DataLoaders (i.e. DataLoaders that ony exsist for the life of an http request). You may use your own implementation if you want.

> it also has a `NoCache` type that implements the cache interface but all methods are noop. If you do not wish to cache anyting.

## Examples
There are a few basic examples in the example folder.
