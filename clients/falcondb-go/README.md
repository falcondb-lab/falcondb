# FalconDB Go SDK

Native protocol driver for [FalconDB](https://github.com/falcondb/falcondb) with
HA failover and connection pooling.

## Installation

```bash
go get github.com/falcondb/falcondb-go
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"

    falcon "github.com/falcondb/falcondb-go"
)

func main() {
    ctx := context.Background()

    conn, err := falcon.Connect(ctx, falcon.Config{
        Host:     "localhost",
        Port:     6543,
        Database: "falcon",
        User:     "falcon",
        Password: "secret",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    rows, err := conn.Query(ctx, "SELECT id, name FROM users WHERE id = 42")
    if err != nil {
        log.Fatal(err)
    }
    for rows.Next() {
        v := rows.Values()
        fmt.Printf("id=%v name=%v\n", v[0], v[1])
    }
}
```

## Connection Pool

```go
pool, err := falcon.NewPool(ctx, falcon.DefaultPoolConfig(falcon.Config{
    Host:     "localhost",
    Port:     6543,
    Database: "mydb",
    User:     "falcon",
    Password: "secret",
}))
if err != nil {
    log.Fatal(err)
}
defer pool.Close()

conn, err := pool.Acquire(ctx)
if err != nil {
    log.Fatal(err)
}
defer pool.Release(conn)

rows, _ := conn.Query(ctx, "SELECT 1")
```

## HA Failover

```go
fc, err := falcon.ConnectFailover(ctx, falcon.FailoverConfig{
    Seeds: []falcon.SeedNode{
        {Host: "node1", Port: 6543},
        {Host: "node2", Port: 6543},
        {Host: "node3", Port: 6543},
    },
    Database:   "mydb",
    User:       "falcon",
    Password:   "secret",
    MaxRetries: 3,
})
if err != nil {
    log.Fatal(err)
}
defer fc.Close()

// Automatic failover on NOT_LEADER / FENCED_EPOCH
rows, err := fc.Query(ctx, "SELECT * FROM orders")
```

## Error Handling

```go
rows, err := conn.Query(ctx, "SELET 1") // typo
if err != nil {
    var fe *falcon.FalconError
    if errors.As(err, &fe) {
        fmt.Println("code:", fe.Code)       // 1000
        fmt.Println("sqlstate:", fe.SQLState) // 42601
        fmt.Println("retryable:", fe.Retryable)
    }
}
```

## Error Codes

| Code | Name | Retryable | Failover |
|------|------|-----------|----------|
| 1000 | SYNTAX_ERROR | ❌ | ❌ |
| 1001 | INVALID_PARAM | ❌ | ❌ |
| 2000 | NOT_LEADER | ✅ | ✅ |
| 2001 | FENCED_EPOCH | ✅ | ✅ |
| 2002 | READ_ONLY | ✅ | ✅ |
| 2003 | SERIALIZATION_CONFLICT | ✅ | ❌ |
| 3000 | INTERNAL_ERROR | ❌ | ❌ |
| 3001 | TIMEOUT | ✅ | ❌ |
| 3002 | OVERLOADED | ✅ | ❌ |
| 4000 | AUTH_FAILED | ❌ | ❌ |
| 4001 | PERMISSION_DENIED | ❌ | ❌ |

## Requirements

- Go 1.21+
- No external dependencies (stdlib only)

## Running Tests

```bash
cd clients/falcondb-go
go test -v ./...
```

## License

Apache-2.0
