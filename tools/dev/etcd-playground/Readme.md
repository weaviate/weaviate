# etcd lock example

> Uses the `etcd` golang client to aquire a (distributed) lock.

The locked process will wait to aquire the lock, then sleep for 10s (pretending
to do something that cannot happen concurrently).

Etcd needs to be running before you start, easiest way is to simply do a
`docker-compose up -d etcd`.

This is best demoed if you open multiple terminals (or panes in `tmux`) side by
side, then start this example with `go run ./locking_example.go` in each of the
panes. You will then see that the sleeping period is locked and sequentially
run across all processes.
