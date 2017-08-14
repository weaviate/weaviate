# crypt/config

## Usage

### Get configuration from a backend

```
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/xordataexchange/crypt/config"
)

var (
	key           = "/app/config"
	secretKeyring = ".secring.gpg"
)

func main() {
	kr, err := os.Open(secretKeyring)
	if err != nil {
		log.Fatal(err)
	}
	defer kr.Close()
	machines := []string{"http://127.0.0.1:4001"}
	cm, err := config.NewEtcdConfigManager(machines, kr)
	if err != nil {
        log.Fatal(err)
    }
	value, err := cm.Get(key)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s\n", value)
}
```

### Monitor backend for configuration changes

```
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/xordataexchange/crypt/config"
)

var (
	key           = "/app/config"
	secretKeyring = ".secring.gpg"
)

func main() {
	kr, err := os.Open(secretKeyring)
	if err != nil {
		log.Fatal(err)
	}
	defer kr.Close()
	machines := []string{"http://127.0.0.1:4001"}
	cm, err := config.NewEtcdConfigManager(machines, kr)
	if err != nil {
		log.Fatal(err)
	}
	stop := make(chan bool, 0)
	resp := cm.Watch(key, stop)
	if err != nil {
		log.Fatal(err)
	}
	for {
		select {
		case r := <-resp:
			if r.Error != nil {
				fmt.Println(r.Error.Error())
			}
			fmt.Printf("%s\n", r.Value)
		}
	}
}
```
