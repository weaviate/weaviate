package main

import (
	"flag"
	"fmt"
	"log"
	"os"
)

var flagset = flag.NewFlagSet("crypt", flag.ExitOnError)

var (
	data          string
	backendName   string
	key           string
	keyring       string
	endpoint      string
	secretKeyring string
	plaintext     bool
	machines      []string
)

func init() {
	flagset.StringVar(&backendName, "backend", "etcd", "backend provider")
	flagset.StringVar(&endpoint, "endpoint", "", "backend url")
	flagset.BoolVar(&plaintext, "plaintext", false, "skip encryption")
}

func main() {
	log.SetFlags(0)
	if len(os.Args) < 2 {
		help()
	}
	cmd := os.Args[1]
	switch cmd {
	case "set":
		setCmd(flagset)
	case "get":
		getCmd(flagset)
	case "list":
		listCmd(flagset)
	default:
		help()
	}
}

func help() {
	fmt.Fprintf(os.Stderr, "usage: %s COMMAND [arg...]", os.Args[0])
	fmt.Fprintf(os.Stderr, "\n\n")
	fmt.Fprintf(os.Stderr, "commands:\n")
	fmt.Fprintf(os.Stderr, "   get   retrieve the value of a key\n")
	fmt.Fprintf(os.Stderr, "   list  retrieve all values under a key\n")
	fmt.Fprintf(os.Stderr, "   set   set the value of a key\n")
	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "-plaintext  don't encrypt or decrypt the values before storage or retrieval\n")

	os.Exit(1)
}
