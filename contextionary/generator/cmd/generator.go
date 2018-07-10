package main

import (
	flags "github.com/jessevdk/go-flags"
  "github.com/creativesoftwarefdn/weaviate/contextionary/generator"
  "os"
)

func main() {
	var options generator.Options
	var parser = flags.NewParser(&options, flags.Default)

	if _, err := parser.Parse(); err != nil {
		if flagsErr, ok := err.(*flags.Error); ok && flagsErr.Type == flags.ErrHelp {
			os.Exit(0)
		} else {
			os.Exit(1)
		}
	}

  generator.Generate(options)
}
