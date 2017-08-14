# bitset

*Go language library to map between non-negative integers and boolean values*

[![Master Build Status](https://secure.travis-ci.org/willf/bitset.png?branch=master)](https://travis-ci.org/willf/bitset?branch=master)
[![Master Coverage Status](https://coveralls.io/repos/willf/bitset/badge.svg?branch=master&service=github)](https://coveralls.io/github/willf/bitset?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/willf/bitset)](https://goreportcard.com/report/github.com/willf/bitset)


## Description

Package bitset implements bitsets, a mapping between non-negative integers and boolean values.
It should be more efficient than map[uint] bool.

It provides methods for setting, clearing, flipping, and testing individual integers.

But it also provides set intersection, union, difference, complement, and symmetric operations, as well as tests to check whether any, all, or no bits are set, and querying a bitset's current length and number of positive bits.

BitSets are expanded to the size of the largest set bit; the memory allocation is approximately Max bits, where Max is the largest set bit. BitSets are never shrunk. On creation, a hint can be given for the number of bits that will be used.

Many of the methods, including Set, Clear, and Flip, return a BitSet pointer, which allows for chaining.

### Example use:

    import "bitset"
    var b BitSet
    b.Set(10).Set(11)
    if b.Test(1000) {
        b.Clear(1000)
    }
    for i,e := v.NextSet(0); e; i,e = v.NextSet(i + 1) {
       fmt.Println("The following bit is set:",i);
    }
    if B.Intersection(bitset.New(100).Set(10)).Count() > 1 {
        fmt.Println("Intersection works.")
    }

As an alternative to BitSets, one should check out the 'big' package, which provides a (less set-theoretical) view of bitsets.

Discussions golang-nuts Google Group:

* [Revised BitSet](https://groups.google.com/forum/#!topic/golang-nuts/5i3l0CXDiBg)
* [simple bitset?](https://groups.google.com/d/topic/golang-nuts/7n1VkRTlBf4/discussion)

Godoc documentation is at: https://godoc.org/github.com/willf/bitset

## Installation

```bash
go get github.com/willf/bitset
```

## Contributing

If you wish to contribute to this project, please branch and issue a pull request against master ("[GitHub Flow](https://guides.github.com/introduction/flow/)")

This project include a Makefile that allows you to test and build the project with simple commands.
To see all available options:
```bash
make help
```

## Running all tests

Before committing the code, please check if it passes all tests using (note: this will install some dependencies):
```bash
make qa
```
