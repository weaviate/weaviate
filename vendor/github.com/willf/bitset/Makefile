# MAKEFILE
#
# @author      Nicola Asuni <nicola@cognitivelogic.com>
# @link        https://github.com/willf/bitset
# ------------------------------------------------------------------------------

# List special make targets that are not associated with files
.PHONY: help all test format fmtcheck vet lint coverage cyclo ineffassign misspell astscan qa deps clean nuke

# Use bash as shell (Note: Ubuntu now uses dash which doesn't support PIPESTATUS).
SHELL=/bin/bash

# Project owner
OWNER=willf

# Project name
PROJECT=bitset

# Name of RPM or DEB package
PKGNAME=${OWNER}-${PROJECT}

# Go lang path. Set if necessary
# GOPATH=$(shell readlink -f $(shell pwd)/../../../../)

# Current directory
CURRENTDIR=$(shell pwd)

# --- MAKE TARGETS ---
all: qa

# Alias for qa target
all: test

# Display general help about this command
help:
	@echo ""
	@echo "$(PROJECT) Makefile."
	@echo "The following commands are available:"
	@echo ""
	@echo "    make qa          : Run all the tests"
	@echo "    make test        : Run the unit tests"
	@echo ""
	@echo "    make format      : Format the source code"
	@echo "    make fmtcheck    : Check if the source code has been formatted"
	@echo "    make vet         : Check for suspicious constructs"
	@echo "    make lint        : Check for style errors"
	@echo "    make coverage    : Generate the coverage report"
	@echo "    make cyclo       : Generate the cyclomatic complexity report"
	@echo "    make ineffassign : Detect ineffectual assignments"
	@echo "    make misspell    : Detect commonly misspelled words in source files"
	@echo "    make astscan     : GO AST scanner"
	@echo ""
	@echo "    make docs        : Generate source code documentation"
	@echo ""
	@echo "    make deps        : Get the dependencies"
	@echo "    make clean       : Remove any build artifact"
	@echo "    make nuke        : Deletes any intermediate file"
	@echo ""


# Run the unit tests
test:
	@mkdir -p target/test
	@mkdir -p target/report
	GOPATH=$(GOPATH) go test -covermode=count -coverprofile=target/report/coverage.out -bench=. -race -v ./... | \
	tee >(PATH=$(GOPATH)/bin:$(PATH) go-junit-report > target/test/report.xml); \
	test $${PIPESTATUS[0]} -eq 0

# Format the source code
format:
	@find ./ -type f -name "*.go" -exec gofmt -w {} \;

# Check if the source code has been formatted
fmtcheck:
	@mkdir -p target
	@find ./ -type f -name "*.go" -exec gofmt -d {} \; | tee target/format.diff
	@test ! -s target/format.diff || { echo "ERROR: the source code has not been formatted - please use 'make format' or 'gofmt'"; exit 1; }

# Check for syntax errors
vet:
	GOPATH=$(GOPATH) go vet ./...

# Check for style errors
lint:
	GOPATH=$(GOPATH) PATH=$(GOPATH)/bin:$(PATH) golint ./...

# Generate the coverage report
coverage:
	GOPATH=$(GOPATH) go tool cover -html=target/report/coverage.out -o target/report/coverage.html

# Report cyclomatic complexity
cyclo:
	@mkdir -p target/report
	GOPATH=$(GOPATH) gocyclo -avg ./ | tee target/report/cyclo.txt ; test $${PIPESTATUS[0]} -eq 0

# Detect ineffectual assignments
ineffassign:
	@mkdir -p target/report
	GOPATH=$(GOPATH) ineffassign ./ | tee target/report/ineffassign.txt ; test $${PIPESTATUS[0]} -eq 0

# Detect commonly misspelled words in source files
misspell:
	@mkdir -p target/report
	GOPATH=$(GOPATH) misspell -error ./  | tee target/report/misspell.txt ; test $${PIPESTATUS[0]} -eq 0

# AST scanner
astscan:
	@mkdir -p target/report
	GOPATH=$(GOPATH) gas --nosec=true ./... | tee target/report/astscan.txt ; test $${PIPESTATUS[0]} -eq 0

# Generate source docs
docs:
	@mkdir -p target/docs
	nohup sh -c 'GOPATH=$(GOPATH) godoc -http=127.0.0.1:6060' > target/godoc_server.log 2>&1 &
	wget --directory-prefix=target/docs/ --execute robots=off --retry-connrefused --recursive --no-parent --adjust-extension --page-requisites --convert-links http://127.0.0.1:6060/pkg/github.com/${OWNER}/${PROJECT}/ ; kill -9 `lsof -ti :6060`
	@echo '<html><head><meta http-equiv="refresh" content="0;./127.0.0.1:6060/pkg/github.com/'${OWNER}'/'${PROJECT}'/index.html"/></head><a href="./127.0.0.1:6060/pkg/github.com/'${OWNER}'/'${PROJECT}'/index.html">'${PKGNAME}' Documentation ...</a></html>' > target/docs/index.html

# Alias to run all quality-assurance checks
qa: deps fmtcheck test vet lint coverage cyclo ineffassign misspell astscan

# --- INSTALL ---

# Get the dependencies
deps:
	GOPATH=$(GOPATH) go get ./...
	GOPATH=$(GOPATH) go get github.com/golang/lint/golint
	GOPATH=$(GOPATH) go get github.com/jstemmer/go-junit-report
	GOPATH=$(GOPATH) go get github.com/axw/gocov/gocov
	GOPATH=$(GOPATH) go get github.com/fzipp/gocyclo
	GOPATH=$(GOPATH) go get github.com/gordonklaus/ineffassign
	GOPATH=$(GOPATH) go get github.com/client9/misspell/cmd/misspell
	GOPATH=$(GOPATH) go get github.com/HewlettPackard/gas

# Remove any build artifact
clean:
	GOPATH=$(GOPATH) go clean ./...

# Deletes any intermediate file
nuke:
	rm -rf ./target
	GOPATH=$(GOPATH) go clean -i ./...
