# Define the location where gofumpt will be installed
GOFUMPT=$(shell go env GOPATH)/bin/gofumpt

# Target to check and install gofumpt if it doesn't exist
install-gofumpt:
	@if [ ! -f $(GOFUMPT) ]; then \
		echo "Installing gofumpt..."; \
		go install mvdan.cc/gofumpt@latest; \
	else \
		echo "gofumpt is already installed."; \
	fi

# Target to format code using gofumpt
format: install-gofumpt
	$(GOFUMPT) -w .

# .PHONY ensures that these targets will run even if there are files named install-gofumpt or format
.PHONY: install-gofumpt format