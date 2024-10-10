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
.PHONY: install-gofumpt format
format: install-gofumpt
	$(GOFUMPT) -w .

# Target to run the swagger code generation script
.PHONY: generate-code
generate-code:
	@echo "Running Swagger code generation script..."
	@sh ./tools/gen-code-from-swagger.sh
