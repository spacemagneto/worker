# This Makefile defines common tasks for a Go project, including linting the code,
# running tests, and generating mocks using mockery.

# Target: lint
# Description: Run the Go linter using golangci-lint.
# This checks the code for stylistic issues, potential bugs, and other improvements.
lint: ## Run linter
	golangci-lint run

# Target: test
# Description: Run all tests in the project.
# The -v flag makes the test output verbose, showing detailed information about each test.
test: ## Run tests
	go test -v ./
