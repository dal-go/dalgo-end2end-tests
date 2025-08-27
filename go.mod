module github.com/dal-go/dalgo-end2end-tests

go 1.23.0

toolchain go1.25.0

require (
	github.com/dal-go/dalgo v0.26.4
	github.com/dal-go/mocks4dalgo v0.3.8
	github.com/stretchr/testify v1.11.1
	github.com/strongo/validation v0.0.7
	go.uber.org/mock v0.6.0
)

//replace github.com/dal-go/dalgo => ../dalgo

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/strongo/random v0.0.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
