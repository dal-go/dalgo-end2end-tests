module github.com/dal-go/dalgo-end2end-tests

go 1.21

toolchain go1.22.4

require (
	github.com/dal-go/dalgo v0.12.1
	github.com/stretchr/testify v1.9.0
	github.com/strongo/validation v0.0.6
)

//replace github.com/dal-go/dalgo => ../dalgo

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/strongo/random v0.0.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
