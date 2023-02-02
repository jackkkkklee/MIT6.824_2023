# MIT6.824_2023
labs of mit6.824

## lab1 start guide: MapReduce
+ build world count plugin
  `
  go build -race -buildmode=plugin ../mrapps/wc.go
  `
+ run coordinator and clean last-tested file
`rm mr-out* `
`go run -race mrcoordinator.go pg-*.txt`
+ run workers
` go run -race mrworker.go wc.so`
+ run test case
`bash test-mr.sh`