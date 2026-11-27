module github.com/metacubex/sing-mux

go 1.26.0

require (
	github.com/metacubex/sing v0.5.9
	github.com/metacubex/smux v0.0.0-20250503055512-501391591dee
	github.com/metacubex/yamux v0.0.0-20250918083631-dd5f17c0be49
	golang.org/x/net v0.62.0
	golang.org/x/sys v0.50.0
)

require golang.org/x/text v0.44.0 // indirect

replace github.com/metacubex/sing => github.com/zhfreal/sing v0.5.10-0.20261127064707-9ba267e5c2a1

replace github.com/metacubex/smux => github.com/zhfreal/smux v1.5.51-0.20261127070556-4b295ae8f4df

replace github.com/metacubex/yamux => github.com/zhfreal/yamux v0.0.0-20261127064844-a1bb256cc823
