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

replace github.com/metacubex/sing => github.com/zhfreal/sing v0.5.10-0.20261117034331-d7da6af0cb5d

replace github.com/metacubex/smux => github.com/zhfreal/smux v1.5.51-0.20261117034334-cfeeefd324e7

replace github.com/metacubex/yamux => github.com/zhfreal/yamux v0.0.0-20261117034336-61c33d890712
