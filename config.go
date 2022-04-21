package main

// TODO: use this config

type CONF struct {
	Debug bool

	Redis struct {
		IP   string
		Port int
		Key  string
	}

	// Fake struct {
	// 	Headers   map[string]string
	// }
}
