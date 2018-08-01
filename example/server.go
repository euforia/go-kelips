package main

import (
	"net/http"

	kelips "github.com/euforia/go-kelips"
)

type httpServer struct {
	kelips *kelips.Kelips
}

func (server *httpServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		server.handleLookup(w, r)
	case http.MethodPost:
		server.handleInsert(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (server *httpServer) handleLookup(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[1:]
	host, err := server.kelips.Lookup(&kelips.Request{Key: []byte(key), TTL: 2})
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error()))
	} else {
		w.Write([]byte(host))
	}
}

func (server *httpServer) handleInsert(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[1:]
	host, err := server.kelips.Insert([]byte(key))
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error()))
	} else {
		w.Write([]byte(host))
	}
}
