package kelips

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/euforia/gossip/transport"
	"github.com/pkg/errors"
)

const (
	kelipsMagic uint16 = 9
)

const (
	endpointKelips = "/kelips"
	endpointPeer   = "/peer"
)

// HTTPTransport implements a HTTP based Transport interface
type HTTPTransport struct {
	// local advertise host
	host string
	// Registered group
	groups map[int64]AffinityGroup

	server *http.Server
	client *http.Client
}

// NewHTTPTransport returns a new HTTPTransport.  If enableMagic is true, a muxed
// client with the magic number is used instead of the default
func NewHTTPTransport(enableMagic bool) *HTTPTransport {
	trans := &HTTPTransport{
		groups: make(map[int64]AffinityGroup),
	}

	trans.initClient(enableMagic)

	return trans
}

// Start starts serving on the transport in a separate go-routine
func (trans *HTTPTransport) Start(ln net.Listener) error {
	trans.server = &http.Server{
		Addr:    trans.host,
		Handler: trans,
	}
	go trans.server.Serve(ln)

	return nil
}

// Shutdown gracefully shuts down the transport
func (trans *HTTPTransport) Shutdown(ctx context.Context) error {
	return trans.server.Shutdown(ctx)
	// return trans.server.Close()
}

func (trans *HTTPTransport) initClient(magic bool) {
	tr := &http.Transport{
		TLSHandshakeTimeout: 5 * time.Second,
		MaxIdleConns:        5,
		MaxIdleConnsPerHost: 3,
	}

	if magic {
		tr.Dial = func(network, addr string) (net.Conn, error) {
			return transport.DialTimeout(addr, kelipsMagic, 5*time.Second)
		}
	} else {
		tr.Dial = (&net.Dialer{
			Timeout: 5 * time.Second,
		}).Dial
	}

	trans.client = &http.Client{
		Transport: tr,
		Timeout:   5 * time.Second,
	}
}

func (trans *HTTPTransport) makeRequest(contact GroupContact, endpoint, method, key string, ttl int) *http.Request {
	url := "http://" + contact.Host + endpoint + "/" + key
	req, _ := http.NewRequest(method, url, nil)
	req.Header.Set("Affinity-Group", fmt.Sprintf("%d", contact.ID))
	// Default originator
	req.Header.Set("Originator", trans.host)
	if ttl > -1 {
		req.Header.Set("Kelips-TTL", strconv.Itoa(ttl))
	}

	return req
}

// Insert key at remote group
func (trans *HTTPTransport) Insert(contact GroupContact, key []byte) (string, error) {
	req := trans.makeRequest(contact, endpointKelips, http.MethodPost, string(key), 3)
	resp, err := trans.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	b, err := readResponse(resp)

	return string(b), err
}

// Lookup should return the home node of the key
func (trans *HTTPTransport) Lookup(contact GroupContact, r *Request) (string, error) {
	req := trans.makeRequest(contact, endpointKelips, http.MethodGet, string(r.Key), r.TTL)
	req.Header.Set("Originator", r.Originator.String())

	resp, err := trans.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	b, err := readResponse(resp)

	return string(b), err
}

// AddPeer makes a remote request to add a peer to a group
func (trans *HTTPTransport) AddPeer(contact GroupContact, host PeerContact) error {
	req := trans.makeRequest(contact, endpointPeer, http.MethodPost, host.Address(), -1)
	resp, err := trans.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	_, err = readResponse(resp)
	return err
}

// Register the affinity group with the transport
func (trans *HTTPTransport) Register(contact GroupContact, group AffinityGroup) {
	trans.groups[contact.ID] = group
	// All registrations will be the local node
	trans.host = contact.Host
}

func (trans *HTTPTransport) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Check group header or bail
	group := trans.getGroup(w, r)
	if group == nil {
		return
	}

	defer r.Body.Close()

	switch {
	case strings.HasPrefix(r.URL.Path, endpointKelips):
		key := strings.TrimPrefix(r.URL.Path, endpointKelips)
		key = strings.TrimPrefix(key, "/")
		if key == "" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		req, err := parseRequest(r)
		if err != nil {
			w.WriteHeader(400)
			w.Write([]byte(err.Error()))
			return
		}
		req.Key = []byte(key)

		switch r.Method {
		case http.MethodGet:
			trans.handleLookup(w, r, group, req)

		case http.MethodPost:
			trans.handleInsert(w, r, group, key)

		}

	case strings.HasPrefix(r.URL.Path, endpointPeer):
		key := strings.TrimPrefix(r.URL.Path, endpointPeer)
		key = strings.TrimPrefix(key, "/")
		if key == "" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		trans.handlePeer(w, r, group, key)

	default:
		w.WriteHeader(http.StatusNotFound)

	}
}

func (trans *HTTPTransport) handleLookup(w http.ResponseWriter, r *http.Request, group AffinityGroup, req *Request) {
	host, err := group.Lookup(req)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error()))
		return
	}

	w.Write([]byte(host))
}

func (trans *HTTPTransport) handleInsert(w http.ResponseWriter, r *http.Request, group AffinityGroup, key string) {

	host, err := group.Insert([]byte(key))
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error()))
		return
	}

	w.Write([]byte(host))
}

func (trans *HTTPTransport) handlePeer(w http.ResponseWriter, r *http.Request, group AffinityGroup, key string) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	log.Println("Transport AddPeer", key)
	err := group.AddPeer(&Peer{Host: key})
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error()))
		return
	}
}

func (trans *HTTPTransport) getGroup(w http.ResponseWriter, r *http.Request) AffinityGroup {
	gid, err := strconv.ParseInt(r.Header.Get("Affinity-Group"), 10, 64)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte("no affinity group " + err.Error()))
		return nil
	}

	group, ok := trans.groups[gid]
	if !ok {
		w.WriteHeader(404)
		return nil
	}

	return group
}

func parseRequest(r *http.Request) (*Request, error) {
	req := &Request{}
	sttl := r.Header.Get("Kelips-TTL")
	ttl, err := strconv.Atoi(sttl)
	if err != nil {
		return nil, err
	}
	req.TTL = ttl

	orig := r.Header.Get("Originator")
	ogc, err := parseGroupContact(orig)
	if err != nil {
		return nil, err
	}

	req.Originator = ogc
	return req, nil
}

func readResponse(resp *http.Response) ([]byte, error) {
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		if len(b) > 0 {
			return nil, errors.New(string(b))
		}
		return nil, errors.New(resp.Status)
	}
	return b, nil
}
