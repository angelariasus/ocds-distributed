package transport

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// TLSFiles locates one identity: the node's certificate and key and the CA it
// trusts. Every node is both a server and a client of its peers, so the same
// identity is presented in both directions.
type TLSFiles struct {
	Cert, Key, CA string
}

func TLSFromEnv() TLSFiles {
	return TLSFiles{Cert: os.Getenv("TLS_CERT"), Key: os.Getenv("TLS_KEY"), CA: os.Getenv("TLS_CA")}
}

func (f TLSFiles) Enabled() bool { return f.Cert != "" }

func (f TLSFiles) load() (tls.Certificate, *x509.CertPool, error) {
	cert, err := tls.LoadX509KeyPair(f.Cert, f.Key)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	pem, err := os.ReadFile(f.CA)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return tls.Certificate{}, nil, errors.New("no CA certificates in " + f.CA)
	}
	return cert, pool, nil
}

// ServerCredentials requires every caller to present a certificate signed by
// the internal CA: a node or client without one cannot even complete the handshake.
func (f TLSFiles) ServerCredentials() (credentials.TransportCredentials, error) {
	if !f.Enabled() {
		return insecure.NewCredentials(), nil
	}
	cert, pool, err := f.load()
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	}), nil
}

// ClientCredentials presents this node's certificate and verifies the peer's
// against the CA and against the host name being dialled.
func (f TLSFiles) ClientCredentials() (credentials.TransportCredentials, error) {
	if !f.Enabled() {
		return insecure.NewCredentials(), nil
	}
	cert, pool, err := f.load()
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		MinVersion:   tls.VersionTLS13,
	}), nil
}
