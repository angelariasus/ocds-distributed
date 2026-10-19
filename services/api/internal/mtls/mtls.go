// Package mtls loads one service identity (certificate, key, trusted CA) and
// builds the TLS configurations for both ends of a mutually authenticated
// connection.
package mtls

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
)

type Identity struct {
	Cert, Key, CA string
}

func (id Identity) Enabled() bool { return id.Cert != "" }

func (id Identity) load() (tls.Certificate, *x509.CertPool, error) {
	cert, err := tls.LoadX509KeyPair(id.Cert, id.Key)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	pem, err := os.ReadFile(id.CA)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return tls.Certificate{}, nil, errors.New("no CA certificates in " + id.CA)
	}
	return cert, pool, nil
}

// Server requires and verifies a client certificate from the CA.
func (id Identity) Server() (*tls.Config, error) {
	cert, pool, err := id.load()
	if err != nil {
		return nil, err
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS12}, nil
}

// Client presents this identity and verifies the server against the CA.
func (id Identity) Client() (*tls.Config, error) {
	cert, pool, err := id.load()
	if err != nil {
		return nil, err
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: pool, MinVersion: tls.VersionTLS12}, nil
}
