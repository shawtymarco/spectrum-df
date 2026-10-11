package spectrum

import (
	"net"
	"testing"
)

func TestParsePublicAddress(t *testing.T) {
	for _, value := range []string{"nethernet://123/456", "nethernet://18446744073709551615/0"} {
		a, err := parsePublicAddress(value)
		if err != nil || a.Network() != "nethernet" || a.String() != value {
			t.Fatalf("parse %q: %v %v", value, a, err)
		}
		if _, ok := a.(*net.UDPAddr); ok {
			t.Fatal("opaque identity masquerades as UDP")
		}
	}
	for _, value := range []string{"nethernet://123", "nethernet://abc/1", "nethernet://1/2/3", "nethernet://1/18446744073709551616"} {
		if _, err := parsePublicAddress(value); err == nil {
			t.Fatalf("accepted malformed address %q", value)
		}
	}
	a, err := parsePublicAddress("127.0.0.1:19132")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := a.(*net.UDPAddr); !ok || a.String() != "127.0.0.1:19132" {
		t.Fatal("legacy address changed")
	}
}
