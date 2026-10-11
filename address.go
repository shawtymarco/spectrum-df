package spectrum

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

// NetherNet IDs identify a transport connection, never an account or an IP.
// The proxy/backend framing remains unchanged; the address field carries this
// explicit scheme rather than an unparseable diagnostic candidate string.
type netherNetAddress string

func (netherNetAddress) Network() string  { return "nethernet" }
func (a netherNetAddress) String() string { return string(a) }

func parsePublicAddress(value string) (net.Addr, error) {
	if rest, ok := strings.CutPrefix(value, "nethernet://"); ok {
		network, connection, found := strings.Cut(rest, "/")
		if !found {
			return nil, fmt.Errorf("invalid NetherNet address")
		}
		if _, err := strconv.ParseUint(network, 10, 64); err != nil {
			return nil, fmt.Errorf("invalid NetherNet network ID")
		}
		if _, err := strconv.ParseUint(connection, 10, 64); err != nil {
			return nil, fmt.Errorf("invalid NetherNet connection ID")
		}
		return netherNetAddress(value), nil
	}
	return net.ResolveUDPAddr("udp", value)
}
