package peerid

import (
	"fmt"
	"strconv"
	"strings"
)

type Client struct {
	Name    string
	Version string
}

func (c Client) String() string {
	return fmt.Sprintf("%s %s", c.Name, c.Version)
}

func isLetter(b byte) bool {
	return (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z')
}

var azureus = map[string]string{
	"7T": "Torrent",
	"AB": "AnyEvent::BitTorrent",
	"AG": "Ares",
	"AR": "Arctic",
	"AT": "Artemis",
	"AV": "Avicora",
	"AX": "BitPump",
	"AZ": "Azureus",
	"A~": "Ares",
	"BB": "BitBuddy",
	"BC": "BitComet",
	"BE": "Baretorrent",
	"BF": "Bitflu",
	"BG": "BTG",
	"BL": "BitBlinder",
	"BP": "BitTorrent Pro",
	"BR": "BitRocket",
	"BS": "BTSlave",
	"BT": "BitTorrent",
	"BW": "BitWombat",
	"Bt": "Bt",
	"CD": "Enhanced CTorrent",
	"CT": "CTorrent",
	"DE": "Deluge",
	"DP": "Propagate Data Client",
	"EB": "EBit",
	"ES": "electric sheep",
	"FC": "FileCroc",
	"FD": "Free Download Manager",
	"FT": "FoxTorrent",
	"FX": "Freebox BitTorrent",
	"GS": "GSTorrent",
	"HK": "Hekate",
	"HL": "Halite",
	"HM": "hMule",
	"HN": "Hydranode",
	"IL": "iLivid",
	"JS": "Justseed.it",
	"JT": "JavaTorrent",
	"KG": "KGet",
	"KT": "KTorrent",
	"LC": "LeechCraft",
	"LH": "LH-ABC",
	"LP": "Lphant",
	"LT": "libtorrent",
	"LW": "LimeWire",
	"MK": "Meerkat",
	"MO": "MonoTorrent",
	"MP": "MooPolice",
	"MR": "Miro",
	"MT": "MoonlightTorrent",
	"NB": "Net::BitTorrent",
	"NX": "Net Transport",
	"OS": "OneSwarm",
	"OT": "OmegaTorrent",
	"PB": "Protocol::BitTorrent",
	"PD": "Pando",
	"PI": "PicoTorrent",
	"PT": "PHPTracker",
	"QD": "QQDownload",
	"RT": "Retriever",
	"RZ": "RezTorrent",
	"SD": "Xunlei",
	"SM": "SoMud",
	"SP": "BitSpirit",
	"SS": "SwarmScope",
	"ST": "SymTorrent",
	"SZ": "Shareaza",
	"S~": "Shareaza",
	"TB": "Torch",
	"TE": "terasaur Seed Bank",
	"TL": "Tribler",
	"TN": "TorrentDotNET",
	"TR": "Transmission",
	"TS": "Torrentstorm",
	"TT": "TuoTu",
	"UL": "uLeecher!",
	"UM": "µTorrent for Mac",
	"UT": "µTorrent",
	"VG": "Vagaa",
	"WD": "WebTorrent Desktop",
	"WT": "BitLet",
	"WW": "WebTorrent",
	"WY": "FireTorrent",
	"XF": "Xfplay",
	"XL": "Xunlei",
	"XS": "XSwifter",
	"XT": "XanTorrent",
	"XX": "Xtorrent",
	"ZT": "ZipTorrent",
	"lt": "libTorrent",
	"qB": "qBittorrent",
	"st": "sharktorrent",
}

func atoi3(a, b, c string) (int, int, int, bool) {
	p1, err := strconv.Atoi(a)
	if err != nil {
		return 0, 0, 0, false
	}
	p2, err := strconv.Atoi(b)
	if err != nil {
		return 0, 0, 0, false
	}
	p3, err := strconv.Atoi(c)
	if err != nil {
		return 0, 0, 0, false
	}
	return p1, p2, p3, true
}

func parseDigit(b byte) (int, bool) {
	switch {
	case b >= '0' && b <= '9':
		return int(b - '0'), true
	case b >= 'A' && b <= 'Z':
		return int(b - 'A' + 10), true
	case b >= 'a' && b <= 'z':
		return int(b - 'a' + 36), true
	default:
		return 0, false
	}
}

func parseDigits(bs []byte) ([]string, bool) {
	// https://wiki.theory.org/BitTorrentSpecification#peer_id
	// says "four ascii digits for version number", but we've seen
	// alpha-numeric versions (with A=10) in the wild, just like
	// in Shadow's style.

	parts := make([]string, len(bs))
	for i, b := range bs {
		n, ok := parseDigit(b)
		if !ok {
			return nil, false
		}
		parts[i] = strconv.Itoa(n)
	}
	return parts, true
}

func Parse(peerID [20]byte) (Client, bool) {
	if peerID[0] == 'M' {
		parts := strings.Split(string(peerID[1:]), "-")
		if len(parts) < 3 {
			return Client{}, false
		}
		p1, p2, p3, ok := atoi3(parts[0], parts[1], parts[2])
		if !ok {
			return Client{}, false
		}
		return Client{
			Name:    "BitTorrent",
			Version: fmt.Sprintf("%d.%d.%d", p1, p2, p3),
		}, true
	} else if peerID[0] == '-' && isLetter(peerID[1]) && isLetter(peerID[2]) && peerID[7] == '-' {
		// Azureus-style peer ID
		app, ok := azureus[string(peerID[1:3])]
		if !ok {
			return Client{}, false
		}

		parts, ok := parseDigits(peerID[3:7])
		if !ok {
			return Client{}, false
		}
		switch app {
		case "µTorrent":
			if peerID[6] == 'W' || peerID[6] == 'S' || peerID[6] == '0' {
				// "W" denotes that "protocol enhancements" are enabled, which is some weird crypto wallet nonsense.
				// No idea what "S" stands for, but it's used when protocol enhancements aren't turned on.
				parts = parts[:3]
			}
		default:
			if peerID[6] == '0' {
				parts = parts[:3]
			}
		}

		return Client{
			Name:    app,
			Version: strings.Join(parts, "."),
		}, true
	} else {
		// XXX implement more schemes
		return Client{}, false
	}
}
