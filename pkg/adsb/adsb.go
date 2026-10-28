package adsb

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/klauspost/compress/zstd"
)

const DefaultWholeEarthEndpoint = "https://globe.adsbexchange.com/re-api/?binCraft&zstd&box=-90,90,-180,180"

type Aircraft struct {
	Hex          string   `json:"hex"`
	Flight       string   `json:"flight,omitempty"`
	Registration string   `json:"registration,omitempty"`
	AircraftType string   `json:"aircraft_type,omitempty"`
	Category     string   `json:"category,omitempty"`
	Lat          *float64 `json:"lat,omitempty"`
	Lon          *float64 `json:"lon,omitempty"`
	AltBaro      *int     `json:"alt,omitempty"`
	Ground       bool     `json:"ground"`
	Speed        *float64 `json:"speed,omitempty"`
	Track        *float64 `json:"track,omitempty"`
	VerticalRate *int     `json:"vert_rate,omitempty"`
	Squawk       string   `json:"squawk,omitempty"`
	Emergency    string   `json:"emergency,omitempty"`
	Seen         float64  `json:"seen"`
}

type GlobeSnapshot struct {
	Timestamp            float64    `json:"timestamp"`
	TotalGlobal          int        `json:"total_global"`
	GlobalAcCountWithPos uint32     `json:"global_with_pos"`
	Aircraft             []Aircraft `json:"aircraft"`
}

var emergencyMap = map[uint8]string{
	1: "general",
	2: "lifeguard",
	3: "minfuel",
	4: "nordo",
	5: "unlawful",
	6: "downed",
	7: "reserved",
}

var zstdDecoder *zstd.Decoder

func init() {
	var err error
	zstdDecoder, err = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
	if err != nil {
		panic(err)
	}
}

func FetchGlobalSnapshot(ctx context.Context, client *http.Client, endpoint string) (*GlobeSnapshot, error) {
	if endpoint == "" {
		endpoint = DefaultWholeEarthEndpoint
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Referer", "https://globe.adsbexchange.com/")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Encoding", "gzip, deflate, br, zstd")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(snippet))
	}

	compressedData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	decompressed, err := zstdDecoder.DecodeAll(compressedData, nil)
	if err != nil {
		return nil, fmt.Errorf("zstd decompression: %w", err)
	}

	return ParseBinCraft(decompressed)
}

func ParseBinCraft(data []byte) (*GlobeSnapshot, error) {
	if len(data) < 52 {
		return nil, fmt.Errorf("buffer too small for binCraft header (%d bytes)", len(data))
	}

	var u32Header [13]uint32
	if err := binary.Read(bytes.NewReader(data[:52]), binary.LittleEndian, &u32Header); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	timestamp := float64(u32Header[0])/1000.0 + float64(u32Header[1])*4294967.296
	stride := int(u32Header[2])
	globalWithPos := u32Header[3]
	binCraftVersion := u32Header[10]

	if stride <= 0 || len(data) < stride {
		return nil, fmt.Errorf("invalid stride %d", stride)
	}

	count := (len(data) - stride) / stride
	aircraftList := make([]Aircraft, 0, count)

	for off := stride; off+stride <= len(data); off += stride {
		acBuf := data[off : off+stride]
		var ac Aircraft

		s320 := int32(binary.LittleEndian.Uint32(acBuf[0:4]))
		hasTilde := (s320 & (1 << 24)) != 0
		hexVal := s320 & 0x00FFFFFF
		hexStr := fmt.Sprintf("%06x", hexVal)
		if hasTilde {
			hexStr = "~" + hexStr
		}
		ac.Hex = hexStr

		if binCraftVersion >= 20240218 {
			s321 := int32(binary.LittleEndian.Uint32(acBuf[4:8]))
			ac.Seen = float64(s321) / 10.0
		} else {
			u163 := binary.LittleEndian.Uint16(acBuf[6:8])
			ac.Seen = float64(u163) / 10.0
		}

		lonRaw := float64(int32(binary.LittleEndian.Uint32(acBuf[8:12]))) / 1e6
		latRaw := float64(int32(binary.LittleEndian.Uint32(acBuf[12:16]))) / 1e6
		baroRateRaw := int(int16(binary.LittleEndian.Uint16(acBuf[16:18]))) * 8
		altBaroRaw := int(int16(binary.LittleEndian.Uint16(acBuf[20:22]))) * 25
		squawkVal := binary.LittleEndian.Uint16(acBuf[32:34])
		gsRaw := float64(int16(binary.LittleEndian.Uint16(acBuf[34:36]))) / 10.0
		trackRaw := float64(int16(binary.LittleEndian.Uint16(acBuf[40:42]))) / 90.0

		catByte := acBuf[64]
		if catByte != 0 {
			ac.Category = fmt.Sprintf("%02X", catByte)
		}
		emergCode := acBuf[67] & 0x0F
		if em, ok := emergencyMap[emergCode]; ok {
			ac.Emergency = em
		}
		airGround := acBuf[68] & 0x0F
		ac.Ground = airGround == 1

		flightStr := strings.TrimSpace(string(bytes.TrimRight(acBuf[78:86], "\x00 ")))
		typeStr := strings.TrimSpace(string(bytes.TrimRight(acBuf[88:92], "\x00 ")))
		regStr := strings.TrimSpace(string(bytes.TrimRight(acBuf[92:104], "\x00 ")))

		ac.Flight = flightStr
		ac.AircraftType = typeStr
		ac.Registration = regStr

		v73 := acBuf[73]
		v74 := acBuf[74]
		v75 := acBuf[75]
		v76 := acBuf[76]

		if (v73 & 8) == 0 {
			ac.Flight = ""
		}
		if (v73&16) != 0 && !ac.Ground {
			ac.AltBaro = &altBaroRaw
		}
		if (v73 & 64) != 0 {
			ac.Lat = &latRaw
			ac.Lon = &lonRaw
		}
		if (v73 & 128) != 0 {
			ac.Speed = &gsRaw
		}

		if (v74 & 8) != 0 {
			ac.Track = &trackRaw
		}
		if (v75 & 1) != 0 {
			ac.VerticalRate = &baroRateRaw
		}
		if (v76&4) != 0 && squawkVal != 0 {
			ac.Squawk = fmt.Sprintf("%04x", squawkVal)
		}

		aircraftList = append(aircraftList, ac)
	}

	return &GlobeSnapshot{
		Timestamp:            timestamp,
		TotalGlobal:          len(aircraftList),
		GlobalAcCountWithPos: globalWithPos,
		Aircraft:             aircraftList,
	}, nil
}
