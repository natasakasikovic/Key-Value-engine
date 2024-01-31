package TokenBucket

import (
	"encoding/binary"
	"time"
)

type TokenBucket struct {
	TokenLimit  uint32
	TokensLeft  uint32
	RefreshRate int64
	LastRefresh int64
}

func NewTokenBucket(tokenLimit uint32, refreshRate int64) *TokenBucket {
	return &TokenBucket{TokenLimit: tokenLimit, TokensLeft: tokenLimit, RefreshRate: refreshRate, LastRefresh: Now()}
}
func (tb *TokenBucket) Serialize() []byte {
	//Size
	size := 4 + 4 + 8 + 8
	bytes := make([]byte, size)
	binary.BigEndian.PutUint32(bytes[0:4], tb.TokenLimit)
	binary.BigEndian.PutUint32(bytes[4:8], tb.TokensLeft)
	binary.BigEndian.PutUint32(bytes[8:16], uint32(tb.RefreshRate))
	binary.BigEndian.PutUint32(bytes[16:24], uint32(tb.LastRefresh))
	return bytes
}
func Deserialize(bytes []byte) *TokenBucket {
	tokenLimit := uint32(binary.BigEndian.Uint32(bytes[0:4]))
	tokensLeft := uint32(binary.BigEndian.Uint32(bytes[4:8]))
	refreshRate := int64(binary.BigEndian.Uint64(bytes[8:16]))
	lastRefresh := int64(binary.BigEndian.Uint64(bytes[16:24]))
	tb := &TokenBucket{
		TokenLimit:  tokenLimit,
		TokensLeft:  tokensLeft,
		RefreshRate: refreshRate,
		LastRefresh: lastRefresh,
	}
	return tb
}
func Now() int64 {
	return time.Now().Unix()
}

func IsPast(stored int64) bool {
	return stored < Now()
}
func (tb *TokenBucket) Refresh() {
	secondsPassed := Now() - tb.LastRefresh
	tb.LastRefresh = Now()
	if secondsPassed >= tb.RefreshRate {
		tb.TokensLeft = tb.TokenLimit
		return
	}

}
func (tb *TokenBucket) IsRequestAvailable() bool {
	tb.Refresh()
	if tb.TokensLeft <= 0 {
		return false
	}
	tb.TokensLeft--
	return true
}
