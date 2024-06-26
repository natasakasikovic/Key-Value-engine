package HyperLogLog

import (
	"encoding/binary"
	"hash/fnv"
	"math"
	"math/bits"
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

func firstKbits(value, k uint64) uint64 {
	return value >> (64 - k)
}

func trailingZeroBits(value uint64) int {
	return bits.TrailingZeros64(value)
}

type HLL struct {
	m   uint64
	p   uint8
	reg []uint8
}

func CreateHLL(p uint8) *HLL {
	var m uint64 = 1 << p
	reg := make([]uint8, m)
	hll := &HLL{m: m, p: p, reg: reg}
	return hll
}

func (hll *HLL) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.reg {
		sum += math.Pow(math.Pow(2.0, float64(val)), -1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.m))
	estimation := alpha * math.Pow(float64(hll.m), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation <= 2.5*float64(hll.m) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.m) * math.Log(float64(hll.m)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hll *HLL) emptyCount() int {
	sum := 0
	for _, val := range hll.reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}

func hashStringToUint64(input string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(input))
	return h.Sum64()
}

func (hll *HLL) Insert(str string) {
	value := hashStringToUint64(str)
	zero_bits := trailingZeroBits(value)
	bucket_index := firstKbits(value, uint64(hll.p))

	existing_value := hll.reg[bucket_index]

	if (zero_bits + 1) > int(existing_value) {
		hll.reg[bucket_index] = uint8(zero_bits) + 1
	}
}
func (hll HLL) Serialize() []byte {
	var size int = 4 + 8 + 4*len(hll.reg)

	bytes := make([]byte, size)
	binary.BigEndian.PutUint32(bytes[0:4], uint32(hll.p))
	binary.BigEndian.PutUint64(bytes[4:12], hll.m)

	for i := 0; i < int(hll.m); i++ {
		binary.BigEndian.PutUint32(bytes[12+4*i:16+4*i], uint32(hll.reg[i]))
	}
	return bytes
}

func Deserialize(bytes []byte) *HLL {

	p := uint32(binary.BigEndian.Uint32(bytes[0:4]))
	m := uint64(binary.BigEndian.Uint64(bytes[4:12]))

	reg := make([]uint8, m)
	for i := 0; i < int(m); i++ {
		reg[i] = uint8(uint32(binary.BigEndian.Uint32(bytes[12+4*i : 16+4*i])))
	}
	return &HLL{
		p:   uint8(p),
		m:   m,
		reg: reg,
	}
}
