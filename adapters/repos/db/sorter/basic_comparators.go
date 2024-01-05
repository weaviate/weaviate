//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package sorter

import (
	"strings"
	"time"

	"github.com/weaviate/weaviate/entities/schema"
)

type basicComparatorProvider struct{}

func (bcp *basicComparatorProvider) provide(dataType schema.DataType, order string) basicComparator {
	switch dataType {
	case schema.DataTypeBlob:
		return newStringComparator(order)
	case schema.DataTypeText:
		return newStringComparator(order)
	case schema.DataTypeTextArray:
		return newStringArrayComparator(order)
	case schema.DataTypeNumber, schema.DataTypeInt:
		return newFloat64Comparator(order)
	case schema.DataTypeNumberArray, schema.DataTypeIntArray:
		return newFloat64ArrayComparator(order)
	case schema.DataTypeDate:
		return newDateComparator(order)
	case schema.DataTypeDateArray:
		return newDateArrayComparator(order)
	case schema.DataTypeBoolean:
		return newBoolComparator(order)
	case schema.DataTypeBooleanArray:
		return newBoolArrayComparator(order)
	case schema.DataTypePhoneNumber:
		return newFloat64ArrayComparator(order)
	case schema.DataTypeGeoCoordinates:
		return newFloat64ArrayComparator(order)
	default:
		return newAnyComparator(order)
	}
}

type basicComparator interface {
	compare(a, b interface{}) int
}

type stringComparator struct {
	lessValue int
}

func newStringComparator(order string) *stringComparator {
	return &stringComparator{lessValue(order)}
}

func (sc *stringComparator) compare(a, b interface{}) int {
	a, b = sc.untypedNil(a), sc.untypedNil(b)
	if a != nil && b != nil {
		return sc.compareStrings(*(a.(*string)), *(b.(*string)))
	}
	return handleNils(a == nil, b == nil, sc.lessValue)
}

func (sc *stringComparator) compareStrings(a, b string) int {
	if strings.EqualFold(a, b) {
		return 0
	}
	if strings.ToLower(a) < strings.ToLower(b) {
		return sc.lessValue
	}
	return -sc.lessValue
}

func (sc *stringComparator) untypedNil(x interface{}) interface{} {
	if x == (*string)(nil) {
		return nil
	}
	return x
}

type stringArrayComparator struct {
	sc *stringComparator
	ic *intComparator
}

func newStringArrayComparator(order string) *stringArrayComparator {
	return &stringArrayComparator{newStringComparator(order), newIntComparator(order)}
}

func (sac *stringArrayComparator) compare(a, b interface{}) int {
	a, b = sac.untypedNil(a), sac.untypedNil(b)
	if a != nil && b != nil {
		aArr, bArr := *(a.(*[]string)), *(b.(*[]string))
		aLen, bLen := len(aArr), len(bArr)

		for i := 0; i < aLen && i < bLen; i++ {
			if res := sac.sc.compareStrings(aArr[i], bArr[i]); res != 0 {
				return res
			}
		}
		return sac.ic.compareInts(aLen, bLen)
	}
	return handleNils(a == nil, b == nil, sac.sc.lessValue)
}

func (sac *stringArrayComparator) untypedNil(x interface{}) interface{} {
	if x == (*[]string)(nil) {
		return nil
	}
	return x
}

type float64Comparator struct {
	lessValue int
}

func newFloat64Comparator(order string) *float64Comparator {
	return &float64Comparator{lessValue(order)}
}

func (fc *float64Comparator) compare(a, b interface{}) int {
	a, b = fc.untypedNil(a), fc.untypedNil(b)
	if a != nil && b != nil {
		return fc.compareFloats64(*(a.(*float64)), *(b.(*float64)))
	}
	return handleNils(a == nil, b == nil, fc.lessValue)
}

func (fc *float64Comparator) compareFloats64(a, b float64) int {
	if a == b {
		return 0
	}
	if a < b {
		return fc.lessValue
	}
	return -fc.lessValue
}

func (fc *float64Comparator) untypedNil(x interface{}) interface{} {
	if x == (*float64)(nil) {
		return nil
	}
	return x
}

type float64ArrayComparator struct {
	fc *float64Comparator
	ic *intComparator
}

func newFloat64ArrayComparator(order string) *float64ArrayComparator {
	return &float64ArrayComparator{newFloat64Comparator(order), newIntComparator(order)}
}

func (fac *float64ArrayComparator) compare(a, b interface{}) int {
	a, b = fac.untypedNil(a), fac.untypedNil(b)
	if a != nil && b != nil {
		aArr, bArr := *(a.(*[]float64)), *(b.(*[]float64))
		aLen, bLen := len(aArr), len(bArr)

		for i := 0; i < aLen && i < bLen; i++ {
			if res := fac.fc.compareFloats64(aArr[i], bArr[i]); res != 0 {
				return res
			}
		}
		return fac.ic.compareInts(aLen, bLen)
	}
	return handleNils(a == nil, b == nil, fac.fc.lessValue)
}

func (fac *float64ArrayComparator) untypedNil(x interface{}) interface{} {
	if x == (*[]float64)(nil) {
		return nil
	}
	return x
}

type dateComparator struct {
	lessValue int
}

func newDateComparator(order string) *dateComparator {
	return &dateComparator{lessValue(order)}
}

func (dc *dateComparator) compare(a, b interface{}) int {
	a, b = dc.untypedNil(a), dc.untypedNil(b)
	if a != nil && b != nil {
		return dc.compareDates(*(a.(*time.Time)), *(b.(*time.Time)))
	}
	return handleNils(a == nil, b == nil, dc.lessValue)
}

func (dc *dateComparator) compareDates(a, b time.Time) int {
	if a.Equal(b) {
		return 0
	}
	if a.Before(b) {
		return dc.lessValue
	}
	return -dc.lessValue
}

func (dc *dateComparator) untypedNil(x interface{}) interface{} {
	if x == (*time.Time)(nil) {
		return nil
	}
	return x
}

type dateArrayComparator struct {
	dc *dateComparator
	ic *intComparator
}

func newDateArrayComparator(order string) *dateArrayComparator {
	return &dateArrayComparator{newDateComparator(order), newIntComparator(order)}
}

func (dac *dateArrayComparator) compare(a, b interface{}) int {
	a, b = dac.untypedNil(a), dac.untypedNil(b)
	if a != nil && b != nil {
		aArr, bArr := *(a.(*[]time.Time)), *(b.(*[]time.Time))
		aLen, bLen := len(aArr), len(bArr)

		for i := 0; i < aLen && i < bLen; i++ {
			if res := dac.dc.compareDates(aArr[i], bArr[i]); res != 0 {
				return res
			}
		}
		return dac.ic.compareInts(aLen, bLen)
	}
	return handleNils(a == nil, b == nil, dac.dc.lessValue)
}

func (dac *dateArrayComparator) untypedNil(x interface{}) interface{} {
	if x == (*[]time.Time)(nil) {
		return nil
	}
	return x
}

type boolComparator struct {
	lessValue int
}

func newBoolComparator(order string) *boolComparator {
	return &boolComparator{lessValue(order)}
}

func (bc *boolComparator) compare(a, b interface{}) int {
	a, b = bc.untypedNil(a), bc.untypedNil(b)
	if a != nil && b != nil {
		return bc.compareBools(*(a.(*bool)), *(b.(*bool)))
	}
	return handleNils(a == nil, b == nil, bc.lessValue)
}

func (bc *boolComparator) compareBools(a, b bool) int {
	if a && b {
		return 0
	}
	if !a && !b {
		return 0
	}
	if !a {
		return bc.lessValue
	}
	return -bc.lessValue
}

func (bc *boolComparator) untypedNil(x interface{}) interface{} {
	if x == (*bool)(nil) {
		return nil
	}
	return x
}

type boolArrayComparator struct {
	bc *boolComparator
	ic *intComparator
}

func newBoolArrayComparator(order string) *boolArrayComparator {
	return &boolArrayComparator{newBoolComparator(order), newIntComparator(order)}
}

func (bac *boolArrayComparator) compare(a, b interface{}) int {
	a, b = bac.untypedNil(a), bac.untypedNil(b)
	if a != nil && b != nil {
		aArr, bArr := *(a.(*[]bool)), *(b.(*[]bool))
		aLen, bLen := len(aArr), len(bArr)

		for i := 0; i < aLen && i < bLen; i++ {
			if res := bac.bc.compareBools(aArr[i], bArr[i]); res != 0 {
				return res
			}
		}
		return bac.ic.compareInts(aLen, bLen)
	}
	return handleNils(a == nil, b == nil, bac.bc.lessValue)
}

func (bac *boolArrayComparator) untypedNil(x interface{}) interface{} {
	if x == (*[]bool)(nil) {
		return nil
	}
	return x
}

type intComparator struct {
	lessValue int
}

func newIntComparator(order string) *intComparator {
	return &intComparator{lessValue(order)}
}

func (ic *intComparator) compare(a, b interface{}) int {
	a, b = ic.untypedNil(a), ic.untypedNil(b)
	if a != nil && b != nil {
		return ic.compareInts(*(a.(*int)), *(b.(*int)))
	}
	return handleNils(a == nil, b == nil, ic.lessValue)
}

func (ic *intComparator) compareInts(a, b int) int {
	if a == b {
		return 0
	}
	if a < b {
		return ic.lessValue
	}
	return -ic.lessValue
}

func (ic *intComparator) untypedNil(x interface{}) interface{} {
	if x == (*int)(nil) {
		return nil
	}
	return x
}

type anyComparator struct {
	lessValue int
}

func newAnyComparator(order string) *anyComparator {
	return &anyComparator{lessValue(order)}
}

func (ac *anyComparator) compare(a, b interface{}) int {
	if a != nil && b != nil {
		return 0
	}
	return handleNils(a == nil, b == nil, ac.lessValue)
}

func handleNils(aNil, bNil bool, lessValue int) int {
	if aNil && bNil {
		return 0
	}
	if aNil {
		return lessValue
	}
	return -lessValue
}

func lessValue(order string) int {
	if order == "desc" {
		return 1
	}
	return -1
}
