package marshal

import (
	"fmt"
	"reflect"
	"strconv"
)

func fieldHeaderName(f reflect.StructField) (string, bool) {
	h := f.Tag.Get("csv")

	if h == "-" {
		return "", false
	}

	// If there is no tag set, use a default name
	if h == "" {
		return f.Name, true
	}

	return h, true
}

type decoderFn func(*reflect.Value, *Row) error

type cfield struct {
	colIndex    int
	structField *reflect.StructField
	decoder     decoderFn
}

func newCfield(index int, sf *reflect.StructField) cfield {
	cf := cfield{
		colIndex:    index,
		structField: sf,
	}

	cf.decoder = cf.unassignedDecoder

	return cf
}

func (cf *cfield) assignUnmarshaller(code int) {
	if code == impsPtr {
		cf.decoder = cf.unmarshalPointer
	} else {
		cf.decoder = cf.unmarshalValue
	}
}

func (cf *cfield) unmarshalPointer(cell *reflect.Value, row *Row) error {
	val := row.At(cf.colIndex)
	m := cell.Addr().Interface().(Unmarshaler)
	m.UnmarshalCSV(val, row)

	return nil
}

func (cf *cfield) unmarshalValue(cell *reflect.Value, row *Row) error {
	val := row.At(cf.colIndex)
	m := cell.Interface().(Unmarshaler)
	m.UnmarshalCSV(val, row)
	return nil
}

func (cf *cfield) assignDecoder() {
	switch cf.structField.Type.Kind() {
	case reflect.String:
		cf.decoder = cf.decodeString
	case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int8:
		cf.decoder = cf.decodeInt
	case reflect.Float32:
		cf.decoder = cf.decodeFloat(32)
	case reflect.Float64:
		cf.decoder = cf.decodeFloat(64)
	case reflect.Bool:
		cf.decoder = cf.decodeBool
	default:
		cf.decoder = cf.ignoreValue
	}
}

func (cf *cfield) decodeBool(cell *reflect.Value, row *Row) error {
	val := row.At(cf.colIndex)
	var bv bool

	bt := cf.structField.Tag.Get("true")
	bf := cf.structField.Tag.Get("false")

	switch val {
	case bt:
		bv = true
	case bf:
		bv = false
	default:
		bv = true
	}

	cell.SetBool(bv)

	return nil
}

func (cf *cfield) decodeInt(cell *reflect.Value, row *Row) error {
	val := row.At(cf.colIndex)
	i, e := strconv.Atoi(val)

	if e != nil {
		return e
	}

	cell.SetInt(int64(i))
	return nil
}

func (cf *cfield) decodeString(cell *reflect.Value, row *Row) error {
	val := row.At(cf.colIndex)
	cell.SetString(val)

	return nil
}

func (cf *cfield) decodeFloat(bit int) decoderFn {
	return func(cell *reflect.Value, row *Row) error {
		val := row.At(cf.colIndex)
		n, err := strconv.ParseFloat(val, bit)

		if err != nil {
			return err
		}

		cell.SetFloat(n)

		return nil
	}
}

func (cf *cfield) ignoreValue(cell *reflect.Value, row *Row) error {
	return nil
}

func (cf *cfield) unassignedDecoder(cell *reflect.Value, row *Row) error {
	return fmt.Errorf("no decoder for %v\n", cf.structField.Name)
}