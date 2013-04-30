package gorient

import (
	"fmt"
	"strconv"
)

func parse(s string) *doc {
	_, out := lex(s)

	p := &par{items: out}
	return parseDoc(p)
}

type par struct {
	items chan item
	peekCount int
	buf [2]item
}

func (p *par) errorf(format string, args ...interface{}) {
	panic(fmt.Errorf(format, args...))
}

func (p *par) peek() item {
	if p.peekCount > 0 {
		return p.buf[p.peekCount-1]
	}
	p.peekCount = 1
	p.buf[0] = <- p.items
	return p.buf[0]
}

func (p *par) backup() {
	p.peekCount++
}

func (p *par) next() item {
	if p.peekCount > 0 {
		p.peekCount--
	} else {
		p.buf[0] = <- p.items
	}
	return p.buf[p.peekCount]
}

type doc struct {
	class string
	fields map[string]interface{}
}

func (d *doc) String() string {
	if len(d.class) > 0 {
		return fmt.Sprintf("%s(%v)", d.class, d.fields)
	}
	return fmt.Sprintf("(%v)", d.fields)
}

func (p *par) expect(t itemType) item {
	i := p.next()
	if i.typ != t {
		p.errorf("expected %s, got %s", t, i.typ)
	}
	return i
}
func parseDoc(p *par) *doc {
	// StartDoc has been seen
	out := &doc{fields: make(map[string]interface{}, 8)}

	f := p.expect(itemSymbol)
	div := p.next()
	if div.typ == itemAt {
		out.class = f.val
		f = p.expect(itemSymbol)
		div = p.next()
	}

	for {
		out.fields[f.val] = parseValue(p)
		n := p.next()
		if n.typ == itemEndDoc {
			return out
		}
		if n.typ != itemComma {
			p.errorf("expected Comma, got %s (%v)", n.val, n.typ)
		}

		f = p.expect(itemSymbol)
		div = p.expect(itemColon)
	}
	return out
}
func parseValue(p *par) interface{} {
	n := p.next()
	switch n.typ {
	case itemComma:
		p.backup()
		return nil
	case itemStartMap:
		return parseMap(p)
	case itemStartDoc:
		return parseDoc(p)
	case itemStartList, itemStartSet:
		return parseList(p)
	case itemString:
		s, err := strconv.Unquote(n.val)
		if err != nil { p.errorf("failed to unquote string: %s", n.val) }
		return s
	case itemRID:
		return n.val
	case itemBinary:
		return n.val
	case itemSymbol:
		if n.val == "null" {
			return nil
		}
		v, err := strconv.ParseBool(n.val)
		if err != nil { p.errorf("failed to parse bool: %s", n.val) }
		return v
	case itemByte:
		v, err := strconv.ParseInt(n.val, 10, 8)
		if err != nil { p.errorf("failed to parse byte: %s", n.val) }
		return byte(v)
	case itemShort:
		v, err := strconv.ParseInt(n.val, 10, 16)
		if err != nil { p.errorf("failed to parse short: %s", n.val) }
		return int16(v)
	case itemInt:
		v, err := strconv.ParseInt(n.val, 10, 32)
		if err != nil { p.errorf("failed to parse int: %s", n.val) }
		return int32(v)
	case itemLong:
		v, err := strconv.ParseInt(n.val, 10, 64)
		if err != nil { p.errorf("failed to parse long: %s", n.val) }
		return v
	case itemDate, itemTime:
		v, err := strconv.ParseUint(n.val, 10, 64)
		if err != nil { p.errorf("failed to parse time: %s", n.val) }
		return v
	case itemFloat:
		v, err := strconv.ParseFloat(n.val, 32)
		if err != nil { p.errorf("failed to parse float: %s", n.val) }
		return float32(v)
	case itemDouble, itemBigDecimal:
		v, err := strconv.ParseFloat(n.val, 64)
		if err != nil { p.errorf("failed to parse double: %s", n.val) }
		return v
	}
	p.errorf("unrecognized: %s (%s)", n.val, n.typ)
	return nil
}

func parseList(p *par) interface{} {
	out := make([]interface{}, 0)
	for {
		n := p.next()
		if n.typ == itemEndList || n.typ == itemEndSet {
			return out
		}
		if n.typ != itemComma {
			p.backup()
		}
		out = append(out, parseValue(p))
	}
	return out
}

func parseMap(p *par) interface{} {
	out := make(map[string]interface{})
	for {
		f := p.next()
		if f.typ == itemEndMap {
			return out
		}
		if f.typ == itemComma {
			f = p.next()
		}
		if f.typ != itemString {
			p.errorf("expected field name (string), got %s", f.typ)
		}
		p.expect(itemColon)
		out[f.val] = parseValue(p)
	}
	return nil
}
