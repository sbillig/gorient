package gorient

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

type itemType int

const (
	itemError itemType = iota
	itemStartDoc
	itemEndDoc
	itemStartList
	itemEndList
	itemStartSet
	itemEndSet
	itemStartMap
	itemEndMap
	itemAt
	itemComma
	itemColon
	itemSymbol
	itemString
	itemRID
	itemBinary
	itemInt
	itemByte
	itemShort
	itemLong
	itemFloat
	itemDouble
	itemBigDecimal
	itemDate
	itemTime
)

var itemName = map[itemType]string {
	itemError: "Error",
	itemStartDoc: "StartDoc",
	itemEndDoc: "EndDoc",
	itemStartList: "StartList",
	itemEndList: "EndList",
	itemStartSet: "StartSet",
	itemEndSet: "EndSet",
	itemStartMap: "StartMap",
	itemEndMap: "EndMap",
	itemAt: "At",
	itemComma: "Comma",
	itemColon: "Colon",
	itemSymbol: "Symbol",
	itemString: "String",
	itemRID: "RID",
	itemBinary: "Binary",
	itemInt: "Int",
	itemByte: "Byte",
	itemShort: "Short",
	itemLong: "Long",
	itemFloat: "Float",
	itemDouble: "Double",
	itemBigDecimal: "BigDecimal",
	itemDate: "Date",
	itemTime: "Time",
}

func (i itemType) String() string {
	return itemName[i]
}

const eof = -1

type item struct {
	typ itemType
	val string
}

func (i *item) String() string {
	return fmt.Sprintf("%s (%s)", i.val, i.typ)
}
type lexer struct {
	input string
	start int
	pos   int
	width int
	state stateFn
	items chan item
}
type stateFn func(*lexer) stateFn

func lex(input string) (*lexer, chan item) {
	l := &lexer{
		input: input,
		items: make(chan item, 10),
	}
	go l.run()
	return l, l.items
}

func (l *lexer) run() {
	for state := lexValue; state != nil; {
		state = state(l)
	}
	close(l.items)
}

func (l *lexer) errorf(format string, args ...interface{}) stateFn {
	l.items <- item{itemError, fmt.Sprintf(format, args...)}
	return nil
}

func (l *lexer) emit(t itemType) {
	it := item{t, l.input[l.start:l.pos]}
	l.items <- it
	l.start = l.pos
}

func (l *lexer) ignore() {
	l.start = l.pos
}

func (l *lexer) eat(n int) {
	l.pos += n
	l.ignore()
}

func (l *lexer) next() (r rune) {
	if l.pos >= len(l.input) {
		l.width = 0
		return eof
	}
	r, l.width = utf8.DecodeRuneInString(l.input[l.pos:])
	l.pos += l.width
	return r
}

func (l *lexer) peek() rune {
	r := l.next()
	l.backup()
	return r
}

func (l *lexer) backup() {
	l.pos -= l.width
}

func lexValue(l *lexer) stateFn {
	rec := func (i itemType) stateFn {
		l.emit(i)
		return lexValue
	}

	c := l.next()
	switch c {
	case ',': return rec(itemComma)
	case '[': return rec(itemStartList)
	case ']': return rec(itemEndList)
	case '{': return rec(itemStartMap)
	case '}': return rec(itemEndMap)
	case '<': return rec(itemStartSet)
	case '>': return rec(itemEndSet)
	case '(': return rec(itemStartDoc)
	case ')': return rec(itemEndDoc)
	case '@': return rec(itemAt)
	case ':': return rec(itemColon)
	case '"':
		return lexString
	case '_':
		l.ignore()
		return lexBinary
	case '#':
		return lexRID
	case eof:
		l.emit(itemEndDoc)
		return nil
	default:
		switch {
		case unicode.IsDigit(c):
			return lexNumber
		case unicode.IsLetter(c):
			return lexSymbol
		default:
			fmt.Println("unrec:", string(c))
		}
	}
	return nil
}

func lexNumber(l *lexer) stateFn {
	r := l.next()
	for unicode.IsDigit(r) || r == '.' {
		r = l.next()
	}
	rec := func(t itemType) stateFn {
		l.backup()
		l.emit(t)
		l.eat(1)
		return lexValue
	}
	switch r {
	case 'b': return rec(itemByte)
	case 's': return rec(itemShort)
	case 'l': return rec(itemLong)
	case 'f': return rec(itemFloat)
	case 'd': return rec(itemDouble)
	case 'c': return rec(itemBigDecimal)
	case 'a': return rec(itemDate)
	case 't': return rec(itemTime)
	default:
		l.backup()
		l.emit(itemInt)
		return lexValue
	}
	return nil
}

func lexRID(l *lexer) stateFn {
	for r := l.next(); unicode.IsDigit(r) || r == ':'; r = l.next() {}
	l.backup()
	l.emit(itemRID)
	return lexValue
}

func lexSymbol(l *lexer) stateFn {
	for {
		switch r := l.next(); {
		// TODO: valid field name characters
		case r == '_' || unicode.IsDigit(r) || unicode.IsLetter(r):
			// continue
		default:
			l.backup()
			l.emit(itemSymbol)
			return lexValue
		}
	}
	return nil
}

func lexBinary(l *lexer) stateFn {
	i := strings.IndexRune(l.input[l.pos:], '_')
	if i < 0 {
		return l.errorf("unending binary")
	}
	l.pos += i
	l.emit(itemBinary)
	l.eat(1)
	return lexValue
}

func lexString(l *lexer) stateFn {
Loop:
	for {
		switch l.next() {
		case '\\':
			if r := l.next(); r != eof && r != '\n' {
				break
			}
			fallthrough
		case eof, '\n':
			return l.errorf("unterminated quoted string")
		case '"':
			break Loop
		}
	}
	l.emit(itemString)
	return lexValue
}
