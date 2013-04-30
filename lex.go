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

func emitVal(l *lexer, i itemType) stateFn {
	l.emit(i)
	return lexValue
}
func lexValue(l *lexer) stateFn {
	c := l.next()
	switch c {
	case ',': return emitVal(l, itemComma)
	case '[': return emitVal(l, itemStartList)
	case ']': return emitVal(l, itemEndList)
	case '{': return emitVal(l, itemStartMap)
	case '}': return emitVal(l, itemEndMap)
	case '<': return emitVal(l, itemStartSet)
	case '>': return emitVal(l, itemEndSet)
	case '(': return emitVal(l, itemStartDoc)
	case ')': return emitVal(l, itemEndDoc)
	case '@': return emitVal(l, itemAt)
	case ':': return emitVal(l, itemColon)
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
func emitNum(l *lexer, i itemType) stateFn {
	l.backup()
	l.emit(i)
	l.eat(1)
	return lexValue
}
func lexNumber(l *lexer) stateFn {
	r := l.next()
	for unicode.IsDigit(r) || r == '.' {
		r = l.next()
	}
	switch r {
	case 'b': return emitNum(l, itemByte)
	case 's': return emitNum(l, itemShort)
	case 'l': return emitNum(l, itemLong)
	case 'f': return emitNum(l, itemFloat)
	case 'd': return emitNum(l, itemDouble)
	case 'c': return emitNum(l, itemBigDecimal)
	case 'a': return emitNum(l, itemDate)
	case 't': return emitNum(l, itemTime)
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
