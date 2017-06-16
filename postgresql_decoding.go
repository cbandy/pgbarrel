package pgbarrel

import (
	"bytes"
	"unicode"
	"unicode/utf8"
)

func pgIsIdentifier(c rune) bool {
	// https://www.postgresql.org/docs/current/static/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
	return ('0' <= c && c <= '9') || (c == '$') || (c == '_') || unicode.IsLetter(c)
}

func pgIsNotIdentifier(c rune) bool { return !pgIsIdentifier(c) }
func pgIsNotNumeric(c rune) bool    { return !pgIsNumeric(c) }

func pgIsNumeric(c rune) bool {
	// https://www.postgresql.org/docs/current/static/sql-syntax-lexical.html#SQL-SYNTAX-CONSTANTS-NUMERIC
	return ('0' <= c && c <= '9') || (c == '.') || (c == 'e') || (c == '+') || (c == '-')
}

func pgParseConstant(src []byte) (remaining, constant []byte) {
	var i int
	c, w := utf8.DecodeRune(src)

	// https://www.postgresql.org/docs/current/static/sql-syntax-lexical.html#SQL-SYNTAX-STRINGS
	if c == '\'' {
		for i < len(src) {
			// advance past quote
			i += w

			// find quote
			if n := bytes.IndexRune(src[i:], '\''); n >= 0 {
				// advance past quote
				i += n + 1
			} else {
				// lacks closing quote
				return src, nil
			}

			if i == len(src) {
				// quote was end
				return src[i:], src[:i]
			}

			c, w = utf8.DecodeRune(src[i:])
			if c != '\'' {
				// quote was closing
				return src[i:], src[:i]
			}
		}
	}

	if len(src) >= 4 &&
		(src[0] == 'n' || src[0] == 'N') &&
		(src[1] == 'u' || src[1] == 'U') &&
		(src[2] == 'l' || src[2] == 'L') &&
		(src[3] == 'l' || src[3] == 'L') {
		return src[4:], src[:4]
	}

	if i = bytes.IndexFunc(src, pgIsNotNumeric); i < 0 {
		// all numeric
		return src[:0], src
	}

	return src[i:], src[:i]
}

func pgParseIdentifier(src []byte) (remaining, identifier []byte) {
	var c rune

	for i, w := 0, 0; i < len(src); i += w {
		c, w = utf8.DecodeRune(src[i:])

		if c == '"' {
		Quoted:
			// advance past quote
			i += w

			// find quote
			if n := bytes.IndexRune(src[i:], '"'); n >= 0 {
				// advance past quote
				i += n + 1
			} else {
				// lacks closing quote
				return src, nil
			}

			if i == len(src) {
				// quote was end
				return src[i:], src[:i]
			}

			c, w = utf8.DecodeRune(src[i:])
			if c == '"' {
				// quote was escaping
				goto Quoted
			}
			if c != '.' {
				// quote was closing
				return src[i:], src[:i]
			}

			// fallthrough to next segment
		}

		if pgIsIdentifier(c) {
			i += w
			n := bytes.IndexFunc(src[i:], pgIsNotIdentifier)

			if n < 0 {
				// all identifier
				return src[:0], src
			}

			i += n
			c, w = utf8.DecodeRune(src[i:])
			if c != '.' {
				// end
				return src[i:], src[:i]
			}

			// continue to next segment
		}
	}

	return src, nil
}
