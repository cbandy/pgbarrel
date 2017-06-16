package pgbarrel

import "testing"

func TestPostgreSQLParseConstant(t *testing.T) {
	for _, tt := range []struct{ input, remaining, constant string }{
		{`null`, ``, `null`},
		{`NULL`, ``, `NULL`},

		{`1`, ``, `1`},
		{`12.34`, ``, `12.34`},
		{`.001`, ``, `.001`},
		{`5e2`, ``, `5e2`},
		{`1.925e-3`, ``, `1.925e-3`},

		{`'a'`, ``, `'a'`},
		{`'abc'`, ``, `'abc'`},
		{`'a''bc'`, ``, `'a''bc'`},

		{`null `, ` `, `null`},
		{`NULL `, ` `, `NULL`},

		{`1 `, ` `, `1`},
		{`12.34 `, ` `, `12.34`},
		{`.001 `, ` `, `.001`},
		{`5e2  `, `  `, `5e2`},
		{`1.925e-3 `, ` `, `1.925e-3`},

		{`'a' `, ` `, `'a'`},
		{`'abc'  `, `  `, `'abc'`},
		{`'a''bc'  `, `  `, `'a''bc'`},
	} {
		r, c := pgParseConstant([]byte(tt.input))

		if string(r) != tt.remaining {
			t.Errorf("Expected `%s` to leave `%s`, got `%s`", tt.input, tt.remaining, r)
		}
		if string(c) != tt.constant {
			t.Errorf("Expected `%s` to be `%s`, got `%s`", tt.input, tt.constant, c)
		}
	}
}

func TestPostgreSQLParseConstantError(t *testing.T) {
	for _, tt := range []string{
		``,
		`[`,
	} {
		r, c := pgParseConstant([]byte(tt))

		if string(r) != tt {
			t.Errorf("Expected `%s` to remain unparsed, got `%s`", tt, r)
		}
		if c != nil {
			t.Errorf("Expected `%s` to produce nil, got `%s`", tt, c)
		}
	}
}

func TestPostgreSQLParseIdentifier(t *testing.T) {
	for _, tt := range []struct{ input, remaining, identifier string }{
		{`a`, ``, `a`},
		{`abc`, ``, `abc`},
		{`a.b.c`, ``, `a.b.c`},
		{`"a"`, ``, `"a"`},
		{`"abc"`, ``, `"abc"`},
		{`"a bc"`, ``, `"a bc"`},
		{`"a""bc"`, ``, `"a""bc"`},
		{`"a".bc`, ``, `"a".bc`},
		{`a."bc"`, ``, `a."bc"`},
		{`"a"."bc"`, ``, `"a"."bc"`},

		{`a `, ` `, `a`},
		{`abc: `, `: `, `abc`},
		{`a.b.c[`, `[`, `a.b.c`},
		{`"a" `, ` `, `"a"`},
		{`"abc" :`, ` :`, `"abc"`},
		{`"a bc"x`, `x`, `"a bc"`},
		{`"a""bc"x`, `x`, `"a""bc"`},
		{`"a".bc[`, `[`, `"a".bc`},
		{`a."bc"x`, `x`, `a."bc"`},
		{`"a"."bc"x`, `x`, `"a"."bc"`},
	} {
		r, i := pgParseIdentifier([]byte(tt.input))

		if string(r) != tt.remaining {
			t.Errorf("Expected `%s` to leave `%s`, got `%s`", tt.input, tt.remaining, r)
		}
		if string(i) != tt.identifier {
			t.Errorf("Expected `%s` to be `%s`, got `%s`", tt.input, tt.identifier, i)
		}
	}
}

func TestPostgreSQLParseIdentifierError(t *testing.T) {
	for _, tt := range []string{
		``,
		`[`,
	} {
		r, i := pgParseIdentifier([]byte(tt))

		if string(r) != tt {
			t.Errorf("Expected `%s` to remain unparsed, got `%s`", tt, r)
		}
		if i != nil {
			t.Errorf("Expected `%s` to produce nil, got `%s`", tt, i)
		}
	}
}
