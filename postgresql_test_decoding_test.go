package pgbarrel

import (
	"reflect"
	"testing"
)

func TestPostgreSQLTestDecodingParse(t *testing.T) {
	for _, tt := range []struct {
		message  string
		expected ReplicationOperation
	}{
		// Transaction
		{`BEGIN 553`, ReplicationOperation{
			Operation: `BEGIN`,
			Target:    `553`,
		}},
		{`COMMIT 553`, ReplicationOperation{
			Operation: `COMMIT`,
			Target:    `553`,
		}},

		// Insert unary ID
		{`table public.contents: INSERT: id[integer]:1 value[text]:'a'`, ReplicationOperation{
			Operation:  `INSERT`,
			Target:     `public.contents`,
			NewColumns: []string{`id`, `value`},
			NewValues:  []string{`1`, `'a'`},
		}},
		{`table public."from": INSERT: id[integer]:2 value[text]:'b'`, ReplicationOperation{
			Operation:  `INSERT`,
			Target:     `public."from"`,
			NewColumns: []string{`id`, `value`},
			NewValues:  []string{`2`, `'b'`},
		}},

		// Insert binary ID
		{`table public."sp ace": INSERT: id1[integer]:3 id2[integer]:92 value[text]:'c'`, ReplicationOperation{
			Operation:  `INSERT`,
			Target:     `public."sp ace"`,
			NewColumns: []string{`id1`, `id2`, `value`},
			NewValues:  []string{`3`, `92`, `'c'`},
		}},

		// Update unary ID
		{`table public.contents: UPDATE: old-key: id[integer]:1 new-tuple: id[integer]:11 value[text]:'m'`, ReplicationOperation{
			Operation:  `UPDATE`,
			Target:     `public.contents`,
			OldColumns: []string{`id`},
			OldValues:  []string{`1`},
			NewColumns: []string{`id`, `value`},
			NewValues:  []string{`11`, `'m'`},
		}},
		{`table public."from": UPDATE: old-key: id[integer]:2 new-tuple: id[integer]:12 value[text]:'b'`, ReplicationOperation{
			Operation:  `UPDATE`,
			Target:     `public."from"`,
			OldColumns: []string{`id`},
			OldValues:  []string{`2`},
			NewColumns: []string{`id`, `value`},
			NewValues:  []string{`12`, `'b'`},
		}},

		// Update binary ID
		{`table public."sp ace": UPDATE: old-key: id1[integer]:3 id2[integer]:92 new-tuple: id1[integer]:13 id2[integer]:92 value[text]:'c'`, ReplicationOperation{
			Operation:  `UPDATE`,
			Target:     `public."sp ace"`,
			OldColumns: []string{`id1`, `id2`},
			OldValues:  []string{`3`, `92`},
			NewColumns: []string{`id1`, `id2`, `value`},
			NewValues:  []string{`13`, `92`, `'c'`},
		}},

		// Delete unary ID
		{`table public.contents: DELETE: id[integer]:1`, ReplicationOperation{
			Operation:  `DELETE`,
			Target:     `public.contents`,
			OldColumns: []string{`id`},
			OldValues:  []string{`1`},
		}},
		{`table public."from": DELETE: id[integer]:2`, ReplicationOperation{
			Operation:  `DELETE`,
			Target:     `public."from"`,
			OldColumns: []string{`id`},
			OldValues:  []string{`2`},
		}},

		// Delete binary ID
		{`table public."sp ace": DELETE: id1[integer]:3 id2[integer]:92`, ReplicationOperation{
			Operation:  `DELETE`,
			Target:     `public."sp ace"`,
			OldColumns: []string{`id1`, `id2`},
			OldValues:  []string{`3`, `92`},
		}},

		// Escaping
		{`table "from"." : DELETE: ": INSERT: " key[] "[integer]:5 arr[integer[]]:'{1,2,3}'`, ReplicationOperation{
			Operation:  `INSERT`,
			Target:     `"from"." : DELETE: "`,
			NewColumns: []string{`" key[] "`, `arr`},
			NewValues:  []string{`5`, `'{1,2,3}'`},
		}},
		{`table "from"." tbl[] ": INSERT: " : DELETE: "[integer]:5 arr[integer[]]:'{1,2,3}'`, ReplicationOperation{
			Operation:  `INSERT`,
			Target:     `"from"." tbl[] "`,
			NewColumns: []string{`" : DELETE: "`, `arr`},
			NewValues:  []string{`5`, `'{1,2,3}'`},
		}},
	} {
		var result ReplicationOperation

		if err := new(pgTestDecoding).Parse([]byte(tt.message), &result); err != nil {
			t.Fatalf("Got %q for zero `%s`", err, tt.message)
		}

		if !reflect.DeepEqual(result, tt.expected) {
			t.Errorf("Expected zero `%s` to be %v, got %v", tt.message, tt.expected, result)
		}

		result = ReplicationOperation{
			OldColumns: make([]string, 5),
			OldValues:  make([]string, 5),
			NewColumns: make([]string, 5),
			NewValues:  make([]string, 5),
		}

		if err := new(pgTestDecoding).Parse([]byte(tt.message), &result); err != nil {
			t.Fatalf("Got %q for initialized `%s`", err, tt.message)
		}

		if len(result.OldColumns) == 0 {
			result.OldColumns = nil
		}
		if len(result.OldValues) == 0 {
			result.OldValues = nil
		}
		if len(result.NewColumns) == 0 {
			result.NewColumns = nil
		}
		if len(result.NewValues) == 0 {
			result.NewValues = nil
		}

		if !reflect.DeepEqual(result, tt.expected) {
			t.Errorf("Expected initialized `%s` to be %v, got %v", tt.message, tt.expected, result)
		}
	}
}
