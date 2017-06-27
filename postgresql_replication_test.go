package pgbarrel

import (
	"context"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type pgserver struct {
	directory string
}

func (s *pgserver) mustConnect(t *testing.T, db string) *pgx.Conn {
	c, err := pgx.Connect(pgx.ConnConfig{Host: s.directory, Database: db})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func (s *pgserver) mustReplConnect(t *testing.T, db string) *pgx.ReplicationConn {
	rc, err := pgx.ReplicationConnect(pgx.ConnConfig{Host: s.directory, Database: db})
	if err != nil {
		t.Fatal(err)
	}
	return rc
}

func (*pgserver) mustExec(t *testing.T, cmd string, arg ...string) {
	out, err := exec.Command(cmd, arg...).CombinedOutput()

	if err != nil {
		t.Log("\n" + string(out))
		t.Fatal(err)
	}
}

func (s *pgserver) start(t *testing.T) {
	var err error

	if s.directory, err = ioutil.TempDir("", "pgbarrel-postgresql"); err != nil {
		t.Fatal(err)
	}

	s.mustExec(t, "pg_ctl",
		"initdb", "-D", s.directory, "-s", "-o", "--auth=trust")

	s.mustExec(t, "cp", "--force", "--target-directory", s.directory, "test/postgresql.conf", "test/pg_hba.conf")

	s.mustExec(t, "pg_ctl",
		"start", "-D", s.directory, "-s", "-w", "-l", filepath.Join(s.directory, "server.log"), "-o", "-k "+s.directory)
}

func (s *pgserver) stop(t *testing.T) {
	if len(s.directory) > 0 {
		defer os.RemoveAll(s.directory)
	}

	s.mustExec(t, "pg_ctl",
		"stop", "-D", s.directory, "-s", "-m", "fast")
}

func TestPostgreSQLReceiver(t *testing.T) {
	s := new(pgserver)
	s.start(t)
	defer s.stop(t)

	exec := func(c *pgx.Conn, sql string, args ...interface{}) error {
		_, err := c.Exec(sql, args...)
		return err
	}

	func() {
		c := s.mustConnect(t, "postgres")
		defer c.Close()
		assert.NoError(t, exec(c, `CREATE DATABASE pgbarrel`))
	}()

	func() {
		c := s.mustConnect(t, "pgbarrel")
		defer c.Close()
		assert.NoError(t, exec(c, `CREATE TABLE normal (id int PRIMARY KEY, value text)`))
		assert.NoError(t, exec(c, `CREATE TABLE compound (id1 int, id2 int, value text, PRIMARY KEY (id1, id2))`))
		assert.NoError(t, exec(c, `CREATE SCHEMA "from"`))
		assert.NoError(t, exec(c, `CREATE TABLE "from"."ta""ble" (" key " int PRIMARY KEY, arr int[])`))
		assert.NoError(t, exec(c, `CREATE TABLE "from"."wild" (" key[] " int PRIMARY KEY, pt point)`))
		assert.NoError(t, exec(c, `SELECT pg_create_logical_replication_slot($1, $2)`, "pgbarrel_test", "test_decoding"))
	}()

	r, err := NewPostgreSQLReceiver("host="+s.directory+" dbname=pgbarrel", "pgbarrel_test", "test_decoding", "")
	assert.NoError(t, err)
	defer r.Close()

	c := s.mustConnect(t, "pgbarrel")
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	activity := make(chan error)
	go func(out chan<- error) {
		defer close(out)
		for _, sql := range []string{
			`INSERT INTO normal (id, value) VALUES (1, 'a')`,
			`INSERT INTO compound (id1, id2, value) VALUES (2, 91, Null), (3, 92, 'c')`,
			`INSERT INTO "from"."ta""ble" (" key ", arr) VALUES (5, '{1,2,3}')`,
			`INSERT INTO "from"."wild" (" key[] ", pt) VALUES (6, '(1,2)')`,

			`UPDATE normal SET (value) = ('m') WHERE id = 1`,
			`UPDATE normal SET (id, value) = (11, 'n') WHERE id = 1`,
			`UPDATE compound SET (id1, value) = (11, 'd') WHERE id1 = 2`,
			`UPDATE compound SET (id1, value) = (id1 + 10, value) WHERE value < 'j'`,

			`DELETE FROM normal WHERE value = 'n'`,
			`DELETE FROM compound`,
			`DELETE FROM "from"."ta""ble" WHERE " key " = 5`,
		} {
			if _, err := c.Exec(sql); err != nil {
				out <- err
				break
			}
		}
	}(activity)

	ops := make(chan *ReplicationOperation, 100)
	if err = r.Start(ctx, ops); err != context.DeadlineExceeded {
		t.Fatal(err)
	}
	assert.NoError(t, <-activity)
	close(ops)

	var (
		begin  = func(op *ReplicationOperation) { assert.Equal(t, "BEGIN", op.Operation) }
		commit = func(op *ReplicationOperation) { assert.Equal(t, "COMMIT", op.Operation) }

		equal = func(expected ReplicationOperation) func(*ReplicationOperation) {
			return func(op *ReplicationOperation) {
				assert.Equal(t, expected.Operation, op.Operation)
				assert.Equal(t, expected.Target, op.Target)

				if n := len(expected.OldColumns); n != 0 || len(op.OldColumns) != n {
					assert.Equal(t, expected.OldColumns, op.OldColumns)
				}
				if n := len(expected.OldValues); n != 0 || len(op.OldValues) != n {
					assert.Equal(t, expected.OldValues, op.OldValues)
				}
				if n := len(expected.NewColumns); n != 0 || len(op.NewColumns) != n {
					assert.Equal(t, expected.NewColumns, op.NewColumns)
				}
				if n := len(expected.NewValues); n != 0 || len(op.NewValues) != n {
					assert.Equal(t, expected.NewValues, op.NewValues)
				}
			}
		}
	)

	for i, compare := range []func(op *ReplicationOperation){
		begin,
		equal(ReplicationOperation{
			Operation: "INSERT", Target: "public.normal",
			NewColumns: []string{"id", "value"},
			NewValues:  []string{"1", "'a'"},
		}),
		commit,

		begin,
		equal(ReplicationOperation{
			Operation: "INSERT", Target: "public.compound",
			NewColumns: []string{"id1", "id2", "value"},
			NewValues:  []string{"2", "91", "null"},
		}),
		equal(ReplicationOperation{
			Operation: "INSERT", Target: "public.compound",
			NewColumns: []string{"id1", "id2", "value"},
			NewValues:  []string{"3", "92", "'c'"},
		}),
		commit,

		begin,
		equal(ReplicationOperation{
			Operation: "INSERT", Target: `"from"."ta""ble"`,
			NewColumns: []string{`" key "`, "arr"},
			NewValues:  []string{"5", "'{1,2,3}'"},
		}),
		commit,

		begin,
		equal(ReplicationOperation{
			Operation: "INSERT", Target: `"from".wild`,
			NewColumns: []string{`" key[] "`, "pt"},
			NewValues:  []string{"6", "'(1,2)'"},
		}),
		commit,

		begin,
		equal(ReplicationOperation{
			Operation: "UPDATE", Target: "public.normal",
			NewColumns: []string{"id", "value"},
			NewValues:  []string{"1", "'m'"},
		}),
		commit,

		begin,
		equal(ReplicationOperation{
			Operation: "UPDATE", Target: "public.normal",
			OldColumns: []string{"id"},
			OldValues:  []string{"1"},
			NewColumns: []string{"id", "value"},
			NewValues:  []string{"11", "'n'"},
		}),
		commit,

		begin,
		equal(ReplicationOperation{
			Operation: "UPDATE", Target: "public.compound",
			OldColumns: []string{"id1", "id2"},
			OldValues:  []string{"2", "91"},
			NewColumns: []string{"id1", "id2", "value"},
			NewValues:  []string{"11", "91", "'d'"},
		}),
		commit,

		begin,
		equal(ReplicationOperation{
			Operation: "UPDATE", Target: "public.compound",
			OldColumns: []string{"id1", "id2"},
			OldValues:  []string{"3", "92"},
			NewColumns: []string{"id1", "id2", "value"},
			NewValues:  []string{"13", "92", "'c'"},
		}),
		equal(ReplicationOperation{
			Operation: "UPDATE", Target: "public.compound",
			OldColumns: []string{"id1", "id2"},
			OldValues:  []string{"11", "91"},
			NewColumns: []string{"id1", "id2", "value"},
			NewValues:  []string{"21", "91", "'d'"},
		}),
		commit,

		begin,
		equal(ReplicationOperation{
			Operation: "DELETE", Target: "public.normal",
			OldColumns: []string{"id"},
			OldValues:  []string{"11"},
		}),
		commit,

		begin,
		equal(ReplicationOperation{
			Operation: "DELETE", Target: "public.compound",
			OldColumns: []string{"id1", "id2"},
			OldValues:  []string{"13", "92"},
		}),
		equal(ReplicationOperation{
			Operation: "DELETE", Target: "public.compound",
			OldColumns: []string{"id1", "id2"},
			OldValues:  []string{"21", "91"},
		}),
		commit,

		begin,
		equal(ReplicationOperation{
			Operation: "DELETE", Target: `"from"."ta""ble"`,
			OldColumns: []string{`" key "`},
			OldValues:  []string{"5"},
		}),
		commit,
	} {
		op := <-ops
		require.NotNilf(t, op, "Got only %v operations", i)
		compare(op)
	}
	assert.Emptyf(t, ops, "%v more operations than expected", len(ops))
	for op := range ops {
		t.Log(*op)
	}
}
