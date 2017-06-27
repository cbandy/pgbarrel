package pgbarrel

import (
	"context"
	"time"

	"github.com/jackc/pgx"
	"github.com/pkg/errors"
)

type pgReplicationConfig struct {
	Slot, Plugin, Options string
}

type pgLogicalReceiver struct {
	conn    *pgx.ReplicationConn
	connCfg pgx.ConnConfig
	decode  pgDecode
	replCfg pgReplicationConfig

	posReceived uint64
	posApplied  uint64
}

func NewPostgreSQLReceiver(conn string, slot, plugin, options string) (*pgLogicalReceiver, error) {
	var (
		recv pgLogicalReceiver
		ok   bool
		err  error
	)

	if recv.connCfg, err = pgx.ParseConnectionString(conn); err != nil {
		return nil, err
	}

	if recv.conn, err = pgx.ReplicationConnect(recv.connCfg); err != nil {
		return nil, err
	}

	if recv.decode, ok = pgDecoders[plugin]; !ok {
		return nil, errors.Errorf("Unknown PostgreSQL logical decoding output plugin: %q", plugin)
	}

	recv.replCfg.Slot = slot
	recv.replCfg.Plugin = plugin
	recv.replCfg.Options = options

	return &recv, nil
}

func (r *pgLogicalReceiver) Close() error {
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}

func (r *pgLogicalReceiver) Start(ctx context.Context, out chan<- *ReplicationOperation) error {
	err := r.conn.StartReplication(r.replCfg.Slot, r.posReceived, -1, r.replCfg.Options)

	const standby_timeout = 10 * time.Second
	var standby_deadline = time.Now().Add(standby_timeout)
	var standby_expired = time.Now().Add(-standby_timeout)

	var message *pgx.ReplicationMessage
	var standby *pgx.StandbyStatus

	for err == nil {
		message, err = r.conn.WaitForReplicationMessage(time.Second)

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err == nil && message != nil {
			if message.ServerHeartbeat != nil && message.ServerHeartbeat.ReplyRequested != 0 {
				standby_deadline = standby_expired
			}

			if message.WalMessage != nil {
				r.posReceived = message.WalMessage.WalStart

				op := ReplicationOperation{
					Position: pgx.FormatLSN(message.WalMessage.WalStart),
				}

				if err = r.decode(message.WalMessage.WalData, &op); err == nil {
					out <- &op
				}
			}
		}

		if err == nil || err == pgx.ErrNotificationTimeout {
			err = nil

			if time.Now().After(standby_deadline) {
				if standby, err = pgx.NewStandbyStatus(r.posApplied); err == nil {
					if err = r.conn.SendStandbyStatus(standby); err == nil {
						standby_deadline = time.Now().Add(standby_timeout)
					}
				}
			}
		}
	}

	return err
}
