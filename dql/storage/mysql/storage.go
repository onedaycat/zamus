package mysql

import (
	"context"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/onedaycat/zamus/dql"
	"github.com/onedaycat/zamus/errors"
)

type dqlMySQL struct {
	db *sqlx.DB
}

func New(db *sqlx.DB) *dqlMySQL {
	return &dqlMySQL{db}
}

func (d *dqlMySQL) Truncate() {
	d.db.Exec(`TRUNCATE dql`)
}

func (d *dqlMySQL) MultiSave(ctx context.Context, msgs dql.DQLMsgs) error {
	query := "INSERT IGNORE INTO dql (id,service,version,timeSeq,lambdaFunction,eventType,aggregateID,eventID,seq,time,dqlTime,eventMsg,error) VALUES "
	data := make([]interface{}, 0, len(msgs)*11)

	for _, msg := range msgs {
		query += "(?,?,?,?,?,?,?,?,?,?,?,?,?),"
		data = append(data, msg.ID, msg.Service, msg.Version, msg.LambdaFunction, msg.EventType, msg.AggregateID, msg.EventID, msg.Seq, msg.Time, msg.DQLTime, msg.EventMsg, msg.Error)
	}

	query = query[0 : len(query)-1]

	if _, err := d.db.ExecContext(ctx, query, data...); err != nil {
		return errors.ErrUnbleSaveDQLMessages.WithCaller().WithCause(err)
	}

	return nil
}
