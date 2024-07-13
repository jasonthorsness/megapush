package main

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"
)

func queryCount(db *sql.DB, table string) (int64, error) {
	count, err := selectSingleValue[int64](db, "SELECT COUNT(*) FROM "+table)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func execDropAndCreateTable(db *sql.DB, table string) error {
	query := `DROP TABLE IF EXISTS ` + table
	_, err := db.Exec(query)
	if err != nil {
		return err
	}
	query = `CREATE TABLE ` + table + `(
    documentID BIGINT NOT NULL,
    payload LONGBLOB NOT NULL,
    SORT KEY(documentID),
    SHARD KEY(documentID))`
	_, err = db.Exec(query)
	if err != nil {
		return err
	}
	return nil
}

func execLoadDataLocalInfile(db *sql.DB, readerName string, table string) (int64, error) {
	query := "LOAD DATA LOCAL INFILE 'Reader::" + readerName + "' INTO TABLE " + table
	res, err := db.Exec(query)
	if err != nil {
		return 0, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return rows, nil
}

func execCreatePipeline(db *sql.DB, table string, bucket string, region string, keyID string, secretKey string, sessionToken string) error {
	query := "CREATE PIPELINE " + table +
		" AS LOAD DATA S3 '" + bucket + "'" +
		" CONFIG '{\"region\":\"" + region + "\"}'" +
		" CREDENTIALS '" +
		`{` +
		`"aws_access_key_id": "` + keyID + "\",\n" +
		`"aws_secret_access_key": "` + secretKey + "\",\n" +
		`"aws_session_token": "` + sessionToken + "\"\n" +
		`}` +
		"' INTO TABLE " + table
	_, err := db.Exec(query)
	if err != nil {
		return err
	}
	return nil
}

func execStartPipeline(db *sql.DB, name string) error {
	query := "START PIPELINE " + name
	_, err := db.Exec(query)
	if err != nil {
		return err
	}
	return nil
}

func execStopPipeline(db *sql.DB, name string) error {
	query := "STOP PIPELINE " + name
	_, err := db.Exec(query)
	if err != nil {
		return err
	}
	return nil
}

func execDropPipeline(db *sql.DB, name string) error {
	query := "DROP PIPELINE " + name
	_, err := db.Exec(query)
	if err != nil {
		return err
	}
	return nil
}

func deleteCompletedBatchesUntilNoneLeftAndStopped(ctx context.Context, db *sql.DB, database string, table string, s3 *s3Connection) error {
	cursor := int64(0)
	for {
		time.Sleep(10 * time.Second)
		values, err := selectManyValues[int64](
			db,
			"SELECT BATCH_SOURCE_PARTITION_ID FROM INFORMATION_SCHEMA.pipelines_batches WHERE"+
				" DATABASE_NAME = '"+database+"'"+
				" AND PIPELINE_NAME = '"+table+"'"+
				" AND BATCH_STATE = 'SUCCEEDED' AND BATCH_SOURCE_PARTITION_ID:>BIGINT > "+
				strconv.FormatInt(cursor, 10)+" ORDER BY BATCH_SOURCE_PARTITION_ID:>BIGINT")
		if err != nil {
			return err
		}
		for _, value := range values {
			if value == cursor+1 {
				cursor = value
				err = s3.delete(ctx, value)
				if err != nil {
					return err
				}
			}
		}
		if len(values) == 0 {
			running, err := selectSingleValue[int64](db,
				"SELECT COUNT(*) FROM INFORMATION_SCHEMA.pipelines WHERE STATE = 'Running' AND PIPELINE_NAME = '"+table+"'")
			if err != nil {
				return err
			}
			if running == 0 {
				break
			}
		}
	}
	return nil
}

func selectSingleValue[T any](db *sql.DB, query string) (T, error) {
	var value T
	rows, err := db.Query(query)
	if err != nil {
		return value, err
	}

	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			panic(err)
		}
	}(rows)

	if !rows.Next() {
		return value, fmt.Errorf("no rows")
	}

	err = rows.Scan(&value)
	if err != nil {
		return value, err
	}

	return value, nil
}

func selectManyValues[T any](db *sql.DB, query string) ([]T, error) {
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}

	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			panic(err)
		}
	}(rows)

	var value T
	var values []T
	for rows.Next() {
		err = rows.Scan(&value)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return values, nil
}
