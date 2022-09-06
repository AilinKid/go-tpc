package ch

import (
	"context"
	"fmt"
)

var allTables []string

func init() {
	allTables = []string{"customer", "district", "history", "item", "new_order", "order_line", "orders", "region", "warehouse",
		"nation", "stock", "supplier"}
}

func (w *Workloader) createTableDDL(ctx context.Context, query string, tableName string, action string) error {
	s := w.getState(ctx)
	fmt.Printf("%s %s\n", action, tableName)
	if _, err := s.Conn.ExecContext(ctx, query); err != nil {
		return err
	}
	return nil
}

// createTables creates tables schema.
func (w *Workloader) createTables(ctx context.Context) error {
	query := `
CREATE TABLE IF NOT EXISTS nation (
    N_NATIONKEY BIGINT NULL,
    N_NAME CHAR(25) NULL,
    N_REGIONKEY BIGINT NULL,
    N_COMMENT VARCHAR(152),
    UNIQUE KEY (N_NATIONKEY)
)`

	if err := w.createTableDDL(ctx, query, "nation", "creating"); err != nil {
		return err
	}

	query = `
CREATE TABLE IF NOT EXISTS region (
    R_REGIONKEY BIGINT NULL,
    R_NAME CHAR(25) NULL,
    R_COMMENT VARCHAR(152),
    UNIQUE KEY (R_REGIONKEY)
)`
	if err := w.createTableDDL(ctx, query, "region", "creating"); err != nil {
		return err
	}

	query = `
CREATE TABLE IF NOT EXISTS supplier (
    S_SUPPKEY BIGINT NULL,
    S_NAME CHAR(25) NULL,
    S_ADDRESS VARCHAR(40) NULL,
    S_NATIONKEY BIGINT NULL,
    S_PHONE CHAR(15) NULL,
    S_ACCTBAL DECIMAL(15, 2) NULL,
    S_COMMENT VARCHAR(101) NULL,
    UNIQUE KEY (S_SUPPKEY)
)`
	if err := w.createTableDDL(ctx, query, "supplier", "creating"); err != nil {
		return err
	}

	return nil
}
