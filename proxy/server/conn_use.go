package server

import (
	"fmt"

	"github.com/flike/kingshard/sqlparser"
)

func (c *ClientConn) handleUseDB(stmt *sqlparser.UseDB) error {
	if len(stmt.DB) == 0 {
		return fmt.Errorf("must have database, not %s", sqlparser.String(stmt))
	}
	fmt.Printf("999999999:db=%s\n", stmt.DB)
	c.db = string(stmt.DB)
	return c.writeOK(nil)
}
