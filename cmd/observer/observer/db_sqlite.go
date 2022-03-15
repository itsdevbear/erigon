package observer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/ledgerwatch/erigon/p2p/enr"
	_ "modernc.org/sqlite"
	"net"
	"net/url"
	"strings"
	"time"
)

type DBSQLite struct {
	db *sql.DB
}

// language=SQL
const (
	sqlCreateSchema = `
CREATE TABLE IF NOT EXISTS nodes (
    id TEXT PRIMARY KEY,
    ip TEXT,
    port_disc INTEGER,
    port_rlpx INTEGER,
    ip_v6 TEXT,
    ip_v6_port_disc INTEGER,
    ip_v6_port_rlpx INTEGER,
    taken_last INTEGER,
    updated INTEGER NOT NULL
)
`

	sqlUpsertNode = `
INSERT INTO nodes(
	id,
    ip,
    port_disc,
    port_rlpx,
    ip_v6,
    ip_v6_port_disc,
    ip_v6_port_rlpx,
    taken_last,
    updated
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
    ip = excluded.ip,
    port_disc = excluded.port_disc,
    port_rlpx = excluded.port_rlpx,
    ip_v6 = excluded.ip_v6,
    ip_v6_port_disc = excluded.ip_v6_port_disc,
    ip_v6_port_rlpx = excluded.ip_v6_port_rlpx,
    updated = excluded.updated
`

	sqlFindCandidates = `
SELECT * FROM nodes LIMIT ?
`

	sqlMarkTakenNodes = `
UPDATE nodes SET taken_last = ? WHERE id IN (123)
`
)

func NewDBSQLite(filePath string) (*DBSQLite, error) {
	db, err := sql.Open("sqlite", filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open DB: %w", err)
	}

	_, err = db.Exec(sqlCreateSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to create the DB schema: %w", err)
	}

	instance := DBSQLite{ db }
	return &instance, nil
}

func (db *DBSQLite) UpsertNode(ctx context.Context, node *enode.Node) error {
	if node.Incomplete() {
		return errors.New("UpsertNode can't save incomplete nodes")
	}
	id, err := nodeID(node)
	if err != nil {
		return fmt.Errorf("failed to get node ID: %w", err)
	}

	var ip *string
	var ipEntry enr.IPv4
	if node.Load(&ipEntry) == nil {
		value := net.IP(ipEntry).String()
		ip = &value
	}

	var ipV6 *string
	var ipV6Entry enr.IPv6
	if node.Load(&ipV6Entry) == nil {
		value := net.IP(ipEntry).String()
		ipV6 = &value
	}

	var portDisc *int
	var portDiscEntry enr.UDP
	if node.Load(&portDiscEntry) == nil {
		value := int(portDiscEntry)
		portDisc = &value
	}

	var ipV6PortDisc *int
	var ipV6PortDiscEntry enr.UDP6
	if node.Load(&ipV6PortDiscEntry) == nil {
		value := int(ipV6PortDiscEntry)
		ipV6PortDisc = &value
	}

	var portRLPx *int
	var portRLPxEntry enr.TCP
	if node.Load(&portRLPxEntry) == nil {
		value := int(portRLPxEntry)
		portRLPx = &value
	}

	var ipV6PortRLPx *int
	var ipV6PortRLPxEntry enr.TCP
	if node.Load(&ipV6PortRLPxEntry) == nil {
		value := int(ipV6PortRLPxEntry)
		ipV6PortRLPx = &value
	}

	var takenLast *int
	updated := time.Now().Unix()

	_, err = db.db.ExecContext(ctx, sqlUpsertNode,
		id,
		ip, portDisc, portRLPx,
		ipV6, ipV6PortDisc, ipV6PortRLPx,
		takenLast,
		updated)
	if err != nil {
		return fmt.Errorf("failed to upsert a node: %w", err)
	}
	return nil
}

func (db *DBSQLite) FindCandidates(ctx context.Context, limit uint) ([]*enode.Node, error) {
	cursor, err := db.db.QueryContext(ctx, sqlFindCandidates, limit)
	if err != nil {
		return nil, fmt.Errorf("FindCandidates failed to query candidates: %w", err)
	}
	defer func() {
		_ = cursor.Close()
	}()

	var nodes []*enode.Node
	for cursor.Next() {
		var id string
		var ip sql.NullString
		var portDisc sql.NullInt32
		var portRLPx sql.NullInt32
		var ipV6 sql.NullString
		var ipV6PortDisc sql.NullInt32
		var ipV6PortRLPx sql.NullInt32
		var takenLastTimestamp sql.NullInt64
		var updatedTimestamp int

		err := cursor.Scan(&id,
			&ip, &portDisc, &portRLPx,
			&ipV6, &ipV6PortDisc, &ipV6PortRLPx,
			&takenLastTimestamp,
			&updatedTimestamp)
		if err != nil {
			return nil, fmt.Errorf("FindCandidates failed to read candidate data: %w", err)
		}

		rec := new(enr.Record)

		nodeWithPubkey, err := enode.ParseV4("enode://" + id)
		if err != nil {
			return nil, fmt.Errorf("FindCandidates failed to decode a public key: %w", err)
		}
		rec.Set((*enode.Secp256k1)(nodeWithPubkey.Pubkey()))

		if ip.Valid {
			value := net.ParseIP(ip.String)
			if value == nil {
				return nil, errors.New("FindCandidates failed to parse IP")
			}
			rec.Set(enr.IP(value))
		}
		if ipV6.Valid {
			value := net.ParseIP(ipV6.String)
			if value == nil {
				return nil, errors.New("FindCandidates failed to parse IPv6")
			}
			rec.Set(enr.IPv6(value))
		}
		if portDisc.Valid {
			rec.Set(enr.UDP(portDisc.Int32))
		}
		if portRLPx.Valid {
			rec.Set(enr.TCP(portRLPx.Int32))
		}
		if ipV6PortDisc.Valid {
			rec.Set(enr.UDP6(ipV6PortDisc.Int32))
		}
		if ipV6PortRLPx.Valid {
			rec.Set(enr.TCP6(ipV6PortRLPx.Int32))
		}

		rec.Set(enr.ID("unsigned"))
		node, err := enode.New(enr.SchemeMap{"unsigned": noSignatureIDScheme{}}, rec)
		if err != nil {
			return nil, fmt.Errorf("FindCandidates failed to make a node: %w", err)
		}

		nodes = append(nodes, node)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("FindCandidates failed to iterate over candidates: %w", err)
	}
	return nodes, nil
}

func (db *DBSQLite) MarkTakenNodes(ctx context.Context, nodes []*enode.Node) error {
	if len(nodes) == 0 {
		return nil
	}

	takenLast := time.Now().Unix()
	ids, err := idsOfNodes(nodes)
	if err != nil {
		return fmt.Errorf("failed to get node IDs: %w", err)
	}

	idsPlaceholders := strings.TrimRight(strings.Repeat("?,", len(ids)), ",")
	query := strings.Replace(sqlMarkTakenNodes, "123", idsPlaceholders, 1)
	args := append([]interface{}{takenLast}, stringsToAny(ids)...)

	_, err = db.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to mark taken nodes: %w", err)
	}
	return nil
}

func (db *DBSQLite) TakeCandidates(ctx context.Context, limit uint) ([]*enode.Node, error) {
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("TakeCandidates failed to start transaction: %w", err)
	}

	nodes, err := db.FindCandidates(ctx, limit)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	err = db.MarkTakenNodes(ctx, nodes)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("TakeCandidates failed to commit transaction: %w", err)
	}
	return nodes, nil
}

func nodeID(node *enode.Node) (string, error) {
	nodeURL, err := url.Parse(node.URLv4())
	if err != nil {
		return "", fmt.Errorf("failed to parse node URL: %w", err)
	}
	id := nodeURL.User.Username()
	return id, nil
}

type noSignatureIDScheme struct {
	enode.V4ID
}

func (noSignatureIDScheme) Verify(_ *enr.Record, _ []byte) error {
	return nil
}

func stringsToAny(strValues []string) []interface{} {
	values := make([]interface{}, 0, len(strValues))
	for _, value := range strValues {
		values = append(values, value)
	}
	return values
}

func idsOfNodes(nodes []*enode.Node) ([]string, error) {
	ids := make([]string, 0, len(nodes))
	for _, node := range nodes {
		id, err := nodeID(node)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}
