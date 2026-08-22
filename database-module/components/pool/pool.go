// Package pool caches database connections across component invocations.
// Postgres pools are keyed by DSN; Redis clients are keyed by URL.
// Each unique DSN/URL produces one shared pool that lives for the process lifetime.
package pool

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

var (
	pgPools      sync.Map // map[string]*pgxpool.Pool
	redisClients sync.Map // map[string]*redis.Client
)

// Postgres returns a cached pgx pool for the given DSN, creating one on first use.
func Postgres(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	if v, ok := pgPools.Load(dsn); ok {
		return v.(*pgxpool.Pool), nil
	}
	p, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if actual, loaded := pgPools.LoadOrStore(dsn, p); loaded {
		p.Close()
		return actual.(*pgxpool.Pool), nil
	}
	return p, nil
}

// TenantSchema derives a per-tenant Postgres schema name from a node's
// namespace + project. Deterministic, collision-resistant, and always a
// valid lowercase SQL identifier ("t_" + 16 hex chars), so it needs no
// quoting or injection-guarding. This is how the shared zero-config
// pgvector bundle isolates tenants: projects that share one database (the
// playground packs every trial into one namespace) each get their own
// schema. The value is derived entirely from the platform-injected node
// identity — a flow author cannot choose, guess, or override another
// tenant's schema. Empty identity yields "" (no scoping).
func TenantSchema(namespace, project string) string {
	if namespace == "" && project == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(namespace + "/" + project))
	return "t_" + hex.EncodeToString(sum[:8])
}

// PostgresScoped is Postgres with an optional per-tenant schema. When
// schema is non-empty it returns a separately-cached pool whose every
// connection runs `CREATE SCHEMA IF NOT EXISTS <schema>; SET search_path
// TO <schema>, public` — so all SQL (table creation, upserts, queries)
// is transparently confined to that schema while extension types like
// `vector` still resolve from public. schema=="" behaves exactly like
// Postgres, so an explicit user DSN (their own database) is never rescoped.
func PostgresScoped(ctx context.Context, dsn, schema string) (*pgxpool.Pool, error) {
	if schema == "" {
		return Postgres(ctx, dsn)
	}
	key := dsn + "\x00" + schema
	if v, ok := pgPools.Load(key); ok {
		return v.(*pgxpool.Pool), nil
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	stmt := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s; SET search_path TO %s, public", schema, schema)
	cfg.AfterConnect = func(ctx context.Context, c *pgx.Conn) error {
		_, err := c.Exec(ctx, stmt)
		return err
	}
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if actual, loaded := pgPools.LoadOrStore(key, p); loaded {
		p.Close()
		return actual.(*pgxpool.Pool), nil
	}
	return p, nil
}

// Redis returns a cached Redis client for the given URL, creating one on first use.
func Redis(url string) (*redis.Client, error) {
	if v, ok := redisClients.Load(url); ok {
		return v.(*redis.Client), nil
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	c := redis.NewClient(opts)
	if actual, loaded := redisClients.LoadOrStore(url, c); loaded {
		if closeErr := c.Close(); closeErr != nil {
			_ = closeErr
		}
		return actual.(*redis.Client), nil
	}
	return c, nil
}

// IsTransientPostgres reports whether a Postgres failure is one a backoff retry
// could clear. Only meaningful for a caller whose statement is safe to re-run —
// it answers "could this succeed later", never "is re-running it safe".
//
// A SQLSTATE reply means the server received and rejected the statement, so the
// same statement gets the same rejection — except for the classes that describe
// the server's condition rather than the query: connection exception (08),
// insufficient resources (53), shutting down or not yet accepting connections
// (57P01-03), and a serialization/deadlock abort (40001, 40P01), which is
// exactly what a caller is told to retry. Anything that is not a server reply
// at all — dial failure, dropped socket, pool acquire timeout — means the
// statement never ran.
func IsTransientPostgres(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch {
		case strings.HasPrefix(pgErr.Code, "08"), strings.HasPrefix(pgErr.Code, "53"),
			pgErr.Code == "57P01", pgErr.Code == "57P02", pgErr.Code == "57P03",
			pgErr.Code == "40001", pgErr.Code == "40P01":
			return true
		}
		return false
	}
	return true
}

// IsNeverSentPostgres reports whether the failure provably happened before the
// statement could have reached the server: the connection attempt itself
// failed (pgconn.ConnectError), or pgx guarantees the query bytes were never
// written (pgconn.SafeToRetry). Unlike IsTransientPostgres, this answers "is
// re-running it safe" too — nothing was sent, so nothing can have applied —
// which is what lets even a non-idempotent write component mark it.
func IsNeverSentPostgres(err error) bool {
	var connectErr *pgconn.ConnectError
	if errors.As(err, &connectErr) {
		return true
	}
	return pgconn.SafeToRetry(err)
}

// IsNeverSentRedis is IsNeverSentPostgres for Redis: true only when the dial
// itself failed, so the command never reached the server and a retry cannot
// double-apply. Anything past a successful dial (i/o timeout mid-command,
// dropped socket) leaves a write's outcome unknown and is not covered here.
func IsNeverSentRedis(err error) bool {
	var opErr *net.OpError
	return errors.As(err, &opErr) && opErr.Op == "dial"
}

// IsTransientRedis is IsTransientPostgres for Redis: same contract, same
// "could this succeed later, not is it safe to repeat" caveat.
//
// A redis.Error is a server reply — WRONGTYPE, NOAUTH, a malformed argument —
// which the same command reproduces forever. The exceptions are the replies
// that mean "not right now": LOADING while a replica reads its dataset,
// TRYAGAIN / CLUSTERDOWN / MASTERDOWN during a cluster reshard or failover.
// Everything else reaching us is transport (dial refused, i/o timeout).
func IsTransientRedis(err error) bool {
	var rErr redis.Error
	if errors.As(err, &rErr) {
		msg := rErr.Error()
		for _, prefix := range []string{"LOADING", "TRYAGAIN", "CLUSTERDOWN", "MASTERDOWN"} {
			if strings.HasPrefix(msg, prefix) {
				return true
			}
		}
		return false
	}
	return true
}
