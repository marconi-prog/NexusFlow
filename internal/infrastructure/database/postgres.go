// Package database implementa ports.Store sobre PostgreSQL, usando LISTEN/NOTIFY para
// propagar eventos entre as instâncias da API e dos workers. Os scripts SQL ficam em
// /migrations na raiz do repositório.
package database

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/marconi-prog/nexusflow/internal/application/ports"
	"github.com/marconi-prog/nexusflow/internal/domain"
	"github.com/marconi-prog/nexusflow/migrations"
)

var migrationsFS = migrations.FS

const notifyChannel = "nexusflow_events"

// limite de payload do NOTIFY é 8000 bytes; acima disso enviamos apenas o id.
const maxNotifyPayload = 7500

var _ ports.Store = (*Store)(nil)

type Store struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

func New(ctx context.Context, url string, maxConns int32, logger *slog.Logger) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("DATABASE_URL inválido: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("falha ao conectar no postgres: %w", err)
	}
	s := &Store{pool: pool, logger: logger}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

// migrate aplica migrações pendentes sob advisory lock (seguro com várias réplicas subindo juntas).
func (s *Store) migrate(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	const lockID = 7_420_133_001
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", lockID); err != nil {
		return err
	}
	defer conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", lockID) //nolint:errcheck

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		return err
	}

	files, err := fs.Glob(migrationsFS, "*.sql")
	if err != nil {
		return err
	}
	sort.Strings(files)
	for _, f := range files {
		version := strings.TrimSuffix(f, ".sql")
		var exists bool
		if err := conn.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=$1)", version).Scan(&exists); err != nil {
			return err
		}
		if exists {
			continue
		}
		sqlBytes, err := migrationsFS.ReadFile(f)
		if err != nil {
			return err
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, string(sqlBytes)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("migração %s: %w", version, err)
		}
		if _, err := tx.Exec(ctx, "INSERT INTO schema_migrations(version) VALUES ($1)", version); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		s.logger.Info("migração aplicada", "version", version)
	}
	return nil
}

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }
func (s *Store) Close()                         { s.pool.Close() }

// ---------- eventos ----------

func (s *Store) AppendEvent(ctx context.Context, e *ports.Event) error {
	if e.OccurredAt.IsZero() {
		e.OccurredAt = time.Now().UTC()
	}
	var data []byte
	if len(e.Data) > 0 {
		b, err := json.Marshal(e.Data)
		if err != nil {
			return err
		}
		data = b
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `INSERT INTO events
			(type, source, actor, occurred_at, process_instance_key, process_definition_id, job_key, job_type, incident_key, duration_ms, data)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING id`,
			e.Type, e.Source, e.Actor, e.OccurredAt, e.ProcessInstanceKey, e.ProcessDefinitionID,
			e.JobKey, e.JobType, e.IncidentKey, e.DurationMs, data,
		).Scan(&e.ID)
		if err != nil {
			return err
		}
		payload, _ := json.Marshal(e)
		if len(payload) > maxNotifyPayload {
			payload = fmt.Appendf(nil, `{"id":%d,"ref":true}`, e.ID)
		}
		_, err = tx.Exec(ctx, "SELECT pg_notify($1, $2)", notifyChannel, string(payload))
		return err
	})
}

const eventColumns = `id, type, source, actor, occurred_at, process_instance_key, process_definition_id, job_key, job_type, incident_key, duration_ms, data`

func scanEvent(row pgx.Row) (ports.Event, error) {
	var e ports.Event
	var data []byte
	err := row.Scan(&e.ID, &e.Type, &e.Source, &e.Actor, &e.OccurredAt, &e.ProcessInstanceKey,
		&e.ProcessDefinitionID, &e.JobKey, &e.JobType, &e.IncidentKey, &e.DurationMs, &data)
	if err != nil {
		return e, err
	}
	if len(data) > 0 {
		_ = json.Unmarshal(data, &e.Data)
	}
	e.OccurredAt = e.OccurredAt.UTC()
	return e, nil
}

func (s *Store) ListEvents(ctx context.Context, f ports.EventFilter, p domain.Page) ([]ports.Event, error) {
	p = p.Normalize()
	var (
		where []string
		args  []any
	)
	add := func(cond string, v any) {
		args = append(args, v)
		where = append(where, fmt.Sprintf(cond, len(args)))
	}
	if len(f.Types) > 0 {
		add("type = ANY($%d)", f.Types)
	}
	if f.ProcessInstanceKey != "" {
		add("process_instance_key = $%d", f.ProcessInstanceKey)
	}
	if f.ProcessDefinitionID != "" {
		add("process_definition_id = $%d", f.ProcessDefinitionID)
	}
	if f.JobType != "" {
		add("job_type = $%d", f.JobType)
	}
	if f.Since != nil {
		add("occurred_at >= $%d", *f.Since)
	}
	if f.Until != nil {
		add("occurred_at <= $%d", *f.Until)
	}
	order := "id DESC"
	if f.AfterID > 0 {
		add("id > $%d", f.AfterID)
		order = "id ASC"
	}
	q := "SELECT " + eventColumns + " FROM events"
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	args = append(args, p.Limit, p.From)
	q += fmt.Sprintf(" ORDER BY %s LIMIT $%d OFFSET $%d", order, len(args)-1, len(args))

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []ports.Event{}
	for rows.Next() {
		e, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) CountEvents(ctx context.Context, since time.Time) ([]ports.EventCount, error) {
	rows, err := s.pool.Query(ctx, `SELECT type, count(*) FROM events WHERE occurred_at >= $1 GROUP BY type ORDER BY type`, since)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (ports.EventCount, error) {
		var c ports.EventCount
		err := r.Scan(&c.Type, &c.Count)
		return c, err
	})
}

func (s *Store) JobStats(ctx context.Context, since time.Time) ([]ports.JobStat, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT job_type,
		       count(*) FILTER (WHERE type = $2),
		       count(*) FILTER (WHERE type = $3),
		       count(*) FILTER (WHERE type = $4),
		       coalesce(avg(duration_ms) FILTER (WHERE type = $2), 0)::float8,
		       coalesce(percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms) FILTER (WHERE type = $2), 0)::float8
		FROM events
		WHERE job_type <> '' AND occurred_at >= $1
		GROUP BY job_type ORDER BY job_type`,
		since, ports.EventJobCompleted, ports.EventJobFailed, ports.EventJobErrorThrown)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (ports.JobStat, error) {
		var j ports.JobStat
		err := r.Scan(&j.JobType, &j.Completed, &j.Failed, &j.ErrorsThrown, &j.AvgDurationMs, &j.P95DurationMs)
		return j, err
	})
}

func (s *Store) PurgeEvents(ctx context.Context, before time.Time) (int64, error) {
	tag, err := s.pool.Exec(ctx, "DELETE FROM events WHERE occurred_at < $1", before)
	return tag.RowsAffected(), err
}

// Listen mantém uma conexão dedicada em LISTEN, reconectando com backoff em caso de falha.
func (s *Store) Listen(ctx context.Context, fn func(ports.Event)) error {
	backoff := time.Second
	for {
		err := s.listenOnce(ctx, fn)
		if ctx.Err() != nil {
			return nil
		}
		s.logger.Warn("listener do postgres caiu; reconectando", "error", err, "backoff", backoff)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 30*time.Second)
	}
}

func (s *Store) listenOnce(ctx context.Context, fn func(ports.Event)) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "LISTEN "+notifyChannel); err != nil {
		return err
	}
	defer conn.Exec(context.Background(), "UNLISTEN "+notifyChannel) //nolint:errcheck
	for {
		n, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			return err
		}
		e, err := s.decodeNotification(ctx, n)
		if err != nil {
			s.logger.Warn("notificação inválida", "error", err)
			continue
		}
		fn(e)
	}
}

func (s *Store) decodeNotification(ctx context.Context, n *pgconn.Notification) (ports.Event, error) {
	var ref struct {
		ID  int64 `json:"id"`
		Ref bool  `json:"ref"`
	}
	if err := json.Unmarshal([]byte(n.Payload), &ref); err != nil {
		return ports.Event{}, err
	}
	if !ref.Ref {
		var e ports.Event
		err := json.Unmarshal([]byte(n.Payload), &e)
		return e, err
	}
	return scanEvent(s.pool.QueryRow(ctx, "SELECT "+eventColumns+" FROM events WHERE id=$1", ref.ID))
}

// ---------- idempotência ----------

func (s *Store) ReserveIdempotency(ctx context.Context, rec ports.IdempotencyRecord) (*ports.IdempotencyRecord, error) {
	tag, err := s.pool.Exec(ctx, `INSERT INTO idempotency_keys(scope, key, request_hash) VALUES ($1,$2,$3)
		ON CONFLICT (scope, key) DO NOTHING`, rec.Scope, rec.Key, rec.RequestHash)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 1 {
		return nil, nil
	}
	var existing ports.IdempotencyRecord
	err = s.pool.QueryRow(ctx, `SELECT scope, key, request_hash, status_code, coalesce(response, ''::bytea), created_at
		FROM idempotency_keys WHERE scope=$1 AND key=$2`, rec.Scope, rec.Key).
		Scan(&existing.Scope, &existing.Key, &existing.RequestHash, &existing.StatusCode, &existing.Response, &existing.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		// liberado entre o INSERT e o SELECT: tenta reservar de novo
		return s.ReserveIdempotency(ctx, rec)
	}
	if err != nil {
		return nil, err
	}
	if existing.RequestHash != rec.RequestHash {
		return &existing, ports.ErrHashMismatch
	}
	return &existing, ports.ErrDuplicate
}

func (s *Store) CompleteIdempotency(ctx context.Context, scope, key string, status int, response []byte) error {
	tag, err := s.pool.Exec(ctx, `UPDATE idempotency_keys SET status_code=$3, response=$4 WHERE scope=$1 AND key=$2`,
		scope, key, status, response)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ports.ErrNotFound
	}
	return nil
}

func (s *Store) ReleaseIdempotency(ctx context.Context, scope, key string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM idempotency_keys WHERE scope=$1 AND key=$2`, scope, key)
	return err
}

func (s *Store) PurgeIdempotency(ctx context.Context, before time.Time) (int64, error) {
	tag, err := s.pool.Exec(ctx, "DELETE FROM idempotency_keys WHERE created_at < $1", before)
	return tag.RowsAffected(), err
}

// ---------- registro de instâncias ----------

func (s *Store) SaveInstance(ctx context.Context, r ports.InstanceRecord) error {
	if r.StartedAt.IsZero() {
		r.StartedAt = time.Now().UTC()
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO process_instances
		(process_instance_key, business_key, process_definition_id, tenant_id, started_by, started_at)
		VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (process_instance_key) DO NOTHING`,
		r.ProcessInstanceKey, r.BusinessKey, r.ProcessDefinitionID, r.TenantID, r.StartedBy, r.StartedAt)
	return err
}

func (s *Store) FindInstancesByBusinessKey(ctx context.Context, bk string) ([]ports.InstanceRecord, error) {
	rows, err := s.pool.Query(ctx, `SELECT process_instance_key, business_key, process_definition_id, tenant_id, started_by, started_at
		FROM process_instances WHERE business_key=$1 ORDER BY started_at DESC LIMIT 100`, bk)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (ports.InstanceRecord, error) {
		var rec ports.InstanceRecord
		err := r.Scan(&rec.ProcessInstanceKey, &rec.BusinessKey, &rec.ProcessDefinitionID, &rec.TenantID, &rec.StartedBy, &rec.StartedAt)
		rec.StartedAt = rec.StartedAt.UTC()
		return rec, err
	})
}
