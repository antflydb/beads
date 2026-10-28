package issueops

import (
	"context"
	"database/sql/driver"
	"regexp"
	"slices"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

// coordinateSQL is the prefix of the shard coordination write every guarded
// create and every retaining sweep makes before it reads or deletes.
var coordinateSQL = regexp.QuoteMeta("REPLACE INTO local_metadata (`key`, value) VALUES")

// coordinationKeysFor returns the distinct, sorted shard keys of ids: the
// argument order CoordinateIDCreatesInTx writes them in.
func coordinationKeysFor(ids ...string) []string {
	var keys []string
	for _, id := range ids {
		key := issueCreateCoordinationKey(id)
		if !slices.Contains(keys, key) {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	return keys
}

// expectCoordination expects one coordination write of exactly keys, in order.
func expectCoordination(mock sqlmock.Sqlmock, keys []string) {
	args := make([]driver.Value, 0, 2*len(keys))
	for _, key := range keys {
		args = append(args, key, sqlmock.AnyArg())
	}
	mock.ExpectExec(coordinateSQL).WithArgs(args...).WillReturnResult(sqlmock.NewResult(0, int64(len(keys))))
}

// TestCoordinateIDCreatesWritesEachShardOnceInShardOrder pins the lock shape:
// one cell per shard however many ids share it, in sorted key order (two
// writers locking overlapping shard sets in the same order cannot deadlock),
// in one statement, and nothing at all for no ids.
func TestCoordinateIDCreatesWritesEachShardOnceInShardOrder(t *testing.T) {
	db, mock := newIDRetentionMock(t)
	ids := []string{"bd-zz", "bd-aa", "bd-zz", "", "bd-mm"}
	expectCoordination(mock, coordinationKeysFor("bd-zz", "bd-aa", "bd-mm"))
	if err := CoordinateIDCreatesInTx(context.Background(), db, ids); err != nil {
		t.Fatalf("CoordinateIDCreatesInTx() = %v", err)
	}
	if err := CoordinateIDCreatesInTx(context.Background(), db, []string{""}); err != nil {
		t.Fatalf("CoordinateIDCreatesInTx(no ids) = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestRefuseRetiredIDCoordinatesBeforeItsProbe pins the upsert guard's order:
// the shard cell is written BEFORE the occupancy probe. Under READ COMMITTED
// that write waits for a retaining sweep holding the same cell, so the probe
// that follows sees the sweep's tombstone instead of the row it deleted.
func TestRefuseRetiredIDCoordinatesBeforeItsProbe(t *testing.T) {
	db, mock := newIDRetentionMock(t)
	expectCoordination(mock, coordinationKeysFor("bd-1"))
	mock.ExpectQuery(regexp.QuoteMeta(idOccupancySQL)).WithArgs("bd-1", "bd-1", "bd-1", "bd-1").
		WillReturnRows(sqlmock.NewRows([]string{"src", "closed_at"}))
	if err := RefuseRetiredIDInTx(context.Background(), db, "bd-1"); err != nil {
		t.Fatalf("RefuseRetiredIDInTx() = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestUpsertCreateCoordinatesBeforeItsRetiredProbe pins the same order on the
// upsert-shaped create body (CreateOnly false, explicit id): the stores'
// CreateIssue, a transaction's create, a one-row batch.
func TestUpsertCreateCoordinatesBeforeItsRetiredProbe(t *testing.T) {
	db, mock := newIDRetentionMock(t)
	expectCoordination(mock, coordinationKeysFor("bd-1"))
	mock.ExpectQuery(regexp.QuoteMeta(idOccupancySQL)).WithArgs("bd-1", "bd-1", "bd-1", "bd-1").
		WillReturnRows(sqlmock.NewRows([]string{"src", "closed_at"}).AddRow("issues", nil))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM wisps WHERE id = ?")).WithArgs("bd-1").
		WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(0))
	bc := &BatchContext{Opts: storage.BatchCreateOptions{SkipPrefixValidation: true}}
	issue := &types.Issue{ID: "bd-1", Title: "t", Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask}
	if _, _, outcome, err := prepareCreateIssueInTx(context.Background(), db, bc, issue, "actor"); err != nil || outcome != prepareWrite {
		t.Fatalf("prepareCreateIssueInTx() = %v, %v; want prepareWrite, nil", outcome, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestUpsertBatchCoordinatesEveryExplicitIDBeforeItsReads pins the batch
// order: an upsert batch locks the shard of every explicit id in one write
// BEFORE its up-front presence and tombstone reads, so those reads cannot
// predate a racing sweep's commit; the rows it then prepares do not lock again.
func TestUpsertBatchCoordinatesEveryExplicitIDBeforeItsReads(t *testing.T) {
	db, mock := newIDRetentionMock(t)
	issues := []*types.Issue{
		{ID: "bd-1", Title: "a", Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask},
		{ID: "bd-2", Title: "b", Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask},
		{Title: "generated", Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask},
	}
	expectCoordination(mock, coordinationKeysFor("bd-1", "bd-2"))
	for _, table := range []string{"issues", "wisps"} {
		mock.ExpectQuery(regexp.QuoteMeta("SELECT id FROM " + table + " WHERE id IN (?,?)")).
			WillReturnRows(sqlmock.NewRows([]string{"id"}))
	}
	for _, table := range IDTombstoneTables() {
		mock.ExpectQuery(regexp.QuoteMeta("SELECT id, closed_at FROM " + table + " WHERE id IN (?,?)")).
			WillReturnRows(sqlmock.NewRows([]string{"id", "closed_at"}))
	}
	cache, err := newCreateBatchCache(context.Background(), db, issues, storage.BatchCreateOptions{})
	if err != nil {
		t.Fatalf("newCreateBatchCache() = %v", err)
	}
	if !cache.coordinated {
		t.Fatal("an upsert batch's cache does not record that it coordinated its explicit ids")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	// A prepared row of this batch answers from the cache and writes no second
	// coordination cell: any statement here is unexpected and fails it.
	bc := &BatchContext{Opts: storage.BatchCreateOptions{SkipPrefixValidation: true}, cache: cache}
	if _, _, outcome, err := prepareCreateIssueInTx(context.Background(), db, bc, issues[0], "actor"); err != nil || outcome != prepareWrite {
		t.Fatalf("prepareCreateIssueInTx(batch row) = %v, %v; want prepareWrite, nil", outcome, err)
	}
}

// TestCreateOnlyBatchSkipsTheUpFrontTombstoneRead pins that a CreateOnly
// batch neither coordinates up front (each row's EnsureIssueIDAvailableInTx
// does) nor reads tombstones it would never consult: its rows are never
// deferred and every one is probed by the create-only guard.
func TestCreateOnlyBatchSkipsTheUpFrontTombstoneRead(t *testing.T) {
	db, mock := newIDRetentionMock(t)
	issues := []*types.Issue{{ID: "bd-1"}, {ID: "bd-2"}}
	for _, table := range []string{"issues", "wisps"} {
		mock.ExpectQuery(regexp.QuoteMeta("SELECT id FROM " + table + " WHERE id IN (?,?)")).
			WillReturnRows(sqlmock.NewRows([]string{"id"}))
	}
	cache, err := newCreateBatchCache(context.Background(), db, issues, storage.BatchCreateOptions{CreateOnly: true})
	if err != nil {
		t.Fatalf("newCreateBatchCache(CreateOnly) = %v", err)
	}
	if cache.coordinated {
		t.Fatal("a CreateOnly batch's cache claims to have coordinated; its rows must each run the create-only guard")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestRetainingSweepCoordinatesBeforeItDeletes pins the sweep half of the
// lock: the tombstone plan a retaining sweep builds BEFORE its delete starts
// by locking the shard of every id it will retire. An upsert that probed
// first holds the cell, so the SERIALIZABLE sweep fails and retries and sees
// the upserted row; an upsert that comes second waits for the sweep's commit.
func TestRetainingSweepCoordinatesBeforeItDeletes(t *testing.T) {
	db, mock := newIDRetentionMock(t)
	closedAt := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	kept := []*types.Issue{{ID: "bd-2", ClosedAt: &closedAt}, {ID: "bd-1", ClosedAt: &closedAt}}
	expectCoordination(mock, coordinationKeysFor("bd-1", "bd-2"))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT 1 FROM wisps LIMIT 1")).WillReturnRows(sqlmock.NewRows([]string{"1"}))
	wisp, durable, err := sweepRetiredIDsInTx(context.Background(), db, kept)
	if err != nil {
		t.Fatalf("sweepRetiredIDsInTx() = %v", err)
	}
	if len(wisp) != 0 || len(durable) != 2 {
		t.Fatalf("sweepRetiredIDsInTx() = %v wisp, %v durable; want both durable", wisp, durable)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
