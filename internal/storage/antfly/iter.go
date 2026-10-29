package antfly

import (
	"context"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

// The aggregate already bounds and materializes these snapshots. Iterators
// preserve the existing CLI contract without holding a native handle open.
func (s *Store) IterIssueComments(ctx context.Context, id string) (storage.Iter[types.Comment], error) {
	rows, e := s.GetIssueComments(ctx, id)
	return storage.NewSliceIter(rows), e
}
func (s *Store) IterDependenciesWithMetadata(ctx context.Context, id string) (storage.Iter[types.IssueWithDependencyMetadata], error) {
	rows, e := s.GetDependenciesWithMetadata(ctx, id)
	return storage.NewSliceIter(rows), e
}
func (s *Store) IterDependentsWithMetadata(ctx context.Context, id string) (storage.Iter[types.IssueWithDependencyMetadata], error) {
	rows, e := s.GetDependentsWithMetadata(ctx, id)
	return storage.NewSliceIter(rows), e
}
func (s *Store) IterIssues(ctx context.Context, q string, f types.IssueFilter) (storage.Iter[types.Issue], error) {
	rows, e := s.SearchIssues(ctx, q, f)
	return storage.NewSliceIter(rows), e
}
func (s *Store) IterReadyWork(ctx context.Context, f types.WorkFilter) (storage.Iter[types.Issue], error) {
	rows, e := s.GetReadyWork(ctx, f)
	return storage.NewSliceIter(rows), e
}
