package schema

import (
	"errors"
	"fmt"
	"strings"
)

// CreationOrder returns tables sorted so that foreign key dependencies are
// satisfied: parents appear before children. Self-referential foreign keys
// (a table referencing itself) are allowed and do not constitute a cycle.
// Tables with no FK dependencies may appear in any stable order.
func CreationOrder(tables ...*TableDef) ([]*TableDef, error) {
	return topoSort(tables, false)
}

// DropOrder returns tables sorted so that children appear before parents,
// which is the reverse of CreationOrder. This is the safe order for DROP TABLE.
func DropOrder(tables ...*TableDef) ([]*TableDef, error) {
	return topoSort(tables, true)
}

// topoSort performs a topological sort using Kahn's algorithm.
// If reverse is true, the result is reversed (drop order).
func topoSort(tables []*TableDef, reverse bool) ([]*TableDef, error) {
	// Build a name -> table index.
	byName := make(map[string]*TableDef, len(tables))
	for _, t := range tables {
		byName[t.Name] = t
	}

	// Build adjacency: edges go from dependency (parent) -> dependent (child).
	// inDegree counts how many parents each table has.
	inDegree := make(map[string]int, len(tables))
	// children[parent] = list of child table names
	children := make(map[string][]string, len(tables))

	for _, t := range tables {
		if _, ok := inDegree[t.Name]; !ok {
			inDegree[t.Name] = 0
		}
		for _, col := range t.cols {
			ref := col.refTable
			if ref == "" {
				continue
			}
			// Skip self-references — not a real dependency edge.
			if ref == t.Name {
				continue
			}
			// Only count references to tables in the input set.
			if _, ok := byName[ref]; !ok {
				continue
			}
			inDegree[t.Name]++
			children[ref] = append(children[ref], t.Name)
		}
	}

	// Seed queue with tables that have no dependencies.
	var queue []string
	for _, t := range tables {
		if inDegree[t.Name] == 0 {
			queue = append(queue, t.Name)
		}
	}

	var sorted []*TableDef
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		sorted = append(sorted, byName[name])

		for _, child := range children[name] {
			inDegree[child]--
			if inDegree[child] == 0 {
				queue = append(queue, child)
			}
		}
	}

	if len(sorted) != len(tables) {
		// Find tables involved in the cycle for a useful error message.
		var cycled []string
		for _, t := range tables {
			if inDegree[t.Name] > 0 {
				cycled = append(cycled, t.Name)
			}
		}
		return nil, fmt.Errorf("%w: %s", ErrCyclicDependency, strings.Join(cycled, ", "))
	}

	if reverse {
		for i, j := 0, len(sorted)-1; i < j; i, j = i+1, j-1 {
			sorted[i], sorted[j] = sorted[j], sorted[i]
		}
	}

	return sorted, nil
}

// ErrCyclicDependency is returned when tables have circular foreign key references.
var ErrCyclicDependency = errors.New("cyclic foreign key dependency among tables")
