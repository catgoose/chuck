package schema

import (
	"errors"
	"fmt"
	"strings"

	"github.com/catgoose/chuck"
)

// CreationOrder returns tables sorted so that foreign key dependencies are
// satisfied: parents appear before children. Self-referential foreign keys
// (a table referencing itself) are allowed and do not constitute a cycle.
// Tables with no FK dependencies may appear in any stable order.
//
// Dependency ordering keys on fully qualified identity (schema + name) so two
// declared tables with the same bare name but different schemas (e.g.
// sg.SalesAgents and cl.SalesAgents) are treated as distinct.
func CreationOrder(tables ...*TableDef) ([]*TableDef, error) {
	return topoSort(tables, false)
}

// DropOrder returns tables sorted so that children appear before parents,
// which is the reverse of CreationOrder. This is the safe order for DROP TABLE.
func DropOrder(tables ...*TableDef) ([]*TableDef, error) {
	return topoSort(tables, true)
}

// topoSort performs a topological sort using Kahn's algorithm. Tables are
// keyed by their fully qualified ObjectName so that schema-qualified
// duplicates of the same bare table name resolve to distinct nodes.
func topoSort(tables []*TableDef, reverse bool) ([]*TableDef, error) {
	byKey := make(map[string]*TableDef, len(tables))
	keys := make([]string, len(tables))
	for i, t := range tables {
		k := objectKey(t.Object())
		byKey[k] = t
		keys[i] = k
	}

	// Build edges and in-degrees keyed by qualified identity. Foreign-key
	// targets without an explicit schema can match either a same-schema
	// declared table or an unqualified declared table — pick the more
	// specific match first so multi-schema graphs resolve correctly.
	inDegree := make(map[string]int, len(tables))
	children := make(map[string][]string, len(tables))

	for i, t := range tables {
		key := keys[i]
		if _, ok := inDegree[key]; !ok {
			inDegree[key] = 0
		}
		for _, col := range t.cols {
			target := col.refObject
			if target.Name == "" {
				continue
			}
			refKey := resolveRefKey(byKey, t, target)
			if refKey == "" {
				continue
			}
			if refKey == key {
				continue
			}
			inDegree[key]++
			children[refKey] = append(children[refKey], key)
		}
	}

	var queue []string
	for _, k := range keys {
		if inDegree[k] == 0 {
			queue = append(queue, k)
		}
	}

	var sorted []*TableDef
	for len(queue) > 0 {
		k := queue[0]
		queue = queue[1:]
		sorted = append(sorted, byKey[k])

		for _, child := range children[k] {
			inDegree[child]--
			if inDegree[child] == 0 {
				queue = append(queue, child)
			}
		}
	}

	if len(sorted) != len(tables) {
		var cycled []string
		for _, k := range keys {
			if inDegree[k] > 0 {
				cycled = append(cycled, k)
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

// resolveRefKey returns the byKey lookup key that target resolves to within
// the declared set, or "" if the target is not in the set. When the target
// has no explicit schema, the search first tries the parent table's schema
// (intra-schema reference), then falls back to an unqualified declaration.
func resolveRefKey(byKey map[string]*TableDef, parent *TableDef, target chuck.ObjectName) string {
	if target.Schema != "" {
		k := objectKey(target)
		if _, ok := byKey[k]; ok {
			return k
		}
		return ""
	}
	// No explicit schema: prefer same-schema target, then unqualified.
	if parent.schema != "" {
		k := objectKey(chuck.ObjectName{Schema: parent.schema, Name: target.Name})
		if _, ok := byKey[k]; ok {
			return k
		}
	}
	k := objectKey(target)
	if _, ok := byKey[k]; ok {
		return k
	}
	return ""
}

// ErrCyclicDependency is returned when tables have circular foreign key references.
var ErrCyclicDependency = errors.New("cyclic foreign key dependency among tables")
