package main

import (
	"fmt"
	"strings"

	dgoapi "github.com/dgraph-io/dgo/v200/protos/api"
)

const (
	upsertQueryMapSize = 1000
)

type UpsertID string

type upsertQueryRecord struct {
	id       UpsertID
	field    string
	value    string
	nodeType string
}

type Quads struct {
	setQuads  []*dgoapi.NQuad
	delQuads  []*dgoapi.NQuad
	upsertIDs map[string]upsertQueryRecord
}

func NewQuads() *Quads {
	return &Quads{
		upsertIDs: make(map[string]upsertQueryRecord, upsertQueryMapSize),
	}
}

func CreateFacetString(key, value string) *dgoapi.Facet {
	modValue := removeInvalidChars(value)
	return &dgoapi.Facet{
		Key:     key,
		Value:   []byte(modValue),
		ValType: 0,
		Tokens:  []string{modValue},
	}
}

func CreateFacetDatetime(key, value string) *dgoapi.Facet {
	return &dgoapi.Facet{
		Key:     key,
		Value:   []byte(value),
		ValType: 4,
		Tokens:  []string{value},
	}
}

func NewFacetArray() []*dgoapi.Facet {
	return []*dgoapi.Facet{}
}

// SetQuadStr adds a graph string type node property.
func (q *Quads) SetQuadStr(sub, pred, obj string, facets ...*dgoapi.Facet) {
	modObj := removeInvalidChars(obj)
	if modObj != "" {
		nq := &dgoapi.NQuad{
			Subject:     sub,
			Predicate:   pred,
			ObjectValue: &dgoapi.Value{Val: &dgoapi.Value_StrVal{StrVal: modObj}},
			Facets:      facets,
		}
		q.setQuads = append(q.setQuads, nq)
	}
}

// SetQuadInt64 adds a graph int type node property.
func (q *Quads) SetQuadInt64(sub, pred string, obj int64, facets ...*dgoapi.Facet) {
	nq := &dgoapi.NQuad{
		Subject:     sub,
		Predicate:   pred,
		ObjectValue: &dgoapi.Value{Val: &dgoapi.Value_IntVal{IntVal: obj}},
		Facets:      facets,
	}
	q.setQuads = append(q.setQuads, nq)
}

// SetQuadStr adds a graph bool type node property.
func (q *Quads) SetQuadBool(sub, pred string, obj bool, facets ...*dgoapi.Facet) {
	nq := &dgoapi.NQuad{
		Subject:     sub,
		Predicate:   pred,
		ObjectValue: &dgoapi.Value{Val: &dgoapi.Value_BoolVal{BoolVal: obj}},
		Facets:      facets,
	}
	q.setQuads = append(q.setQuads, nq)
}

// SetQuadRel adds a graph edge.
func (q *Quads) SetQuadRel(sub, pred, obj string, facets ...*dgoapi.Facet) {
	nq := &dgoapi.NQuad{
		Subject:   sub,
		Predicate: pred,
		ObjectId:  obj,
		Facets:    facets,
	}
	q.setQuads = append(q.setQuads, nq)
}

// SetQuadStrUpsert adds a graph edge from an upsert node.
func (q *Quads) SetQuadStrUpsert(id UpsertID, pred, obj string, facets ...*dgoapi.Facet) {
	modObj := removeInvalidChars(obj)
	if modObj != "" {
		nq := &dgoapi.NQuad{
			Subject:     fmt.Sprintf("uid(%s)", id),
			Predicate:   pred,
			ObjectValue: &dgoapi.Value{Val: &dgoapi.Value_StrVal{StrVal: modObj}},
			Facets:      facets,
		}
		q.setQuads = append(q.setQuads, nq)
	}
}

// SetQuadBoolUpsert adds a graph bool type node property to an upsert node.
func (q *Quads) SetQuadBoolUpsert(id UpsertID, pred string, obj bool, facets ...*dgoapi.Facet) {
	nq := &dgoapi.NQuad{
		Subject:     fmt.Sprintf("uid(%s)", id),
		Predicate:   pred,
		ObjectValue: &dgoapi.Value{Val: &dgoapi.Value_BoolVal{BoolVal: obj}},
		Facets:      facets,
	}
	q.setQuads = append(q.setQuads, nq)
}

// SetQuadRelUpsertFrom adds a graph edge from an upsert node.
func (q *Quads) SetQuadRelUpsertFrom(fromID UpsertID, pred, obj string, facets ...*dgoapi.Facet) {
	nq := &dgoapi.NQuad{
		Subject:   fmt.Sprintf("uid(%s)", fromID),
		Predicate: pred,
		ObjectId:  obj,
		Facets:    facets,
	}
	q.setQuads = append(q.setQuads, nq)
}

// SetQuadRelUpsertTo adds a graph edge to an upsert node.
func (q *Quads) SetQuadRelUpsertTo(sub, pred string, toID UpsertID, facets ...*dgoapi.Facet) {
	nq := &dgoapi.NQuad{
		Subject:   sub,
		Predicate: pred,
		ObjectId:  fmt.Sprintf("uid(%s)", toID),
		Facets:    facets,
	}
	q.setQuads = append(q.setQuads, nq)
}

// SetQuadRelUpsertFromTo adds a graph edge from and upsert node to an upsert node.
func (q *Quads) SetQuadRelUpsertFromTo(fromID UpsertID, pred string, toID UpsertID, facets ...*dgoapi.Facet) {
	nq := &dgoapi.NQuad{
		Subject:   fmt.Sprintf("uid(%s)", fromID),
		Predicate: pred,
		ObjectId:  fmt.Sprintf("uid(%s)", toID),
		Facets:    facets,
	}
	q.setQuads = append(q.setQuads, nq)
}

// DelQuadProp removes a graph node property.
func (q *Quads) DelQuadProp(sub, pred string) {
	nq := &dgoapi.NQuad{
		Subject:     sub,
		Predicate:   pred,
		ObjectValue: &dgoapi.Value{Val: &dgoapi.Value_DefaultVal{DefaultVal: "_STAR_ALL"}},
	}
	q.delQuads = append(q.delQuads, nq)
}

// DelQuadRel removes a graph edge.
func (q *Quads) DelQuadRel(sub, pred, obj string) {
	nq := &dgoapi.NQuad{
		Subject:   sub,
		Predicate: pred,
		ObjectId:  obj,
	}
	q.delQuads = append(q.delQuads, nq)
}

// AddUpsertQuery adds an upsert query and returns its ID
func (q *Quads) AddUpsertQuery(field, value, nodeType string) UpsertID {
	var uqr upsertQueryRecord
	var contains bool
	modValue := removeInvalidChars(value)
	key := modValue + ":" + field + ":" + nodeType
	if uqr, contains = q.upsertIDs[key]; !contains {
		uqr = upsertQueryRecord{
			id:       UpsertID(fmt.Sprintf("upsert_id_%d", len(q.upsertIDs))),
			field:    field,
			value:    modValue,
			nodeType: nodeType,
		}
		q.upsertIDs[key] = uqr
	}
	return uqr.id
}

// Size returns the quantity of quads to set and to delete
func (q *Quads) Size() int {
	return len(q.setQuads) + len(q.delQuads)
}

// Request returns the dgraph request to perform the mutations
func (q *Quads) Request() *dgoapi.Request {
	mu := &dgoapi.Mutation{
		Set:       q.setQuads,
		Del:       q.delQuads,
		CommitNow: true,
	}
	req := &dgoapi.Request{
		Mutations: []*dgoapi.Mutation{mu},
		CommitNow: true,
	}
	if len(q.upsertIDs) > 0 {
		req.Query = q.upsertQuery()
	}
	return req
}

func (q *Quads) String() string {
	var buf strings.Builder
	if len(q.upsertIDs) > 0 {
		buf.WriteString("# Upsert Query\n")
		buf.WriteString(q.upsertQuery())
		buf.WriteString("\n\n")
	}
	if len(q.setQuads) > 0 {
		buf.WriteString("# Set Quads\n")
		for _, sq := range q.setQuads {
			obj := sq.ObjectId
			if obj == "" {
				obj = "\"" + sq.ObjectValue.GetStrVal() + "\""
			}
			buf.WriteString(fmt.Sprintf("%s %s %s .\n", sq.Subject, sq.Predicate, obj))
		}
	}
	if len(q.delQuads) > 0 {
		buf.WriteString("# Del Quads\n")
		for _, dq := range q.delQuads {
			obj := dq.ObjectId
			if obj == "" {
				obj = dq.ObjectValue.GetStrVal()
			}
			buf.WriteString(fmt.Sprintf("%s %s %s .\n", dq.Subject, dq.Predicate, obj))
		}
	}
	return buf.String()
}

func (q *Quads) upsertQuery() string {
	var buf strings.Builder
	buf.WriteString("query {\n")
	count := 0
	for _, uqr := range q.upsertIDs {
		buf.WriteString(fmt.Sprintf("\tqu%d(func: eq(%s, \"%s\")) @filter(type(%s)) {\n", count, uqr.field, uqr.value, uqr.nodeType))
		buf.WriteString(fmt.Sprintf("\t\t%s as uid\n", uqr.id))
		buf.WriteString("\t}\n")
		count++
	}
	buf.WriteString("}")
	return buf.String()
}

// Clear clears the quads
func (q *Quads) Clear() {
	q.setQuads = nil
	q.delQuads = nil
	q.upsertIDs = make(map[string]upsertQueryRecord, upsertQueryMapSize)
}
