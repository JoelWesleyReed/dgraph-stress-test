package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/alecthomas/kingpin"
)

const (
	dgraphTimeout = 10 * time.Minute
)

func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		if i%8 == 0 {
			b[i] = ' '
		} else {
			b[i] = charset[rand.Intn(len(charset))]
		}
	}
	return string(b)
}

func lessRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	pos := rand.Intn(len(charset))
	return strings.Repeat(string(charset[pos]), length)
}

func initDgraphConn(ctx context.Context, dgraphURLs []string, nodeTypeCount, nodePredCount int) (*GraphConnection, error) {
	// Open connection
	connectCtx, connectCancel := context.WithTimeout(ctx, dgraphTimeout)
	defer connectCancel()
	dgc, err := NewGraphConnection(connectCtx, dgraphURLs, nil)
	if err != nil {
		return nil, err
	}

	// Load schema
	var schema strings.Builder
	for i := 0; i < nodeTypeCount; i++ {
		schema.WriteString(fmt.Sprintf("type Node%d {\n", i))
		schema.WriteString("\tname\n")
		for j := 0; j < nodePredCount; j++ {
			schema.WriteString(fmt.Sprintf("\tpred%d\n", j))
		}
		schema.WriteString("\tNEXT\n")
		for k := 0; k < nodeTypeCount; k++ {
			schema.WriteString(fmt.Sprintf("\tLINK%d\n", k))
		}
		schema.WriteString("}\n\n")
	}
	schema.WriteString("name: string @index(term) .\n")
	for j := 0; j < nodePredCount; j++ {
		schema.WriteString(fmt.Sprintf("pred%d: string @index(hash) .\n", j))
	}
	schema.WriteString("NEXT: [uid] .\n")
	for k := 0; k < nodeTypeCount; k++ {
		schema.WriteString(fmt.Sprintf("LINK%d: [uid] .\n", k))
	}
	fmt.Printf("Schema:\n%s\n", schema.String())
	err = dgc.LoadSchema(ctx, Schema(schema.String()))
	if err != nil {
		return dgc, err
	}
	return dgc, nil
}

// Creates graph with unconnected nodes
func testUnconnected(ctx context.Context, dgc *GraphConnection, nodeTypeCount, nodePredCount, predStringLength, rounds int) error {
	quads := NewQuads()
	for i := 0; i < nodeTypeCount; i++ {
		subj := fmt.Sprintf("_:%d", i)
		quads.SetQuadStr(subj, "dgraph.type", fmt.Sprintf("Node%d", i))
		quads.SetQuadStr(subj, "name", fmt.Sprintf("Node%d", i))
		for j := 0; j < nodePredCount; j++ {
			quads.SetQuadStr(subj, fmt.Sprintf("pred%d", j), randomString(predStringLength))
		}
	}

	fmt.Printf("# Test Unconnnected: %d rounds; %d node types; %d predicates\n", rounds, nodeTypeCount, nodePredCount)
	fmt.Println("round,time (ms)")
	for r := 0; r < rounds; r++ {
		startTime := time.Now()
		err := dgc.Mutate(context.Background(), quads)
		endTime := time.Now()
		if err != nil {
			return err
		}
		fmt.Printf("%d,%d\n", r, endTime.Sub(startTime).Milliseconds())
	}

	return nil
}

// Creates graph with multiple fully connected subgraphs that are not connected
// to one another
func testConnectedSubgraphs(ctx context.Context, dgc *GraphConnection, nodeTypeCount, nodePredCount, predStringLength, rounds int) error {
	quads := NewQuads()
	for i := 0; i < nodeTypeCount; i++ {
		subj := fmt.Sprintf("_:%d", i)
		quads.SetQuadStr(subj, "dgraph.type", fmt.Sprintf("Node%d", i))
		quads.SetQuadStr(subj, "name", fmt.Sprintf("Node%d", i))
		for j := 0; j < nodePredCount; j++ {
			quads.SetQuadStr(subj, fmt.Sprintf("pred%d", j), randomString(predStringLength))
		}
		for k := 0; k < nodeTypeCount; k++ {
			quads.SetQuadRel(subj, fmt.Sprintf("LINK%d", k), fmt.Sprintf("_:%d", k))
		}
	}

	fmt.Printf("# Test Connnected Subgraphs: %d rounds; %d node types; %d predicates\n", rounds, nodeTypeCount, nodePredCount)
	fmt.Println("round,time (ms)")
	for r := 0; r < rounds; r++ {
		startTime := time.Now()
		err := dgc.Mutate(context.Background(), quads)
		endTime := time.Now()
		if err != nil {
			return err
		}
		fmt.Printf("%d,%d\n", r, endTime.Sub(startTime).Milliseconds())
	}

	return nil
}

// Creates graph with multiple fully connected subgraphs which are connected
// to one another
func testFullyConnected(ctx context.Context, dgc *GraphConnection, nodeTypeCount, nodePredCount, predStringLength, rounds int) error {
	quads := NewQuads()
	fmt.Printf("# Test Fully Connnected: %d rounds; %d node types; %d predicates of %d length\n", rounds, nodeTypeCount, nodePredCount, predStringLength)
	fmt.Println("round,quad-count,time (ms)")
	for r := 0; r < rounds; r++ {
		for i := 0; i < nodeTypeCount; i++ {
			nodeName := fmt.Sprintf("Node-%d.%d", r, i)
			nodeType := fmt.Sprintf("Node%d", i)
			upsertIDCurrent := quads.AddUpsertQuery("name", nodeName, nodeType)

			quads.SetQuadStrUpsert(upsertIDCurrent, "dgraph.type", nodeType)
			quads.SetQuadStrUpsert(upsertIDCurrent, "name", nodeName)
			for j := 0; j < nodePredCount; j++ {
				quads.SetQuadStrUpsert(upsertIDCurrent, fmt.Sprintf("pred%d", j), lessRandomString(predStringLength))
			}

			if i == 0 {
				nodeNamePlus1 := fmt.Sprintf("Node-%d.%d", r+1, i)
				upsertIDPlus1 := quads.AddUpsertQuery("name", nodeNamePlus1, nodeType)
				quads.SetQuadStrUpsert(upsertIDPlus1, "dgraph.type", nodeType)
				quads.SetQuadStrUpsert(upsertIDPlus1, "name", nodeNamePlus1)
				quads.SetQuadRelUpsertFromTo(upsertIDCurrent, "NEXT", upsertIDPlus1)
			}

			for k := 0; k < nodeTypeCount; k++ {
				upsertIDLink := quads.AddUpsertQuery("name", fmt.Sprintf("Node-%d.%d", r, k), fmt.Sprintf("Node%d", k))
				quads.SetQuadRelUpsertFromTo(upsertIDCurrent, fmt.Sprintf("LINK%d", k), upsertIDLink)
			}
		}

		fmt.Printf("%s\n\n\n\n", quads.String())

		startTime := time.Now()
		err := dgc.Mutate(context.Background(), quads)
		endTime := time.Now()
		if err != nil {
			return err
		}
		fmt.Printf("%d,%d,%d\n", r, quads.Size(), endTime.Sub(startTime).Milliseconds())
		quads.Clear()
	}

	return nil
}

var (
	app           = kingpin.New("dgraph-stress-test", "create a synthetic graph")
	dgraphAddr    = app.Flag("dgraph-addr", "set the connection string (host:port) for Dgraph DB; use multiple flags for multiple servers").Default("127.0.0.1:9080").Strings()
	nodeTypeCount = app.Flag("node-type-count", "set the number of node types").Default("50").Int()
	nodePredCount = app.Flag("node-pred-count", "set the number of predicates per node").Default("50").Int()
	predStringLen = app.Flag("pred-string-len", "set the length of the string to store in each predicate").Default("20").Int()
	rounds        = app.Flag("rounds", "set the number of rounds to perform").Default("500000").Int()
)

func main() {
	_, err := app.Parse(os.Args[1:])
	if err != nil {
		panic(err)
	}
	fmt.Printf("# dgraph-addr(s): %v\n", *dgraphAddr)

	dgc, err := initDgraphConn(context.Background(), *dgraphAddr, *nodeTypeCount, *nodePredCount)
	if err != nil {
		panic(err)
	}
	defer dgc.Close()

	err = testFullyConnected(context.Background(), dgc, *nodeTypeCount, *nodePredCount, *predStringLen, *rounds)
	if err != nil {
		panic(err)
	}

}
