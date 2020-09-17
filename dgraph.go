package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	dgo "github.com/dgraph-io/dgo/v200"
	dgoapi "github.com/dgraph-io/dgo/v200/protos/api"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/connectivity"
)

const (
	maxSendMsgSize = 1024 * 1024 * 1024
	maxRecvMsgSize = 1024 * 1024 * 1024

	dgTimeout    = 10 * time.Second
	dgMaxRetries = 10
)

type GraphConnection struct {
	gCl        *dgo.Dgraph
	gConnsURLS []string
	gConns     []*grpc.ClientConn
	logger     *zap.Logger
}

type Schema string

// NewGraphConnection sets up and opens a new Dgraph connection.
func NewGraphConnection(ctx context.Context, dgraphURLs []string, logger *zap.Logger) (*GraphConnection, error) {
	// Init logger
	if logger == nil {
		logger, _ = zap.NewDevelopment()
	}

	// Set up the Dgraph DB connection.
	if len(dgraphURLs) == 0 {
		return nil, fmt.Errorf("must provide at least one dgraph connection URL")
	}

	gc := &GraphConnection{
		gConnsURLS: dgraphURLs,
		logger:     logger,
	}

	return gc, gc.openConnection(ctx)
}

// Ready returns true if all connections are in a Ready state.
func (gc *GraphConnection) Ready() (ready bool) {
	for _, conn := range gc.gConns {
		if conn.GetState() == connectivity.Ready {
			ready = true
		} else {
			ready = false
			return
		}
	}
	return
}

func (gc *GraphConnection) openConnection(ctx context.Context) error {
	// Set up dgraph config.
	backoffConfig := backoff.DefaultConfig
	backoffConfig.MaxDelay = 30 * time.Second

	dgraphOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.FailOnNonTempDialError(true),
		grpc.WithConnectParams(
			grpc.ConnectParams{
				Backoff:           backoffConfig,
				MinConnectTimeout: 20 * time.Second,
			},
		),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxRecvMsgSize),
			grpc.MaxCallSendMsgSize(maxSendMsgSize),
		),
	}

	var dgGrpcConns []*grpc.ClientConn
	var dgAPICls []dgoapi.DgraphClient
	for _, url := range gc.gConnsURLS {
		dgGrpcConn, err := grpc.DialContext(ctx, url, dgraphOpts...)
		if err != nil {
			return fmt.Errorf("unable to dial dgraph alpha server %s: %s", url, err)
		} else {
			dc := dgoapi.NewDgraphClient(dgGrpcConn)
			dgAPICls = append(dgAPICls, dc)
			dgGrpcConns = append(dgGrpcConns, dgGrpcConn)
		}
	}

	// Check to make sure all servers are connected.
	if len(dgGrpcConns) != len(gc.gConnsURLS) {
		return fmt.Errorf("unable to connect to dgraph alpha servers at URLs: %v", gc.gConnsURLS)
	}

	gc.gConns = dgGrpcConns
	gc.gCl = dgo.NewDgraphClient(dgAPICls...)
	return nil
}

func (gc *GraphConnection) LoadSchema(ctx context.Context, schema Schema) (err error) {
	op := &dgoapi.Operation{Schema: string(schema)}
	timeout := dgTimeout
	retry := 0
	for {
		err = gc.gCl.Alter(ctx, op)
		if err == nil {
			if retry > 0 {
				gc.logger.Warn("dgraph alter schema retry successful", zap.Int("attempt", retry))
			}
			break
		} else {
			retryAgain, nextRetry, reopenErr := gc.checkError(ctx, err, retry, dgMaxRetries)
			if reopenErr != nil {
				return fmt.Errorf("unable to reconnect to dgraph: %s", err)
			}
			if !retryAgain {
				return fmt.Errorf("unable to perform dgraph alter schema in %d attempts: %s", retry, err)
			}
			retry = nextRetry
			gc.logger.Warn("dgraph alter schema failed, retrying...", zap.Error(err), zap.Duration("retry-in", timeout), zap.Int("attempt", retry))
		}
		time.Sleep(timeout)
		timeout *= 2
	}
	return nil
}

func (gc *GraphConnection) Mutate(ctx context.Context, q *Quads) (err error) {
	req := q.Request()
	timeout := dgTimeout
	retry := 0
	for {
		_, err = gc.gCl.NewTxn().Do(ctx, req)
		if err == nil {
			if retry > 0 {
				gc.logger.Warn("dgraph transaction retry successful", zap.Int("attempt", retry))
			}
			break
		} else {
			retryAgain, nextRetry, reopenErr := gc.checkError(ctx, err, retry, dgMaxRetries)
			if reopenErr != nil {
				return fmt.Errorf("unable to reconnect to dgraph: %s", err)
			}
			if !retryAgain {
				return fmt.Errorf("unable to perform dgraph transaction in %d attempts: %s", retry, err)
			}
			retry = nextRetry
			gc.logger.Warn("dgraph transaction failed, retrying...", zap.Error(err), zap.Duration("retry-in", timeout), zap.Int("attempt", retry))
		}
		time.Sleep(timeout)
		timeout *= 2
	}
	return nil
}

// checkError looks for errors that should be retried.
func (gc *GraphConnection) checkError(ctx context.Context, err error, retry, maxRetries int) (retryAgain bool, nextRetry int, reopenErr error) {
	if retry >= maxRetries {
		return // do not retry (retryAgain == false)
	}
	nextRetry = retry + 1
	errStr := strings.ToLower(err.Error())
	// check to see if the error is one that should be retried, or if the
	// connection should be reopened first and then retried, or if it an
	// error that should not be retried.
	if err == dgo.ErrAborted || strings.Contains(errStr, "aborted") || strings.Contains(errStr, "less than mints") || strings.Contains(errStr, "transaction is too old") {
		retryAgain = true
	} else if strings.Contains(errStr, "transport is closing") || strings.Contains(errStr, "unhealthy connection") {
		retryAgain = true
		gc.closeConnection()
		time.Sleep(5 * time.Second)
		reopenErr = gc.openConnection(ctx)
	} else {
		retryAgain = false
	}
	return // do not retry (retryAgain == false)
}

// Close closes the dgraph connections.
func (gc *GraphConnection) Close() {
	gc.closeConnection()
}

func (gc *GraphConnection) closeConnection() {
	if gc.gCl != nil {
		for _, conn := range gc.gConns {
			conn.Close()
		}
	}
}
