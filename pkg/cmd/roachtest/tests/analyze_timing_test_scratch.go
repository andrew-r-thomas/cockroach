// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"golang.org/x/sync/errgroup"
)

func registerAnalyzeTimingScratch(r registry.Registry) {
	for _, warehouses := range []int{10, 100} {
		for _, parallel := range []bool{false, true} {
			warehouses, parallel := warehouses, parallel
			mode := "sequential"
			if parallel {
				mode = "parallel"
			}
			r.Add(registry.TestSpec{
				Name: fmt.Sprintf(
					"scratch/analyze-timing/warehouses=%d/%s", warehouses, mode,
				),
				Owner:            registry.OwnerDisasterRecovery,
				Cluster:          r.MakeClusterSpec(4+1, spec.WorkloadNode()),
				CompatibleClouds: registry.AllExceptAWS,
				Suites:           registry.ManualOnly,
				Timeout:          30 * time.Minute,
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runAnalyzeTimingTest(ctx, t, c, warehouses, parallel)
				},
			})
		}
	}
}

func runAnalyzeTimingTest(
	ctx context.Context, t test.Test, c cluster.Cluster, warehouses int, parallel bool,
) {
	const tenantName = "analyze-test"
	clusterSettings := install.MakeClusterSettings()
	c.Start(ctx, t.L(), option.DefaultStartOpts(), clusterSettings, c.CRDBNodes())

	startOpts := option.StartSharedVirtualClusterOpts(tenantName)
	c.StartServiceForVirtualCluster(ctx, t.L(), startOpts, clusterSettings)

	t.Status(fmt.Sprintf("importing tpcc with %d warehouses", warehouses))
	initStart := timeutil.Now()
	initCmd := fmt.Sprintf(
		`./cockroach workload init tpcc --data-loader import --warehouses %d {pgurl%s:%s}`,
		warehouses, c.CRDBNodes(), tenantName,
	)
	c.Run(ctx, option.WithNodes(c.WorkloadNode()), initCmd)
	t.L().Printf("tpcc import took %s", timeutil.Since(initStart))

	conn := c.Conn(
		ctx, t.L(), c.CRDBNodes()[0],
		option.VirtualClusterName(tenantName),
		option.DBName("tpcc"),
		option.User("root"),
		option.AuthMode(install.AuthRootCert),
	)
	defer conn.Close()
	db := sqlutils.MakeSQLRunner(conn)

	rows := db.QueryStr(t, `SELECT table_name FROM [SHOW TABLES]`)
	tables := make([]string, len(rows))
	for i, row := range rows {
		tables[i] = row[0]
	}
	t.L().Printf("found %d tables: %v", len(tables), tables)

	mode := "sequential"
	if parallel {
		mode = "parallel"
	}
	t.Status(fmt.Sprintf("running ANALYZE (%s) on %d tables", mode, len(tables)))
	totalStart := timeutil.Now()

	if parallel {
		g, gCtx := errgroup.WithContext(ctx)
		for _, table := range tables {
			table := table
			g.Go(func() error {
				// Each goroutine needs its own connection.
				c2 := c.Conn(
					gCtx, t.L(), c.CRDBNodes()[0],
					option.VirtualClusterName(tenantName),
					option.DBName("tpcc"),
					option.User("root"),
					option.AuthMode(install.AuthRootCert),
				)
				defer c2.Close()
				start := timeutil.Now()
				if _, err := c2.ExecContext(gCtx, fmt.Sprintf(`ANALYZE "%s"`, table)); err != nil {
					return err
				}
				t.L().Printf("ANALYZE %q took %s (parallel)", table, timeutil.Since(start))
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			t.Fatal(err)
		}
	} else {
		for _, table := range tables {
			start := timeutil.Now()
			db.Exec(t, fmt.Sprintf(`ANALYZE "%s"`, table))
			t.L().Printf("ANALYZE %q took %s (sequential)", table, timeutil.Since(start))
		}
	}

	t.L().Printf("total ANALYZE time (%s): %s", mode, timeutil.Since(totalStart))
}
