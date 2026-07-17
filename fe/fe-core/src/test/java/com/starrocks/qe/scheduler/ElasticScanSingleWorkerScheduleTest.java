// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.qe.scheduler;

import com.starrocks.common.Config;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.proto.PUpdateExchangeSendersRequest;
import com.starrocks.proto.PUpdateExchangeSendersResult;
import com.starrocks.proto.StatusPB;
import com.starrocks.qe.scheduler.dag.FragmentInstanceExecState;
import com.starrocks.qe.scheduler.slot.DeployState;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * A query that starts on a single worker must keep the incremental {@code AllAtOnceExecutionSchedule}
 * when elastic scan is armed, instead of downgrading to {@code SingleNodeSchedule} (which deploys once
 * and skips the loop that grows onto newly joined CNs). This is the canonical autoscale-from-one case,
 * so it runs with {@code singleNodeTest = true} to force {@code ExecutionDAG.getWorkerNum() == 1}.
 */
public class ElasticScanSingleWorkerScheduleTest extends SchedulerTestBase {
    private static final String SQL = "select L_ORDERKEY from lineitem";
    private static final int NEW_BACKEND_ID = 10005;

    private final List<String> events = new ArrayList<>();

    @BeforeAll
    public static void beforeClass() throws Exception {
        singleNodeTest = true;
        SchedulerTestBase.beforeClass();
    }

    @AfterAll
    public static void afterClass() {
        SchedulerTestBase.afterClass();
        singleNodeTest = false;
    }

    @BeforeEach
    public void before() {
        Config.enable_elastic_scan_execution = true;
        connectContext.getSessionVariable().setEnableOlapIncrementalScanRanges(true);
        connectContext.getSessionVariable().setEnableElasticScanStages(true);
        connectContext.getSessionVariable().setConnectorIncrementalScanRangeNumber(4);
        events.clear();
    }

    @AfterEach
    public void after() throws Exception {
        try {
            UtFrameUtils.dropMockBackend(NEW_BACKEND_ID);
        } catch (Exception e) {
            // the test may not have added the backend
        }
        Config.enable_elastic_scan_execution = false;
        connectContext.getSessionVariable().setEnableOlapIncrementalScanRanges(false);
        connectContext.getSessionVariable().setEnableElasticScanStages(false);
    }

    private static void mockSharedDataLakeScan() {
        new MockUp<DefaultWorkerProvider>() {
            @Mock
            public boolean allowUsingBackupNode() {
                return true;
            }
        };
        new MockUp<OlapScanNode>() {
            @Mock
            public boolean isLocalNativeTable() {
                return false;
            }
        };
    }

    private void mockSenderRegistration() {
        new MockUp<BackendServiceClient>() {
            @Mock
            public Future<PUpdateExchangeSendersResult> updateExchangeSenders(TNetworkAddress address,
                                                                              PUpdateExchangeSendersRequest request) {
                events.add((Boolean.TRUE.equals(request.unregister) ? "unregister:" : "register:") + request.beNumber);
                PUpdateExchangeSendersResult result = new PUpdateExchangeSendersResult();
                StatusPB status = new StatusPB();
                status.statusCode = TStatusCode.OK.getValue();
                status.errorMsgs = new ArrayList<>();
                result.status = status;
                return CompletableFuture.completedFuture(result);
            }
        };
    }

    private void captureDeploysAndJoinBackendMidQuery() {
        new MockUp<Deployer>() {
            boolean backendJoined = false;

            @Mock
            public void deployFragments(DeployState deployState) throws Exception {
                if (!backendJoined) {
                    backendJoined = true;
                    UtFrameUtils.addMockBackend(NEW_BACKEND_ID, "127.0.0.5", 9060);
                }
                for (List<FragmentInstanceExecState> execStates : deployState.getThreeStageExecutionsToDeploy()) {
                    for (FragmentInstanceExecState execState : execStates) {
                        events.add("deploy:" + execState.getWorker().getId());
                    }
                }
            }
        };
    }

    @Test
    public void testSingleWorkerElasticQueryStillGrows() throws Exception {
        mockSharedDataLakeScan();
        mockSenderRegistration();
        captureDeploysAndJoinBackendMidQuery();

        startScheduling(SQL);

        // The query started on one worker but kept the incremental schedule (the guard), so the CN that
        // joined mid-query received a late scan instance. SingleNodeSchedule would have skipped the loop
        // and produced no such deploy.
        Assertions.assertTrue(events.stream().anyMatch(e -> e.startsWith("register:")), events.toString());
        Assertions.assertTrue(events.stream().anyMatch(e -> e.equals("deploy:" + NEW_BACKEND_ID)), events.toString());
    }
}
