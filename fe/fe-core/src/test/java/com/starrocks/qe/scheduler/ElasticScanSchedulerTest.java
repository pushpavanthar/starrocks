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
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.scheduler.dag.FragmentInstanceExecState;
import com.starrocks.qe.scheduler.elastic.ElasticScanEligibility;
import com.starrocks.qe.scheduler.slot.DeployState;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPlanFragmentExecParams;
import com.starrocks.thrift.TScanRangeParams;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class ElasticScanSchedulerTest extends SchedulerTestBase {
    private static final String SQL = "select L_ORDERKEY from lineitem";
    private static final int LINEITEM_TABLETS = 20;
    private static final int NEW_BACKEND_ID = 10004;

    private final List<String> events = new ArrayList<>();
    private final List<PUpdateExchangeSendersRequest> senderRequests = new ArrayList<>();
    private boolean rejectSenderRegistration = false;

    @BeforeEach
    public void before() {
        Config.enable_elastic_scan_execution = true;
        connectContext.getSessionVariable().setEnableOlapIncrementalScanRanges(true);
        connectContext.getSessionVariable().setEnableElasticScanStages(true);
        connectContext.getSessionVariable().setConnectorIncrementalScanRangeNumber(4);
        events.clear();
        senderRequests.clear();
        rejectSenderRegistration = false;
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
        connectContext.getSessionVariable().setEnablePhasedScheduler(false);
    }

    /** Mimic a shared-data lake scan in the shared-nothing harness (see IncrementalDeployOlapTest). */
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
                senderRequests.add(request);
                events.add((Boolean.TRUE.equals(request.unregister) ? "unregister:" : "register:") + request.beNumber);
                PUpdateExchangeSendersResult result = new PUpdateExchangeSendersResult();
                StatusPB status = new StatusPB();
                status.statusCode = rejectSenderRegistration && !Boolean.TRUE.equals(request.unregister)
                        ? TStatusCode.INTERNAL_ERROR.getValue() : TStatusCode.OK.getValue();
                status.errorMsgs = new ArrayList<>();
                result.status = status;
                return CompletableFuture.completedFuture(result);
            }
        };
    }

    /**
     * Captures deploy requests and lets the new backend "join the warehouse" on the first deploy
     * wave — after the prepare-time worker snapshot, so it is genuinely new to the query.
     */
    private List<TExecPlanFragmentParams> captureDeploysAndJoinBackendMidQuery() {
        List<TExecPlanFragmentParams> requests = new ArrayList<>();
        new MockUp<Deployer>() {
            boolean backendJoined = false;

            @Mock
            public void deployFragments(DeployState deployState) throws Exception {
                if (!backendJoined) {
                    backendJoined = true;
                    UtFrameUtils.addMockBackend(NEW_BACKEND_ID, "127.0.0.4", 9060);
                }
                for (List<FragmentInstanceExecState> execStates : deployState.getThreeStageExecutionsToDeploy()) {
                    for (FragmentInstanceExecState execState : execStates) {
                        events.add("deploy:" + execState.getWorker().getId());
                        requests.add(execState.getRequestToDeploy().deepCopy());
                    }
                }
            }
        };
        return requests;
    }

    private static List<TScanRangeParams> collectScanRanges(TPlanFragmentExecParams params) {
        List<TScanRangeParams> res = new ArrayList<>();
        if (params.isSetPer_node_scan_ranges()) {
            params.getPer_node_scan_ranges().values().forEach(res::addAll);
        }
        if (params.isSetNode_to_per_driver_seq_scan_ranges()) {
            params.getNode_to_per_driver_seq_scan_ranges().values()
                    .forEach(perDriverSeq -> perDriverSeq.values().forEach(res::addAll));
        }
        return res;
    }

    private static Set<Long> collectTabletIds(List<TExecPlanFragmentParams> requests) {
        Set<Long> tabletIds = new HashSet<>();
        for (TExecPlanFragmentParams request : requests) {
            for (TScanRangeParams p : collectScanRanges(request.params)) {
                if (!p.isEmpty() && p.getScan_range().isSetInternal_scan_range()) {
                    tabletIds.add(p.getScan_range().getInternal_scan_range().getTablet_id());
                }
            }
        }
        return tabletIds;
    }

    @Test
    public void testAddsInstanceOnNewWorker() throws Exception {
        mockSharedDataLakeScan();
        mockSenderRegistration();
        List<TExecPlanFragmentParams> deploys = captureDeploysAndJoinBackendMidQuery();

        DefaultCoordinator coordinator = startScheduling(SQL);
        Assertions.assertFalse(
                ElasticScanEligibility.findEligibleFragments(connectContext, coordinator.getJobSpec(),
                        coordinator.getExecutionDAG()).isEmpty());

        // The sender was registered on the downstream exchange, never unregistered.
        Assertions.assertFalse(senderRequests.isEmpty());
        Assertions.assertTrue(senderRequests.stream().noneMatch(r -> Boolean.TRUE.equals(r.unregister)));

        // Sender registration must precede the first deploy to the new worker.
        int registerIndex = events.indexOf(events.stream()
                .filter(e -> e.startsWith("register:")).findFirst().orElse(null));
        int lateIndex = events.indexOf("deploy:" + NEW_BACKEND_ID);
        Assertions.assertTrue(lateIndex >= 0, "no deploy to the new worker: " + events);
        Assertions.assertTrue(registerIndex >= 0 && registerIndex < lateIndex,
                "sender registration must precede the late deploy: " + events);

        // The new worker's deploy request is a full deploy carrying scan ranges plus a sentinel.
        List<TExecPlanFragmentParams> lateDeploys = new ArrayList<>();
        int deployIndex = 0;
        for (String event : events) {
            if (event.startsWith("deploy:")) {
                if (event.equals("deploy:" + NEW_BACKEND_ID)) {
                    lateDeploys.add(deploys.get(deployIndex));
                }
                deployIndex++;
            }
        }
        Assertions.assertFalse(lateDeploys.isEmpty());
        TExecPlanFragmentParams first = lateDeploys.get(0);
        Assertions.assertTrue(first.isSetFragment(), "late deploy must carry the plan fragment");
        List<TScanRangeParams> ranges = collectScanRanges(first.params);
        Assertions.assertTrue(ranges.stream().anyMatch(p -> !p.isEmpty()), "late deploy carries scan ranges");
        Assertions.assertTrue(ranges.stream().anyMatch(TScanRangeParams::isEmpty), "late deploy carries a sentinel");

        // Every tablet is still delivered exactly once across all instances.
        Assertions.assertEquals(LINEITEM_TABLETS, collectTabletIds(deploys).size());
    }

    @Test
    public void testRegistrationFailureAbortsAddAndQueryContinues() throws Exception {
        mockSharedDataLakeScan();
        mockSenderRegistration();
        rejectSenderRegistration = true;
        List<TExecPlanFragmentParams> deploys = captureDeploysAndJoinBackendMidQuery();

        startScheduling(SQL);

        // Compensating unregistrations were sent, the new worker was never deployed to,
        // and the query still delivered all tablets on its original instances.
        Assertions.assertTrue(events.stream().anyMatch(e -> e.startsWith("unregister:")), events.toString());
        Assertions.assertTrue(events.stream().noneMatch(e -> e.equals("deploy:" + NEW_BACKEND_ID)), events.toString());
        Assertions.assertEquals(LINEITEM_TABLETS, collectTabletIds(deploys).size());
    }

    @Test
    public void testFlagsOffNoElasticActivity() throws Exception {
        mockSharedDataLakeScan();
        mockSenderRegistration();
        connectContext.getSessionVariable().setEnableElasticScanStages(false);
        List<TExecPlanFragmentParams> deploys = captureDeploysAndJoinBackendMidQuery();

        startScheduling(SQL);

        Assertions.assertTrue(senderRequests.isEmpty());
        Assertions.assertTrue(events.stream().noneMatch(e -> e.equals("deploy:" + NEW_BACKEND_ID)));
        Assertions.assertEquals(LINEITEM_TABLETS, collectTabletIds(deploys).size());
    }

    @Test
    public void testMergingExchangeIneligible() throws Exception {
        mockSharedDataLakeScan();
        mockSenderRegistration();
        captureDeploysAndJoinBackendMidQuery();

        DefaultCoordinator coordinator = startScheduling(SQL + " order by L_ORDERKEY limit 100000");
        Assertions.assertTrue(
                ElasticScanEligibility.findEligibleFragments(connectContext, coordinator.getJobSpec(),
                        coordinator.getExecutionDAG()).isEmpty());
        Assertions.assertTrue(senderRequests.isEmpty());
    }

    @Test
    public void testPhasedSchedulerIneligible() throws Exception {
        mockSharedDataLakeScan();
        mockSenderRegistration();
        connectContext.getSessionVariable().setEnablePhasedScheduler(true);
        captureDeploysAndJoinBackendMidQuery();

        startScheduling(SQL);
        Assertions.assertTrue(senderRequests.isEmpty());
    }
}
