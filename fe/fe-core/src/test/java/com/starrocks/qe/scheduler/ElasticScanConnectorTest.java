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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * Elastic growth for a connector (Iceberg/Hive) scan: a fragment whose single scan node routes
 * through HDFSBackendSelector must grow onto a CN that joins mid-query, receiving connector scan
 * ranges on the reuse round via {@code ElasticScanBackendSelector}.
 */
public class ElasticScanConnectorTest extends SchedulerConnectorTestBase {
    private static final String SQL = "select * from hive0.file_split_db.file_split_tbl";
    private static final int NEW_BACKEND_ID = 10006;

    private final List<String> events = new ArrayList<>();
    private final List<PUpdateExchangeSendersRequest> senderRequests = new ArrayList<>();

    @BeforeEach
    public void before() {
        Config.enable_elastic_scan_execution = true;
        connectContext.getSessionVariable().setEnableConnectorIncrementalScanRanges(true);
        connectContext.getSessionVariable().setEnableElasticScanStages(true);
        connectContext.getSessionVariable().setConnectorIncrementalScanRangeNumber(4);
        events.clear();
        senderRequests.clear();
    }

    @AfterEach
    public void after() throws Exception {
        try {
            UtFrameUtils.dropMockBackend(NEW_BACKEND_ID);
        } catch (Exception e) {
            // the test may not have added the backend
        }
        Config.enable_elastic_scan_execution = false;
        connectContext.getSessionVariable().setEnableConnectorIncrementalScanRanges(false);
        connectContext.getSessionVariable().setEnableElasticScanStages(false);
    }

    /** Mimic a shared-data worker provider so any CN can serve any connector split. */
    private static void mockSharedData() {
        new MockUp<DefaultWorkerProvider>() {
            @Mock
            public boolean allowUsingBackupNode() {
                return true;
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
                status.statusCode = TStatusCode.OK.getValue();
                status.errorMsgs = new ArrayList<>();
                result.status = status;
                return CompletableFuture.completedFuture(result);
            }
        };
    }

    private List<TExecPlanFragmentParams> captureDeploysAndJoinBackendMidQuery() {
        List<TExecPlanFragmentParams> requests = new ArrayList<>();
        new MockUp<Deployer>() {
            boolean backendJoined = false;

            @Mock
            public void deployFragments(DeployState deployState) throws Exception {
                if (!backendJoined) {
                    backendJoined = true;
                    UtFrameUtils.addMockBackend(NEW_BACKEND_ID, "127.0.0.6", 9060);
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

    @Test
    public void testAddsConnectorInstanceOnNewWorker() throws Exception {
        mockSharedData();
        mockSenderRegistration();
        List<TExecPlanFragmentParams> deploys = captureDeploysAndJoinBackendMidQuery();

        DefaultCoordinator coordinator = startScheduling(SQL);
        Assertions.assertTrue(coordinator.getJobSpec().isIncrementalScanRanges());
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

        // The new worker's deploys carry connector (hdfs) scan ranges (across creation + reuse
        // rounds) plus the has_more sentinel — proving the late instance receives connector work.
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
        Assertions.assertTrue(lateDeploys.stream().noneMatch(TExecPlanFragmentParams::isSetFragment),
                "late instance's captured deploys are incremental requests; the sentinel-only "
                        + "initial deploy bypasses deployFragments");
        List<TScanRangeParams> allLateRanges = new ArrayList<>();
        lateDeploys.forEach(d -> allLateRanges.addAll(collectScanRanges(d.params)));
        Assertions.assertTrue(
                allLateRanges.stream().anyMatch(p -> !p.isEmpty() && p.scan_range.isSetHdfs_scan_range()),
                "late instance receives connector scan ranges");
        Assertions.assertTrue(allLateRanges.stream().anyMatch(TScanRangeParams::isEmpty),
                "late deploy carries a sentinel");
    }
}
