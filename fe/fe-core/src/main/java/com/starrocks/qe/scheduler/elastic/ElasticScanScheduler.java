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

package com.starrocks.qe.scheduler.elastic;

import com.google.common.collect.Lists;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.planner.DataStreamSink;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.ScanNode;
import com.starrocks.proto.PPlanFragmentCancelReason;
import com.starrocks.proto.PUniqueId;
import com.starrocks.proto.PUpdateExchangeSendersRequest;
import com.starrocks.proto.PUpdateExchangeSendersResult;
import com.starrocks.qe.scheduler.Deployer;
import com.starrocks.qe.scheduler.QueryRuntimeProfile;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.qe.scheduler.dag.ExecutionDAG;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.qe.scheduler.dag.FragmentInstanceExecState;
import com.starrocks.qe.scheduler.dag.JobSpec;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TPlanFragmentDestination;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeParams;
import com.starrocks.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.starrocks.qe.scheduler.dag.FragmentInstanceExecState.DeploymentResult;

/**
 * Grows eligible scan fragments onto compute nodes that join the warehouse while the query runs.
 *
 * <p>Runs on the schedule-task thread, serialized with incremental scan-range assignment — this
 * serialization is what makes the protocol safe: while the coordinator still holds undelivered
 * ranges, live scan instances have has_more=true and keep their exchange senders open, so
 * downstream receivers cannot finish while a sender is being registered.
 *
 * <p>An add is best-effort until the sender is registered downstream: registration failures
 * (old-version BE, receiver gone, timeout) abort the add with compensating unregistrations and
 * the query continues at its current width. Once the instance is deployed, failures follow the
 * regular deploy semantics.
 */
public class ElasticScanScheduler {
    private static final Logger LOG = LogManager.getLogger(ElasticScanScheduler.class);

    // ponytail: constants, promote to session variables only if real deployments need tuning.
    private static final int MAX_ADDED_INSTANCES = 8;
    private static final int MIN_REMAINING_BATCHES = 2;

    private final JobSpec jobSpec;
    private final ExecutionDAG dag;
    private final QueryRuntimeProfile profile;
    private final Supplier<WorkerProvider> workerCapture;
    private final Supplier<Boolean> queryTerminal;
    private final int scanRangeBatchSize;
    private final List<ExecutionFragment> eligibleFragments;

    private final List<FragmentInstance> preparedInstances = new ArrayList<>();
    // Deployed by the previous round's deployPreparedInstances, awaiting merge into the next
    // round's deploy states (schedule-thread only, like everything here).
    private final List<FragmentInstanceExecState> deployedExecStates = new ArrayList<>();
    // Workers whose add was aborted: one attempt per worker per query, otherwise a persistently
    // failing worker would be re-detected as "new" and retried every round.
    private final Set<Long> abortedWorkerIds = new HashSet<>();
    private int addedInstances = 0;
    private int abortedAdds = 0;

    public ElasticScanScheduler(JobSpec jobSpec, ExecutionDAG dag, QueryRuntimeProfile profile,
                                Supplier<WorkerProvider> workerCapture, Supplier<Boolean> queryTerminal,
                                int scanRangeBatchSize, List<ExecutionFragment> eligibleFragments) {
        this.jobSpec = jobSpec;
        this.dag = dag;
        this.profile = profile;
        this.workerCapture = workerCapture;
        this.queryTerminal = queryTerminal;
        this.scanRangeBatchSize = scanRangeBatchSize;
        this.eligibleFragments = eligibleFragments;
        LOG.info("elastic scan: armed for {} eligible fragment(s), query={}",
                eligibleFragments.size(), DebugUtil.printId(jobSpec.getQueryId()));
    }

    /**
     * Registers instances for newly joined workers, AFTER the round's scan-range assignment: the
     * new instance's initial deploy carries only a keep-alive sentinel and zero ranges, so an
     * aborted deploy loses nothing. It joins range assignment from the next round, once
     * {@link #deployPreparedInstances(Deployer)} has confirmed the deploy.
     */
    public void prepareInstances() {
        if (queryTerminal.get()) {
            return;
        }
        for (ExecutionFragment fragment : eligibleFragments) {
            if (addedInstances + preparedInstances.size() >= MAX_ADDED_INSTANCES) {
                return;
            }
            ScanNode scanNode = fragment.getScanNodes().iterator().next();
            boolean growable = hasGrowableWork(scanNode);
            List<ComputeNode> newWorkers = growable ? findNewWorkers(fragment) : Collections.emptyList();
            LOG.debug("elastic scan: round fragment={} hasMore={} growable={} newWorkers={} query={}",
                    fragment.getFragmentId(), scanNode.hasMoreScanRanges(), growable, newWorkers.size(),
                    DebugUtil.printId(jobSpec.getQueryId()));
            if (!growable) {
                continue;
            }
            for (ComputeNode worker : newWorkers) {
                if (addedInstances + preparedInstances.size() >= MAX_ADDED_INSTANCES) {
                    break;
                }
                FragmentInstance late = tryPrepareInstance(fragment, worker);
                if (late != null) {
                    preparedInstances.add(late);
                }
            }
        }
    }

    /**
     * Deploys the instances prepared this round with a sentinel-only request (has_more=true, zero
     * scan ranges), isolated from the coordinator's deploy failure handler: a failed or ambiguous
     * deploy aborts only the add — best-effort cancel of the maybe-started instance, compensating
     * sender unregistration (tombstoned on the BE), rollback of the DAG registration — and the
     * query continues at its current width. Successfully deployed instances are stashed and join
     * the next round's assignment through {@link #drainDeployedExecStates()}.
     */
    public void deployPreparedInstances(Deployer deployer) {
        if (preparedInstances.isEmpty()) {
            return;
        }
        long timeoutMs = jobSpec.getQueryOptions().getQuery_delivery_timeout() * 1000L;
        for (FragmentInstance instance : preparedInstances) {
            ExecutionFragment fragment = instance.getExecFragment();
            ScanNode scanNode = fragment.getScanNodes().iterator().next();
            TScanRangeParams sentinel = new TScanRangeParams();
            sentinel.setScan_range(new TScanRange());
            sentinel.setEmpty(true);
            sentinel.setHas_more(true);
            instance.addScanRanges(scanNode.getId().asInt(), Lists.newArrayList(sentinel));

            FragmentInstanceExecState execState = deployer.createLateInstanceExecState(instance);
            execState.deployAsync();
            DeploymentResult res = execState.waitForDeploymentCompletion(timeoutMs);
            if (res.getStatusCode() != TStatusCode.OK) {
                abortDeployedInstance(fragment, instance, execState, res);
                continue;
            }
            // The standard flag/recheck idiom: a cancel that raced this deploy may have fanned out
            // before the exec state was registered, so the orphan must be cancelled directly here.
            if (queryTerminal.get()) {
                abortDeployedInstance(fragment, instance, execState,
                        new DeploymentResult(TStatusCode.CANCELLED, "query finished during elastic add", null));
                continue;
            }
            addedInstances++;
            deployedExecStates.add(execState);
            LOG.info("elastic scan: added instance {} on worker {} for fragment {} of query {}",
                    DebugUtil.printId(instance.getInstanceId()), instance.getWorkerId(),
                    instance.getFragmentId(), DebugUtil.printId(jobSpec.getQueryId()));
        }
        preparedInstances.clear();
        profile.updateElasticScanInfo(addedInstances, abortedAdds);
    }

    /**
     * Hands over the exec states deployed by the previous round so the caller merges them into the
     * round's deploy states: they then receive scan ranges with every subsequent batch.
     */
    public List<FragmentInstanceExecState> drainDeployedExecStates() {
        if (deployedExecStates.isEmpty()) {
            return Collections.emptyList();
        }
        List<FragmentInstanceExecState> res = new ArrayList<>(deployedExecStates);
        deployedExecStates.clear();
        return res;
    }

    private void abortDeployedInstance(ExecutionFragment fragment, FragmentInstance instance,
                                       FragmentInstanceExecState execState, DeploymentResult res) {
        LOG.warn("elastic scan: late instance {} deploy failed on worker {}, aborting add, query={} status={}",
                DebugUtil.printId(instance.getInstanceId()), instance.getWorkerId(),
                DebugUtil.printId(jobSpec.getQueryId()), res.getStatus());
        // An ambiguous timeout may have actually started the instance: best-effort cancel first,
        // then compensate the sender registration (the unregister tombstones the be_number on the
        // BE, so even a register still in flight cannot resurrect it).
        execState.cancelFragmentInstance(PPlanFragmentCancelReason.INTERNAL_ERROR, "elastic scan add aborted");
        unregisterSenders(fragment, instance.getIndexInJob());
        abortedWorkerIds.add(instance.getWorkerId());
        dag.unregisterLateInstance(fragment, instance);
        profile.finishInstance(instance.getInstanceId());
        abortedAdds++;
    }

    private boolean hasGrowableWork(ScanNode scanNode) {
        if (!scanNode.hasMoreScanRanges()) {
            return false;
        }
        // OLAP exposes an exact remaining count, so skip growing near the end of delivery. Connector
        // sources (Iceberg/Hive) are streaming with no count; hasMoreScanRanges() is the only signal.
        if (scanNode instanceof OlapScanNode) {
            return ((OlapScanNode) scanNode).numRemainingScanRanges()
                    >= (long) MIN_REMAINING_BATCHES * scanRangeBatchSize;
        }
        return true;
    }

    private List<ComputeNode> findNewWorkers(ExecutionFragment fragment) {
        Set<Long> currentWorkers = fragment.getInstances().stream()
                .map(FragmentInstance::getWorkerId)
                .collect(Collectors.toSet());
        try {
            WorkerProvider provider = workerCapture.get();
            return provider.getAllWorkers().stream()
                    .filter(worker -> !currentWorkers.contains(worker.getId()))
                    .filter(worker -> !abortedWorkerIds.contains(worker.getId()))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            // e.g. the warehouse currently has no alive nodes; nothing to add this round.
            LOG.debug("elastic scan: capturing workers failed for query {}",
                    DebugUtil.printId(jobSpec.getQueryId()), e);
            return Collections.emptyList();
        }
    }

    private FragmentInstance tryPrepareInstance(ExecutionFragment fragment, ComputeNode worker) {
        int beNumber = dag.reserveLateIndexInJob();
        if (!registerSenders(fragment, beNumber)) {
            unregisterSenders(fragment, beNumber);
            abortedWorkerIds.add(worker.getId());
            abortedAdds++;
            profile.updateElasticScanInfo(addedInstances, abortedAdds);
            return null;
        }
        FragmentInstance late;
        try {
            late = dag.registerLateInstance(fragment, worker, beNumber);
        } catch (Exception e) {
            // e.g. the fragment builds runtime filters (guarded again inside registerLateInstance);
            // an add must abort with compensation, never propagate and fail the query.
            LOG.warn("elastic scan: late instance registration failed for query {}",
                    DebugUtil.printId(jobSpec.getQueryId()), e);
            unregisterSenders(fragment, beNumber);
            abortedWorkerIds.add(worker.getId());
            abortedAdds++;
            profile.updateElasticScanInfo(addedInstances, abortedAdds);
            return null;
        }
        if (!profile.attachInstance(late.getInstanceId())) {
            // The query already finished; nothing further will be scheduled for it. Roll the
            // instance back out of the DAG so nothing ever assigns ranges to it.
            unregisterSenders(fragment, beNumber);
            dag.unregisterLateInstance(fragment, late);
            abortedAdds++;
            return null;
        }
        return late;
    }

    private boolean registerSenders(ExecutionFragment fragment, int beNumber) {
        int exchNodeId = ((DataStreamSink) fragment.getPlanFragment().getSink()).getExchNodeId().asInt();
        long timeoutMs = jobSpec.getQueryOptions().getQuery_delivery_timeout() * 1000L;
        List<Future<PUpdateExchangeSendersResult>> futures = new ArrayList<>();
        try {
            for (TPlanFragmentDestination dest : fragment.getDestinations()) {
                futures.add(BackendServiceClient.getInstance().updateExchangeSenders(
                        dest.getBrpc_server(), buildRequest(dest, exchNodeId, beNumber, false)));
            }
            for (Future<PUpdateExchangeSendersResult> future : futures) {
                PUpdateExchangeSendersResult result = future.get(timeoutMs, TimeUnit.MILLISECONDS);
                if (result.status == null || result.status.statusCode == null
                        || result.status.statusCode != TStatusCode.OK.getValue()) {
                    LOG.warn("elastic scan: sender registration rejected, be_number={} query={} status={}",
                            beNumber, DebugUtil.printId(jobSpec.getQueryId()),
                            result.status == null ? "null" : result.status.errorMsgs);
                    return false;
                }
            }
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (Exception e) {
            // Includes method-not-found from an old-version BE: elastic is opportunistic.
            LOG.warn("elastic scan: sender registration failed, be_number={} query={}",
                    beNumber, DebugUtil.printId(jobSpec.getQueryId()), e);
            return false;
        }
    }

    private void unregisterSenders(ExecutionFragment fragment, int beNumber) {
        // Best-effort compensation; unregistering a never-registered sender tombstones the
        // be_number on the BE, so even a register still in flight cannot resurrect it. A lost
        // unregister leaves a phantom sender that hangs the receiver until query timeout, so
        // retry once per destination before giving up.
        int exchNodeId = ((DataStreamSink) fragment.getPlanFragment().getSink()).getExchNodeId().asInt();
        for (TPlanFragmentDestination dest : fragment.getDestinations()) {
            for (int attempt = 0; attempt < 2; attempt++) {
                try {
                    BackendServiceClient.getInstance().updateExchangeSenders(
                            dest.getBrpc_server(), buildRequest(dest, exchNodeId, beNumber, true));
                    break;
                } catch (Exception e) {
                    LOG.warn("elastic scan: sender unregistration failed, attempt={} be_number={} query={}",
                            attempt + 1, beNumber, DebugUtil.printId(jobSpec.getQueryId()), e);
                }
            }
        }
    }

    private static PUpdateExchangeSendersRequest buildRequest(TPlanFragmentDestination dest, int exchNodeId,
                                                              int beNumber, boolean unregister) {
        PUpdateExchangeSendersRequest request = new PUpdateExchangeSendersRequest();
        PUniqueId finstId = new PUniqueId();
        finstId.hi = dest.getFragment_instance_id().getHi();
        finstId.lo = dest.getFragment_instance_id().getLo();
        request.finstId = finstId;
        request.nodeId = exchNodeId;
        request.beNumber = beNumber;
        request.unregister = unregister;
        return request;
    }
}
