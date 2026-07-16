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

import com.starrocks.common.Config;
import com.starrocks.planner.DataSink;
import com.starrocks.planner.DataStreamSink;
import com.starrocks.planner.ExchangeNode;
import com.starrocks.planner.MultiCastPlanFragment;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.scheduler.dag.ExecutionDAG;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.JobSpec;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Decides which fragments of a query may grow scan instances on compute nodes that join the
 * warehouse mid-query. Evaluated once at prepare; per-round decisions (has more work, new node
 * present) live in {@link ElasticScanScheduler}.
 */
public final class ElasticScanEligibility {
    private ElasticScanEligibility() {
    }

    public static List<ExecutionFragment> findEligibleFragments(ConnectContext context, JobSpec jobSpec,
                                                                ExecutionDAG dag) {
        SessionVariable sessionVariable = context.getSessionVariable();
        if (!Config.enable_elastic_scan_execution
                || !sessionVariable.isEnableElasticScanStages()
                || !sessionVariable.isEnableOlapIncrementalScanRanges()
                // The phased scheduler counts scheduling instances per fragment at prepare;
                // a late instance's finish would corrupt that accounting. Follow-up.
                || sessionVariable.enablePhasedScheduler()
                || !jobSpec.isEnablePipeline()
                || !jobSpec.isIncrementalScanRanges()
                || jobSpec.isLoadType()) {
            return Collections.emptyList();
        }

        List<ExecutionFragment> res = new ArrayList<>();
        for (ExecutionFragment fragment : dag.getFragmentsInPostorder()) {
            if (isEligibleFragment(fragment)) {
                res.add(fragment);
            }
        }
        return res;
    }

    private static boolean isEligibleFragment(ExecutionFragment fragment) {
        // A single plain OLAP scan; colocated/bucket/replicated/local-native scans never arm
        // incremental delivery, so they self-exclude at runtime through hasMoreScanRanges().
        Collection<ScanNode> scanNodes = fragment.getScanNodes();
        if (scanNodes.size() != 1 || !(scanNodes.iterator().next() instanceof OlapScanNode)) {
            return false;
        }

        PlanFragment planFragment = fragment.getPlanFragment();
        if (planFragment instanceof MultiCastPlanFragment) {
            return false;
        }
        // Only a plain stream sink feeds a single growable exchange; result and table sinks are
        // not exchanges, and split sinks fan out to several.
        DataSink sink = planFragment.getSink();
        if (!(sink instanceof DataStreamSink)) {
            return false;
        }
        // A merging receiver has a fixed one-queue-per-sender layout that cannot grow.
        ExchangeNode destNode = planFragment.getDestNode();
        if (destNode == null || destNode.isMerge()) {
            return false;
        }
        // Global-runtime-filter merge accounting is sized by the plan-time instance count.
        return planFragment.getBuildRuntimeFilters().isEmpty();
    }
}
