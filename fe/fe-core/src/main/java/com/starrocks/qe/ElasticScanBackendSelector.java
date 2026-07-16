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

package com.starrocks.qe;

import com.starrocks.common.StarRocksException;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TScanRangeParams;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Assigns an incremental scan-range batch across the fragment's CURRENT instances by
 * assigned-range count, ignoring tablet ownership. Used for elastic-armed scans on incremental
 * (reuse) rounds: in shared-data mode any worker can serve any tablet, and instances added on
 * newly joined compute nodes own no tablets yet, so replica-driven selection would never route
 * work to them. Trades data-cache affinity on later batches for even utilization.
 */
public class ElasticScanBackendSelector implements BackendSelector {
    private final ScanNode scanNode;
    private final List<TScanRangeLocations> locations;
    private final FragmentScanRangeAssignment assignment;
    private final ExecutionFragment execFragment;

    public ElasticScanBackendSelector(ScanNode scanNode, List<TScanRangeLocations> locations,
                                      FragmentScanRangeAssignment assignment, ExecutionFragment execFragment) {
        this.scanNode = scanNode;
        this.locations = locations;
        this.assignment = assignment;
        this.execFragment = execFragment;
    }

    @Override
    public void computeScanRangeAssignment() throws StarRocksException {
        Map<ComputeNode, Integer> assignedPerWorker = new LinkedHashMap<>();
        for (FragmentInstance instance : execFragment.getInstances()) {
            assignedPerWorker.putIfAbsent(instance.getWorker(), 0);
        }
        if (assignedPerWorker.isEmpty()) {
            throw new StarRocksException("no instances to assign elastic scan ranges to");
        }

        for (TScanRangeLocations scanRangeLocations : locations) {
            ComputeNode minWorker = null;
            int minAssigned = Integer.MAX_VALUE;
            for (Map.Entry<ComputeNode, Integer> entry : assignedPerWorker.entrySet()) {
                if (entry.getValue() < minAssigned) {
                    minAssigned = entry.getValue();
                    minWorker = entry.getKey();
                }
            }
            assignedPerWorker.put(minWorker, minAssigned + 1);
            assignment.put(minWorker.getId(), scanNode.getId().asInt(),
                    new TScanRangeParams(scanRangeLocations.scan_range));
        }

        BackendSelector.appendIncrementalScanRangeSentinel(scanNode, assignedPerWorker.keySet(), assignment);
    }
}
