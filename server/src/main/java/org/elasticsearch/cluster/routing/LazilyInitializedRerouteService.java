/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.routing;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Priority;

/**
 * A {@link RerouteService} that can be initialized lazily. The real reroute service, {@link BatchedRerouteService}, depends on components
 * constructed quite late in the construction of the node, but other components constructed earlier eventually need access to the reroute
 * service too.
 */
public class LazilyInitializedRerouteService implements RerouteService {

    private final SetOnce<RerouteService> delegate = new SetOnce<>();

    @Override
    public void reroute(String reason, Priority priority, ActionListener<ClusterState> listener) {
        assert delegate.get() != null;
        delegate.get().reroute(reason, priority, listener);
    }

    public void setRerouteService(RerouteService rerouteService) {
        delegate.set(rerouteService);
    }
}
