/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.execution.engine.indexing;

import io.crate.breaker.RamAccounting;
import io.crate.execution.dml.ShardRequest;

import io.crate.sql.tree.ArraySubQueryExpression;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.index.shard.ShardId;

import java.util.*;
import java.util.function.Function;

public final class ShardedRequests<TReq extends ShardRequest<TReq, TItem>, TItem extends ShardRequest.Item>
    implements Accountable, Releasable {

    final Map<String, List<ItemAndRoutingAndSourceInfo<TItem>>> itemsByMissingIndex = new HashMap<>();
    final Map<String, Set<ReadFailureAndLineNumber>> itemsWithFailureBySourceUri = new HashMap<>();
    final List<RowSourceInfo> rowSourceInfos = new ArrayList<>();
    final Map<ShardLocation, TReq> itemsByShard = new HashMap<>();

    private final Function<ShardId, TReq> requestFactory;
    private final RamAccounting ramAccounting;

    private int location = -1;
    private long usedMemoryEstimate = 0L;

    /**
     * @param requestFactory function to create a request
     */
    public ShardedRequests(Function<ShardId, TReq> requestFactory, RamAccounting ramAccounting) {
        this.requestFactory = requestFactory;
        this.ramAccounting = ramAccounting;
    }

    /**
     * @param itemSizeInBytes an estimate of how many bytes the item occupies in memory
     */
    public void add(TItem item, long itemSizeInBytes, String indexName, String routing, RowSourceInfo rowSourceInfo) {
        ramAccounting.addBytes(itemSizeInBytes);
        usedMemoryEstimate += itemSizeInBytes;
        List<ItemAndRoutingAndSourceInfo<TItem>> items = itemsByMissingIndex.computeIfAbsent(indexName, k -> new ArrayList<>());
        items.add(new ItemAndRoutingAndSourceInfo<>(item, routing, rowSourceInfo));
    }

    public void add(TItem item, long itemSizeInBytes, ShardLocation shardLocation, RowSourceInfo rowSourceInfo) {
        ramAccounting.addBytes(itemSizeInBytes);
        usedMemoryEstimate += itemSizeInBytes;
        TReq req = itemsByShard.get(shardLocation);
        if (req == null) {
            req = requestFactory.apply(shardLocation.shardId);
            itemsByShard.put(shardLocation, req);
        }
        location++;
        req.add(location, item);
        rowSourceInfos.add(rowSourceInfo);
    }

    void addFailedItem(String sourceUri, String readFailure, Long lineNumber) {
        Set<ReadFailureAndLineNumber> itemsWithFailure = itemsWithFailureBySourceUri.computeIfAbsent(
            sourceUri, k -> new HashSet<>());
        itemsWithFailure.add(new ReadFailureAndLineNumber(readFailure, lineNumber));
    }

    @Override
    public long ramBytesUsed() {
        return usedMemoryEstimate;
    }

    public Map<String, List<ItemAndRoutingAndSourceInfo<TItem>>> itemsByMissingIndex() {
        return itemsByMissingIndex;
    }

    public Map<ShardLocation, TReq> itemsByShard() {
        return itemsByShard;
    }

    public static class ItemAndRoutingAndSourceInfo<TItem> {
        final TItem item;
        final String routing;
        final RowSourceInfo rowSourceInfo;

        ItemAndRoutingAndSourceInfo(TItem item, String routing, RowSourceInfo rowSourceInfo) {
            this.item = item;
            this.routing = routing;
            this.rowSourceInfo = rowSourceInfo;
        }

        public String routing() {
            return routing;
        }

        public TItem item() {
            return item;
        }
    }

    static class ReadFailureAndLineNumber {
        final String readFailure;
        final long lineNumber;

        ReadFailureAndLineNumber(String readFailure, long lineNumber) {
            this.readFailure = readFailure;
            this.lineNumber = lineNumber;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            ReadFailureAndLineNumber that = (ReadFailureAndLineNumber) obj;
            return Objects.equals(this.readFailure, that.readFailure);
        }

        @Override
        public int hashCode() {
            return readFailure.hashCode();
        }
    }

    @Override
    public void close() {
        ramAccounting.addBytes(-usedMemoryEstimate);
        usedMemoryEstimate = 0L;
    }
}
