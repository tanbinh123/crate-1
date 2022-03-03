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

package io.crate.execution.engine.aggregation;

import io.crate.breaker.RamAccounting;
import io.crate.breaker.SizeEstimator;
import io.crate.types.ByteType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.IntegerType;
import io.crate.types.LongType;
import io.crate.types.ShortType;
import io.crate.types.TimestampType;
import it.unimi.dsi.fastutil.bytes.Byte2ObjectOpenCustomHashMap;
import it.unimi.dsi.fastutil.bytes.ByteHash;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenCustomHashMap;
import it.unimi.dsi.fastutil.ints.IntHash;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenCustomHashMap;
import it.unimi.dsi.fastutil.longs.LongHash;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.shorts.Short2ObjectOpenCustomHashMap;
import it.unimi.dsi.fastutil.shorts.ShortHash;
import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.lucene.util.RamUsageEstimator;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public final class GroupByMaps {

    private static final Map<DataType<?>, Integer> ENTRY_OVERHEAD_PER_TYPE = Map.ofEntries(
        Map.entry(DataTypes.BYTE, 6),
        Map.entry(DataTypes.SHORT, 6),
        Map.entry(DataTypes.INTEGER, 8),
        Map.entry(DataTypes.LONG, 12)
    );

    private static final XXHash32 xxHash32 = XXHashFactory.fastestInstance().hash32();

    private static final class ByteHashStrategy implements ByteHash.Strategy {

        private static final ByteHashStrategy INSTANCE = new ByteHashStrategy();

        @Override
        public int hashCode(byte value) {
            return xxHash32.hash(new byte[]{value}, 0, Byte.BYTES, 0);
        }

        @Override
        public boolean equals(byte a, byte b) {
            return a == b;
        }
    }

    private static final class ShortHashStrategy implements ShortHash.Strategy {

        private static final ShortHashStrategy INSTANCE = new ShortHashStrategy();

        @Override
        public int hashCode(short value) {
            return xxHash32.hash(new byte[] {
                (byte)(value >> 8),
                (byte)value}, 0, Short.BYTES, 0);
        }

        @Override
        public boolean equals(short a, short b) {
            return a == b;
        }
    }

    private static final class IntHashStrategy implements IntHash.Strategy {

        private static final IntHashStrategy INSTANCE = new IntHashStrategy();

        @Override
        public int hashCode(int value) {
            return xxHash32.hash(new byte[] {
                (byte)(value >> 24),
                (byte)(value >> 16),
                (byte)(value >> 8),
                (byte)value}, 0, Integer.BYTES, 0);
        }

        @Override
        public boolean equals(int a, int b) {
            return a == b;
        }
    }

    private static final class LongHashStrategy implements LongHash.Strategy {

        private static final LongHashStrategy INSTANCE = new LongHashStrategy();

        @Override
        public int hashCode(long value) {
            return xxHash32.hash(new byte[] {
                (byte)(value >> 56),
                (byte)(value >> 48),
                (byte)(value >> 40),
                (byte)(value >> 32),
                (byte)(value >> 24),
                (byte)(value >> 16),
                (byte)(value >> 8),
                (byte)value}, 0, Long.BYTES, 0);
        }

        @Override
        public boolean equals(long a, long b) {
            return a == b;
        }
    }

    public static <K, V> BiConsumer<Map<K, V>, K> accountForNewEntry(RamAccounting ramAccounting,
                                                                     SizeEstimator<K> sizeEstimator,
                                                                     @Nullable DataType<K> type) {
        Integer entryOverHead = type == null ? null : ENTRY_OVERHEAD_PER_TYPE.get(type);
        if (entryOverHead == null) {
            return (map, k) -> ramAccounting.addBytes(RamUsageEstimator.alignObjectSize(sizeEstimator.estimateSize(k) + 36));
        } else {
            return (map, k) -> {
                int mapSize = map.size();
                // If mapSize is a power of 2 then the map is going to grow by doubling its size.
                if (mapSize >= 4 && (mapSize & (mapSize - 1)) == 0) {
                    ramAccounting.addBytes(mapSize * (long) entryOverHead);
                }
            };
        }
    }

    public static <K, V> Supplier<Map<K, V>> mapForType(DataType<K> type) {

        switch (type.id()) {
            case ByteType.ID:
                return () -> new PrimitiveMapWithNulls(new Byte2ObjectOpenCustomHashMap<V>(ByteHashStrategy.INSTANCE));
            case ShortType.ID:
                return () -> new PrimitiveMapWithNulls(new Short2ObjectOpenCustomHashMap<V>(ShortHashStrategy.INSTANCE));
            case IntegerType.ID:
                return () -> new PrimitiveMapWithNulls(new Int2ObjectOpenCustomHashMap(IntHashStrategy.INSTANCE));

            case LongType.ID:
            case TimestampType.ID_WITH_TZ:
            case TimestampType.ID_WITHOUT_TZ:

                return () -> new PrimitiveMapWithNulls(new Long2ObjectOpenCustomHashMap<V>(LongHashStrategy.INSTANCE));

            default:
                return Object2ObjectOpenHashMap::new;
        }
    }
}
