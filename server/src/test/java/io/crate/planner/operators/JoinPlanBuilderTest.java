/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.planner.operators;

import static org.hamcrest.Matchers.is;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.relations.JoinPair;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;

public class JoinPlanBuilderTest extends CrateDummyClusterServiceUnitTest {

    private SqlExpressions sqlExpressions;

    @Before
    public void prepare() throws Exception {
        sqlExpressions = new SqlExpressions(T3.sources(clusterService));
    }

    @Test
    public void test_extract_constant_join_conditions() {
        Symbol joinCondition = sqlExpressions.asSymbol("t1.x = t2.y AND t1.x = 4");
        var join = JoinPair.of(new RelationName(null, "t1"), new RelationName(null, "t2"), JoinType.INNER, joinCondition);
        var constantConditions =  new HashMap<Set<RelationName>, Symbol>();
        var processedJoin = JoinPlanBuilder.extractConstantJoinConditions(join, constantConditions);
        assertThat(processedJoin.condition(), is(sqlExpressions.asSymbol("t1.x = t2.y")));
        assertThat(constantConditions, is(Map.of(Set.of(new RelationName("doc", "t1")), sqlExpressions.asSymbol("x = 4"))));
    }

    @Test
    public void test_skip_non_constant_join_conditions() {
        Symbol joinCondition = sqlExpressions.asSymbol("t1.x = t2.y AND t1.i = t2.i");
        var join = JoinPair.of(new RelationName(null, "t1"), new RelationName(null, "t2"), JoinType.INNER, joinCondition);
        var constantConditions =  new HashMap<Set<RelationName>, Symbol>();
        var processedJoin = JoinPlanBuilder.extractConstantJoinConditions(join, constantConditions);
        assertThat(processedJoin, is(processedJoin));
        assertThat(constantConditions, is(Map.of()));
    }
}
