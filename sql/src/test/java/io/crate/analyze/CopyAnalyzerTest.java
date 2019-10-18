/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.analyze;

import io.crate.data.RowN;
import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.exceptions.PartitionUnknownException;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.SchemaUnknownException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.execution.dsl.phases.FileUriCollectPhase;
import io.crate.execution.dsl.projection.WriterProjection;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.planner.statement.CopyFromPlan;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static io.crate.analyze.TableDefinitions.TEST_PARTITIONED_TABLE_IDENT;
import static io.crate.analyze.TableDefinitions.USER_TABLE_IDENT;
import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;
import static io.crate.testing.SymbolMatchers.isReference;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;

public class CopyAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private PlannerContext plannerContext;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.builder(clusterService).enableDefaultTables().build();
        plannerContext = e.getPlannerContext(clusterService.state());
    }

    @SuppressWarnings("unchecked")
    private <S> S analyze(String stmt, Object... arguments) {
        AnalyzedStatement analyzedStatement = e.analyze(stmt);
        if (analyzedStatement instanceof AnalyzedCopyFrom) {
            return (S) CopyFromPlan.createStatement(
                (AnalyzedCopyFrom) analyzedStatement,
                plannerContext.transactionContext(),
                plannerContext.functions(),
                new RowN(arguments),
                SubQueryResults.EMPTY
            );
        } else {
            return (S) analyzedStatement;
        }
    }

    @Test
    public void testCopyFromExistingTable() throws Exception {
        BoundedCopyFrom analysis = analyze(
            "COPY users FROM '/some/distant/file.ext'");
        assertThat(analysis.tableInfo().ident(), is(USER_TABLE_IDENT));
        assertThat(analysis.uri(), isLiteral("/some/distant/file.ext"));
    }

    @Test
    public void testCopyFromExistingPartitionedTable() {
        BoundedCopyFrom analysis = analyze("COPY parted FROM '/some/distant/file.ext'");
        assertThat(analysis.tableInfo().ident(), is(TEST_PARTITIONED_TABLE_IDENT));
        assertThat(analysis.uri(), isLiteral("/some/distant/file.ext"));
    }

    @Test
    public void testCopyFromPartitionedTablePARTITIONKeywordTooManyArgs() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        analyze("COPY parted PARTITION (a=1, b=2, c=3) FROM '/some/distant/file.ext'");
    }

    @Test
    public void testCopyFromPartitionedTablePARTITIONKeywordValidArgs() throws Exception {
        BoundedCopyFrom analysis = analyze(
            "COPY parted PARTITION (date=1395874800000) FROM '/some/distant/file.ext'");
        String parted = new PartitionName(
            new RelationName("doc", "parted"), Collections.singletonList("1395874800000")).ident();
        assertThat(analysis.partitionIdent(), equalTo(parted));
    }

    @Test
    public void testCopyFromNonExistingTable() throws Exception {
        expectedException.expect(RelationUnknown.class);
        analyze("COPY unknown FROM '/some/distant/file.ext'");
    }

    @Test
    public void testCopyFromSystemTable() throws Exception {
        expectedException.expect(OperationOnInaccessibleRelationException.class);
        expectedException.expectMessage("The relation \"sys.shards\" doesn't support or allow INSERT "+
                                        "operations, as it is read-only.");
        analyze("COPY sys.shards FROM '/nope/nope/still.nope'");
    }

    @Test
    public void testCopyFromUnknownSchema() throws Exception {
        expectedException.expect(SchemaUnknownException.class);
        analyze("COPY suess.shards FROM '/nope/nope/still.nope'");
    }

    @Test
    public void testCopyFromParameter() throws Exception {
        String path = "/some/distant/file.ext";
        BoundedCopyFrom analysis = analyze("COPY users FROM ?", new Object[]{path});
        assertThat(analysis.tableInfo().ident(), is(USER_TABLE_IDENT));
        assertThat(analysis.uri(), isLiteral(path));
    }

    @Test
    public void convertCopyFrom_givenFormatIsSetToJsonInStatement_thenInputFormatIsSetToJson() {
        BoundedCopyFrom analysis = analyze(
            "COPY users FROM '/some/distant/file.ext' WITH (format='json')");
        assertThat(analysis.inputFormat(), is(FileUriCollectPhase.InputFormat.JSON));
    }

    @Test
    public void convertCopyFrom_givenFormatIsSetToCsvInStatement_thenInputFormatIsSetToCsv() {
        BoundedCopyFrom analysis = analyze(
            "COPY users FROM '/some/distant/file.ext' WITH (format='csv')");
        assertThat(analysis.inputFormat(), is(FileUriCollectPhase.InputFormat.CSV));
    }

    @Test
    public void convertCopyFrom_givenFormatIsNotSetInStatement_thenInputFormatDefaultsToJson() {
        BoundedCopyFrom analysis = analyze(
            "COPY users FROM '/some/distant/file.ext'");
        assertThat(analysis.inputFormat(), is(FileUriCollectPhase.InputFormat.JSON));
    }

    @Test
    public void testCopyToFile() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Using COPY TO without specifying a DIRECTORY is not supported");
        analyze("copy users to '/blah.txt'");
    }

    @Test
    public void testCopyToDirectory() throws Exception {
        CopyToAnalyzedStatement analysis = analyze("copy users to directory '/foo'");
        TableInfo tableInfo = analysis.relation().subRelation().tableInfo();
        assertThat(tableInfo.ident(), is(USER_TABLE_IDENT));
        assertThat(analysis.uri(), isLiteral("/foo"));
    }

    @Test
    public void testCopySysTableTo() throws Exception {
        expectedException.expect(OperationOnInaccessibleRelationException.class);
        expectedException.expectMessage("The relation \"sys.nodes\" doesn't support or allow COPY TO " +
                                        "operations, as it is read-only.");
        analyze("copy sys.nodes to directory '/foo'");
    }

    @Test
    public void testCopyToWithColumnList() throws Exception {
        CopyToAnalyzedStatement analysis = analyze("copy users (id, name) to DIRECTORY '/tmp'");
        List<Symbol> outputs = analysis.relation().outputs();
        assertThat(outputs.size(), is(2));
        assertThat(outputs.get(0), isReference("_doc['id']"));
        assertThat(outputs.get(1), isReference("_doc['name']"));
    }

    @Test
    public void testCopyToFileWithCompressionParams() throws Exception {
        CopyToAnalyzedStatement analysis = analyze("copy users to directory '/blah' with (compression='gzip')");
        TableInfo tableInfo = analysis.relation().subRelation().tableInfo();
        assertThat(tableInfo.ident(), is(USER_TABLE_IDENT));

        assertThat(analysis.uri(), isLiteral("/blah"));
        assertThat(analysis.compressionType(), is(WriterProjection.CompressionType.GZIP));
    }

    @Test
    public void testCopyToFileWithUnknownParams() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("setting 'foo' not supported");
        analyze("copy users to directory '/blah' with (foo='gzip')");
    }

    @Test
    public void testCopyToFileWithPartitionedTable() throws Exception {
        CopyToAnalyzedStatement analysis = analyze("copy parted to directory '/blah'");
        TableInfo tableInfo = analysis.relation().subRelation().tableInfo();
        assertThat(tableInfo.ident(), is(TEST_PARTITIONED_TABLE_IDENT));
        assertThat(analysis.overwrites().size(), is(1));
    }

    @Test
    public void testCopyToFileWithPartitionClause() throws Exception {
        CopyToAnalyzedStatement analysis = analyze("copy parted partition (date=1395874800000) to directory '/blah'");
        String parted = new PartitionName(
            new RelationName("doc", "parted"), Collections.singletonList("1395874800000")).asIndexName();
        assertThat(analysis.relation().where().partitions(), contains(parted));
    }

    @Test
    public void testCopyToDirectoryWithPartitionClause() throws Exception {
        CopyToAnalyzedStatement analysis = analyze("copy parted partition (date=1395874800000) to directory '/tmp'");
        String parted = new PartitionName(
            new RelationName("doc", "parted"), Collections.singletonList("1395874800000")).asIndexName();
        assertThat(analysis.relation().where().partitions(), contains(parted));
        assertThat(analysis.overwrites().size(), is(0));
    }

    @Test
    public void testCopyToDirectoryWithNotExistingPartitionClause() throws Exception {
        expectedException.expect(PartitionUnknownException.class);
        expectedException.expectMessage("No partition for table 'doc.parted' with ident '04130' exists");
        analyze("copy parted partition (date=0) to directory '/tmp/'");
    }

    @Test
    public void testCopyToWithWhereClause() throws Exception {
        CopyToAnalyzedStatement analysis = analyze("copy parted where id = 1 to directory '/tmp/foo'");
        assertThat(analysis.relation().where().query(), isFunction("op_="));
    }

    @Test
    public void testCopyToWithPartitionIdentAndPartitionInWhereClause() throws Exception {
        CopyToAnalyzedStatement analysis = analyze(
            "copy parted partition (date=1395874800000) where date = 1395874800000 to directory '/tmp/foo'");
        String parted = new PartitionName(
            new RelationName("doc", "parted"), Collections.singletonList("1395874800000")).asIndexName();
        assertThat(analysis.relation().where().partitions(), contains(parted));
    }


    @Test
    public void testCopyToWithPartitionIdentAndWhereClause() throws Exception {
        CopyToAnalyzedStatement analysis = analyze(
            "copy parted partition (date=1395874800000) where id = 1 to directory '/tmp/foo'");
        String parted = new PartitionName(
            new RelationName("doc", "parted"), Collections.singletonList("1395874800000")).asIndexName();
        WhereClause where = analysis.relation().where();
        assertThat(where.partitions(), contains(parted));
        assertThat(where.query(), isFunction("op_="));
    }

    @Test
    public void testCopyToWithNotExistingPartitionClause() throws Exception {
        expectedException.expect(PartitionUnknownException.class);
        expectedException.expectMessage("No partition for table 'doc.parted' with ident '04130' exists");
        analyze("copy parted partition (date=0) to directory '/tmp/blah'");
    }

    @Test
    public void testCopyToFileWithSelectedColumnsAndOutputFormatParam() throws Exception {
        CopyToAnalyzedStatement analysis = analyze("copy users (id, name) to directory '/blah' with (format='json_object')");
        TableInfo tableInfo = analysis.relation().subRelation().tableInfo();
        assertThat(tableInfo.ident(), is(USER_TABLE_IDENT));

        assertThat(analysis.uri(), isLiteral("/blah"));
        assertThat(analysis.outputFormat(), is(WriterProjection.OutputFormat.JSON_OBJECT));
        assertThat(analysis.outputNames(), contains("id", "name"));
    }

    @Test
    public void testCopyToFileWithUnsupportedOutputFormatParam() throws Exception {
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage("Output format not supported without specifying columns.");
        analyze("copy users to directory '/blah' with (format='json_array')");
    }

    @Test
    public void testCopyFromWithReferenceAssignedToProperty() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage(
            "Columns cannot be used in this context." +
            " Maybe you wanted to use a string literal which requires single quotes: 'gzip'");
        analyze("COPY users FROM '/blah.txt' with (compression = gzip)");
    }

    @Test
    public void testCopyFromFileUriArray() throws Exception {
        List<String> files = Arrays.asList("/f1.json", "/f2.json");
        BoundedCopyFrom copyFrom = analyze(
            "COPY users FROM ?", new Object[]{files});
        assertThat(copyFrom.uri(), isLiteral(List.of("/f1.json", "/f2.json")));
    }

    @Test
    public void testCopyFromInvalidTypedExpression() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("fileUri must be of type STRING or STRING ARRAY. Got integer_array");
        Object[] files = $(1, 2, 3);
        analyze("COPY users FROM ?", new Object[]{files});
    }

    @Test
    public void testStringAsNodeFiltersArgument() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid parameter passed to node_filters. " +
                                        "Expected an object with name or id keys and string values. Got 'invalid'");
        analyze("COPY users FROM '/' WITH (node_filters='invalid')");
    }

    @Test
    public void testObjectWithWrongKeyAsNodeFiltersArgument() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid node_filters arguments: [dummy]");
        analyze("COPY users FROM '/' WITH (node_filters={dummy='invalid'})");
    }

    @Test
    public void testObjectWithInvalidValueTypeAsNodeFiltersArgument() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("node_filters argument 'name' must be a String, not 20 (Long)");
        analyze("COPY users FROM '/' WITH (node_filters={name=20})");
    }
}
