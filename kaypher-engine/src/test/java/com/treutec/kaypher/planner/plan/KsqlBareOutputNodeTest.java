/*
 * Copyright 2019 Treu Techologies
 *
 * See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.treutec.kaypher.planner.plan;

import static com.treutec.kaypher.planner.plan.PlanTestUtil.SOURCE_NODE;
import static com.treutec.kaypher.planner.plan.PlanTestUtil.TRANSFORM_NODE;
import static com.treutec.kaypher.planner.plan.PlanTestUtil.verifyProcessorNode;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.treutec.kaypher.execution.builder.KaypherQueryBuilder;
import com.treutec.kaypher.execution.context.QueryContext;
import com.treutec.kaypher.execution.streams.KSPlanBuilder;
import com.treutec.kaypher.function.FunctionRegistry;
import com.treutec.kaypher.function.InternalFunctionRegistry;
import com.treutec.kaypher.logging.processing.ProcessingLogContext;
import com.treutec.kaypher.metastore.MetaStore;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.query.QueryId;
import com.treutec.kaypher.query.id.QueryIdGenerator;
import com.treutec.kaypher.schema.kaypher.Column;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.schema.kaypher.types.SqlTypes;
import com.treutec.kaypher.serde.KeySerde;
import com.treutec.kaypher.structured.SchemaKStream;
import com.treutec.kaypher.testutils.AnalysisTestUtil;
import com.treutec.kaypher.util.KaypherConfig;
import com.treutec.kaypher.util.MetaStoreFixture;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyDescription;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KaypherBareOutputNodeTest {

  private static final String FILTER_NODE = "KSTREAM-FILTER-0000000002";
  private static final String FILTER_MAPVALUES_NODE = "KSTREAM-MAPVALUES-0000000003";
  private static final String SIMPLE_SELECT_WITH_FILTER = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100 EMIT CHANGES;";

  private SchemaKStream stream;
  private StreamsBuilder builder;
  private final MetaStore metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
  private final QueryId queryId = new QueryId("output-test");
  private final KaypherConfig kaypherConfig = new KaypherConfig(Collections.emptyMap());

  @Mock
  private KaypherQueryBuilder kaypherStreamBuilder;
  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private KeySerde<Struct> keySerde;

  @Before
  public void before() {
    builder = new StreamsBuilder();

    when(kaypherStreamBuilder.getQueryId()).thenReturn(queryId);
    when(kaypherStreamBuilder.getKaypherConfig()).thenReturn(new KaypherConfig(Collections.emptyMap()));
    when(kaypherStreamBuilder.getStreamsBuilder()).thenReturn(builder);
    when(kaypherStreamBuilder.getProcessingLogContext()).thenReturn(ProcessingLogContext.create());
    when(kaypherStreamBuilder.getFunctionRegistry()).thenReturn(functionRegistry);
    when(kaypherStreamBuilder.buildNodeContext(any())).thenAnswer(inv ->
        new QueryContext.Stacker()
            .push(inv.getArgument(0).toString()));
    when(kaypherStreamBuilder.buildKeySerde(any(), any(), any())).thenReturn(keySerde);

    final KaypherBareOutputNode planNode = (KaypherBareOutputNode) AnalysisTestUtil
        .buildLogicalPlan(kaypherConfig, SIMPLE_SELECT_WITH_FILTER, metaStore);

    stream = planNode.buildStream(kaypherStreamBuilder);
    stream.getSourceStep().build(new KSPlanBuilder(kaypherStreamBuilder));
  }

  @Test
  public void shouldBuildSourceNode() {
    final TopologyDescription.Source node = (TopologyDescription.Source) getNodeByName(SOURCE_NODE);
    final List<String> successors = node.successors().stream().map(TopologyDescription.Node::name).collect(Collectors.toList());
    assertThat(node.predecessors(), equalTo(Collections.emptySet()));
    assertThat(successors, equalTo(Collections.singletonList(TRANSFORM_NODE)));
    assertThat(node.topicSet(), equalTo(ImmutableSet.of("test1")));
  }

  @Test
  public void shouldBuildTransformNode() {
    final TopologyDescription.Processor node = (TopologyDescription.Processor) getNodeByName(TRANSFORM_NODE);
    verifyProcessorNode(node, Collections.singletonList(SOURCE_NODE), Collections.singletonList(FILTER_NODE));
  }

  @Test
  public void shouldBuildFilterNode() {
    final TopologyDescription.Processor node = (TopologyDescription.Processor) getNodeByName(FILTER_NODE);
    verifyProcessorNode(node, Collections.singletonList(TRANSFORM_NODE), Collections.singletonList(FILTER_MAPVALUES_NODE));
  }

  @Test
  public void shouldCreateCorrectSchema() {
    final LogicalSchema schema = stream.getSchema();
    assertThat(schema.value(), contains(
        Column.of(ColumnName.of("COL0"), SqlTypes.BIGINT),
        Column.of(ColumnName.of("COL2"), SqlTypes.STRING),
        Column.of(ColumnName.of("COL3"), SqlTypes.DOUBLE)));
  }

  @Test
  public void shouldComputeQueryIdCorrectly() {
    // Given:
    final KaypherBareOutputNode node
        = (KaypherBareOutputNode) AnalysisTestUtil
        .buildLogicalPlan(kaypherConfig, "select col0 from test1 EMIT CHANGES;", metaStore);
    final QueryIdGenerator queryIdGenerator = mock(QueryIdGenerator.class);

    // When:
    final Set<QueryId> ids = IntStream.range(0, 100)
        .mapToObj(i -> node.getQueryId(queryIdGenerator))
        .collect(Collectors.toSet());

    // Then:
    assertThat(ids.size(), equalTo(100));
    verifyNoMoreInteractions(queryIdGenerator);
  }

  private TopologyDescription.Node getNodeByName(final String nodeName) {
    return PlanTestUtil.getNodeByName(builder.build(), nodeName);
  }
}
