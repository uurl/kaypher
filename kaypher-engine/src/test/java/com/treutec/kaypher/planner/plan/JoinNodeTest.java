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

import static com.treutec.kaypher.metastore.model.DataSource.DataSourceType;
import static com.treutec.kaypher.planner.plan.PlanTestUtil.SOURCE_NODE;
import static com.treutec.kaypher.planner.plan.PlanTestUtil.TRANSFORM_NODE;
import static com.treutec.kaypher.planner.plan.PlanTestUtil.getNodeByName;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.treutec.kaypher.execution.builder.KaypherQueryBuilder;
import com.treutec.kaypher.execution.context.QueryContext;
import com.treutec.kaypher.execution.ddl.commands.KaypherTopic;
import com.treutec.kaypher.execution.streams.KSPlanBuilder;
import com.treutec.kaypher.function.FunctionRegistry;
import com.treutec.kaypher.function.InternalFunctionRegistry;
import com.treutec.kaypher.metastore.MetaStore;
import com.treutec.kaypher.metastore.model.DataSource;
import com.treutec.kaypher.metastore.model.KeyField;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.parser.tree.WithinExpression;
import com.treutec.kaypher.planner.plan.JoinNode.JoinType;
import com.treutec.kaypher.query.QueryId;
import com.treutec.kaypher.schema.kaypher.Column;
import com.treutec.kaypher.schema.kaypher.ColumnRef;
import com.treutec.kaypher.schema.kaypher.FormatOptions;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.schema.kaypher.types.SqlTypes;
import com.treutec.kaypher.serde.Format;
import com.treutec.kaypher.serde.FormatInfo;
import com.treutec.kaypher.serde.ValueFormat;
import com.treutec.kaypher.services.KafkaTopicClient;
import com.treutec.kaypher.services.ServiceContext;
import com.treutec.kaypher.structured.SchemaKStream;
import com.treutec.kaypher.structured.SchemaKTable;
import com.treutec.kaypher.testutils.AnalysisTestUtil;
import com.treutec.kaypher.util.KaypherConfig;
import com.treutec.kaypher.util.KaypherException;
import com.treutec.kaypher.util.MetaStoreFixture;
import com.treutec.kaypher.util.SchemaUtil;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;


@SuppressWarnings({"SameParameterValue", "OptionalGetWithoutIsPresent"})
@RunWith(MockitoJUnitRunner.class)
public class JoinNodeTest {

  private static final LogicalSchema LEFT_SOURCE_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("C0"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("L1"), SqlTypes.STRING)
      .build();

  private static final LogicalSchema RIGHT_SOURCE_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("C0"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("R1"), SqlTypes.STRING)
      .build();

  private static final SourceName LEFT_ALIAS = SourceName.of("left");
  private static final SourceName RIGHT_ALIAS = SourceName.of("right");

  private static final LogicalSchema LEFT_NODE_SCHEMA = LEFT_SOURCE_SCHEMA
      .withMetaAndKeyColsInValue()
      .withAlias(LEFT_ALIAS);

  private static final LogicalSchema RIGHT_NODE_SCHEMA = RIGHT_SOURCE_SCHEMA
      .withMetaAndKeyColsInValue()
      .withAlias(RIGHT_ALIAS);

  private static final LogicalSchema JOIN_SCHEMA = joinSchema();

  private static final Optional<ColumnRef> NO_KEY_FIELD = Optional.empty();
  private static final ValueFormat VALUE_FORMAT = ValueFormat.of(FormatInfo.of(Format.JSON));
  private static final ValueFormat OTHER_FORMAT = ValueFormat.of(FormatInfo.of(Format.DELIMITED));
  private final KaypherConfig kaypherConfig = new KaypherConfig(new HashMap<>());
  private StreamsBuilder builder;
  private JoinNode joinNode;

  private static final ColumnRef LEFT_JOIN_FIELD_REF = ColumnRef.of(LEFT_ALIAS, ColumnName.of("C0"));
  private static final ColumnRef RIGHT_JOIN_FIELD_REF = ColumnRef.of(RIGHT_ALIAS, ColumnName.of("R1"));

  private static final KeyField leftJoinField = KeyField.of(LEFT_JOIN_FIELD_REF);

  private static final KeyField rightJoinField = KeyField.of(RIGHT_JOIN_FIELD_REF);

  private static final Optional<WithinExpression> WITHIN_EXPRESSION =
      Optional.of(new WithinExpression(10, TimeUnit.SECONDS));

  private static final PlanNodeId nodeId = new PlanNodeId("join");
  private static final QueryId queryId = new QueryId("join-query");
  private static final QueryContext.Stacker CONTEXT_STACKER =
      new QueryContext.Stacker().push(nodeId.toString());

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private DataSource<?> leftSource;
  @Mock
  private DataSource<?> rightSource;
  @Mock
  private DataSourceNode left;
  @Mock
  private DataSourceNode right;
  @Mock
  private SchemaKStream<String> leftSchemaKStream;
  @Mock
  private SchemaKStream<String> rightSchemaKStream;
  @Mock
  private SchemaKTable<String> leftSchemaKTable;
  @Mock
  private SchemaKTable<String> rightSchemaKTable;
  @Mock
  private KaypherQueryBuilder kaypherStreamBuilder;
  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private KafkaTopicClient mockKafkaTopicClient;

  @Before
  public void setUp() {
    builder = new StreamsBuilder();

    final ServiceContext serviceContext = mock(ServiceContext.class);
    when(serviceContext.getTopicClient())
        .thenReturn(mockKafkaTopicClient);

    when(kaypherStreamBuilder.getKaypherConfig()).thenReturn(kaypherConfig);
    when(kaypherStreamBuilder.getStreamsBuilder()).thenReturn(builder);
    when(kaypherStreamBuilder.getServiceContext()).thenReturn(serviceContext);
    when(kaypherStreamBuilder.withKaypherConfig(any())).thenReturn(kaypherStreamBuilder);
    when(kaypherStreamBuilder.getFunctionRegistry()).thenReturn(functionRegistry);
    when(kaypherStreamBuilder.buildNodeContext(any())).thenAnswer(inv ->
        new QueryContext.Stacker()
            .push(inv.getArgument(0).toString()));

    when(left.getAlias()).thenReturn(LEFT_ALIAS);
    when(right.getAlias()).thenReturn(RIGHT_ALIAS);

    when(left.getDataSourceType()).thenReturn(DataSourceType.KSTREAM);
    when(right.getDataSourceType()).thenReturn(DataSourceType.KTABLE);

    when(left.getSchema()).thenReturn(LEFT_NODE_SCHEMA);
    when(right.getSchema()).thenReturn(RIGHT_NODE_SCHEMA);

    when(left.getPartitions(mockKafkaTopicClient)).thenReturn(2);
    when(right.getPartitions(mockKafkaTopicClient)).thenReturn(2);

    setUpSource(left, VALUE_FORMAT, leftSource, "Foobar1");
    setUpSource(right, OTHER_FORMAT, rightSource, "Foobar2");

    when(leftSchemaKStream.getKeyField()).thenReturn(leftJoinField);
    when(leftSchemaKTable.getKeyField()).thenReturn(leftJoinField);
    when(rightSchemaKTable.getKeyField()).thenReturn(rightJoinField);
  }

  @Test
  public void shouldThrowIfLeftKeyFieldNotInLeftSchema() {
    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Invalid join field");

    // When:
    new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.LEFT,
        left,
        right,
        ColumnRef.withoutSource(ColumnName.of("won't find me")),
        RIGHT_JOIN_FIELD_REF,
        Optional.empty()
    );
  }

  @Test
  public void shouldThrowIfRightKeyFieldNotInRightSchema() {
    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Invalid join field");

    // When:
    new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.LEFT,
        left,
        right,
        LEFT_JOIN_FIELD_REF,
        ColumnRef.withoutSource(ColumnName.of("won't find me")),
        Optional.empty()
    );
  }

  @Test
  public void shouldReturnLeftJoinKeyAsKeyField() {
    // When:
    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinType.LEFT,
        left,
        right,
        LEFT_JOIN_FIELD_REF,
        RIGHT_JOIN_FIELD_REF,
        Optional.empty()
    );

    // Then:
    assertThat(joinNode.getKeyField().ref(), is(Optional.of(LEFT_JOIN_FIELD_REF)));
  }

  @Test
  public void shouldBuildSourceNode() {
    setupTopicClientExpectations(1, 1);
    buildJoin();
    final TopologyDescription.Source node = (TopologyDescription.Source) getNodeByName(
        builder.build(), SOURCE_NODE);
    final List<String> successors = node.successors().stream().map(TopologyDescription.Node::name)
        .collect(Collectors.toList());
    assertThat(node.predecessors(), equalTo(Collections.emptySet()));
    assertThat(successors, equalTo(Collections.singletonList(TRANSFORM_NODE)));
    assertThat(node.topicSet(), equalTo(ImmutableSet.of("test2")));
  }

  @Test
  public void shouldHaveLeftJoin() {
    setupTopicClientExpectations(1, 1);
    buildJoin();
    final Topology topology = builder.build();
    final TopologyDescription.Processor leftJoin
        = (TopologyDescription.Processor) getNodeByName(topology, "Join");
    final List<String> predecessors = leftJoin.predecessors().stream()
        .map(TopologyDescription.Node::name).collect(Collectors.toList());
    assertThat(leftJoin.stores(), equalTo(Utils.mkSet("KafkaTopic_Right-reduce")));
    assertThat(predecessors, equalTo(Collections.singletonList("Join-repartition-source")));
  }

  @Test
  public void shouldThrowOnPartitionMismatch() {
    // Given:
    setupTopicClientExpectations(1, 2);

    // Then:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage(
        "Can't join TEST1 with TEST2 since the number of partitions don't match. TEST1 "
            + "partitions = 1; TEST2 partitions = 2. Please repartition either one so that the "
            + "number of partitions match."
    );

    // When:
    buildJoin(
          "SELECT t1.col0, t2.col0, t2.col1 "
              + "FROM test1 t1 LEFT JOIN test2 t2 ON t1.col0 = t2.col0 EMIT CHANGES;"
    );
  }

  @Test
  public void shouldPerformStreamToStreamLeftJoin() {
    // Given:
    setupStream(left, leftSchemaKStream);
    setupStream(right, rightSchemaKStream);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.LEFT,
        left,
        right,
        LEFT_JOIN_FIELD_REF,
        RIGHT_JOIN_FIELD_REF,
        WITHIN_EXPRESSION
    );

    // When:
    joinNode.buildStream(kaypherStreamBuilder);

    // Then:
    verify(leftSchemaKStream).leftJoin(
        eq(rightSchemaKStream),
        eq(JOIN_SCHEMA),
        eq(leftJoinField),
        eq(WITHIN_EXPRESSION.get().joinWindow()),
        eq(VALUE_FORMAT),
        eq(OTHER_FORMAT),
        eq(CONTEXT_STACKER)
    );
  }

  @Test
  public void shouldPerformStreamToStreamInnerJoin() {
    // Given:
    setupStream(left, leftSchemaKStream);
    setupStream(right, rightSchemaKStream);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.INNER,
        left,
        right,
        LEFT_JOIN_FIELD_REF,
        RIGHT_JOIN_FIELD_REF,
        WITHIN_EXPRESSION
    );

    // When:
    joinNode.buildStream(kaypherStreamBuilder);

    // Then:
    verify(leftSchemaKStream).join(
        eq(rightSchemaKStream),
        eq(JOIN_SCHEMA),
        eq(leftJoinField),
        eq(WITHIN_EXPRESSION.get().joinWindow()),
        eq(VALUE_FORMAT),
        eq(OTHER_FORMAT),
        eq(CONTEXT_STACKER)
    );
  }

  @Test
  public void shouldPerformStreamToStreamOuterJoin() {
    // Given:
    setupStream(left, leftSchemaKStream);
    setupStream(right, rightSchemaKStream);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.OUTER,
        left,
        right,
        LEFT_JOIN_FIELD_REF,
        RIGHT_JOIN_FIELD_REF,
        WITHIN_EXPRESSION
    );

    // When:
    joinNode.buildStream(kaypherStreamBuilder);

    // Then:
    verify(leftSchemaKStream).outerJoin(
        eq(rightSchemaKStream),
        eq(JOIN_SCHEMA),
        eq(KeyField.none()),
        eq(WITHIN_EXPRESSION.get().joinWindow()),
        eq(VALUE_FORMAT),
        eq(OTHER_FORMAT),
        eq(CONTEXT_STACKER)
    );
  }

  @Test
  public void shouldNotPerformStreamStreamJoinWithoutJoinWindow() {
    // Given:
    when(left.getDataSourceType()).thenReturn(DataSourceType.KSTREAM);
    when(right.getDataSourceType()).thenReturn(DataSourceType.KSTREAM);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.INNER,
        left,
        right,
        LEFT_JOIN_FIELD_REF,
        RIGHT_JOIN_FIELD_REF,
        Optional.empty()
    );

    // Then:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage(
        "Stream-Stream joins must have a WITHIN clause specified. None was provided."
    );

    // When:
    joinNode.buildStream(kaypherStreamBuilder);
  }

  @Test
  public void shouldNotPerformJoinIfInputPartitionsMisMatch() {
    // Given:
    when(left.getPartitions(mockKafkaTopicClient)).thenReturn(3);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.OUTER,
        left,
        right,
        LEFT_JOIN_FIELD_REF,
        RIGHT_JOIN_FIELD_REF,
        WITHIN_EXPRESSION
    );

    // Then:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage(
        "Can't join Foobar1 with Foobar2 since the number of partitions don't match."
    );

    // When:
    joinNode.buildStream(kaypherStreamBuilder);
  }

  @Test
  public void shouldFailJoinIfTableCriteriaColumnIsNotKey() {
    // Given:
    setupStream(left, leftSchemaKStream);
    setupTable(right, rightSchemaKTable);

    final ColumnRef rightCriteriaColumn =
        getNonKeyColumn(RIGHT_SOURCE_SCHEMA, RIGHT_ALIAS, RIGHT_JOIN_FIELD_REF);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.LEFT,
        left,
        right,
        LEFT_JOIN_FIELD_REF,
        rightCriteriaColumn,
        Optional.empty()
    );

    // Then:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage(String.format(
        "Source table (%s) key column (%s) is not the column used in the join criteria (%s). "
            + "Only the table's key column or 'ROWKEY' is supported in the join criteria.",
        RIGHT_ALIAS.toString(FormatOptions.noEscape()),
        RIGHT_JOIN_FIELD_REF.toString(FormatOptions.noEscape()),
        rightCriteriaColumn.toString(FormatOptions.noEscape())
    ));

    // When:
    joinNode.buildStream(kaypherStreamBuilder);
  }

  @Test
  public void shouldFailJoinIfTableHasNoKeyAndJoinFieldIsNotRowKey() {
    // Given:
    setupStream(left, leftSchemaKStream);
    setupTable(right, rightSchemaKTable, NO_KEY_FIELD);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.LEFT,
        left,
        right,
        LEFT_JOIN_FIELD_REF,
        RIGHT_JOIN_FIELD_REF,
        Optional.empty()
    );

    // Then:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage(
        "Source table (" + RIGHT_ALIAS.name() + ") has no key column defined. "
            + "Only 'ROWKEY' is supported in the join criteria."
    );

    // When:
    joinNode.buildStream(kaypherStreamBuilder);
  }

  @Test
  public void shouldHandleJoinIfTableHasNoKeyAndJoinFieldIsRowKey() {
    // Given:
    setupStream(left, leftSchemaKStream);
    setupTable(right, rightSchemaKTable, NO_KEY_FIELD);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.LEFT,
        left,
        right,
        LEFT_JOIN_FIELD_REF,
        ColumnRef.of(SourceName.of("right"), ColumnName.of("ROWKEY")),
        Optional.empty()
    );

    // When:
    joinNode.buildStream(kaypherStreamBuilder);

    // Then:
    verify(leftSchemaKStream).leftJoin(
        eq(rightSchemaKTable),
        eq(JOIN_SCHEMA),
        eq(leftJoinField),
        eq(VALUE_FORMAT),
        eq(CONTEXT_STACKER)
    );
  }

  @Test
  public void shouldPerformStreamToTableLeftJoin() {
    // Given:
    setupStream(left, leftSchemaKStream);
    setupTable(right, rightSchemaKTable);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.LEFT,
        left,
        right,
        LEFT_JOIN_FIELD_REF,
        RIGHT_JOIN_FIELD_REF,
        Optional.empty()
    );

    // When:
    joinNode.buildStream(kaypherStreamBuilder);

    // Then:
    verify(leftSchemaKStream).leftJoin(
        eq(rightSchemaKTable),
        eq(JOIN_SCHEMA),
        eq(leftJoinField),
        eq(VALUE_FORMAT),
        eq(CONTEXT_STACKER)
    );
  }

  @Test
  public void shouldPerformStreamToTableInnerJoin() {
    // Given:
    setupStream(left, leftSchemaKStream);
    setupTable(right, rightSchemaKTable);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.INNER,
        left,
        right,
        LEFT_JOIN_FIELD_REF,
        RIGHT_JOIN_FIELD_REF,
        Optional.empty()
    );

    // When:
    joinNode.buildStream(kaypherStreamBuilder);

    // Then:
    verify(leftSchemaKStream).join(
        eq(rightSchemaKTable),
        eq(JOIN_SCHEMA),
        eq(leftJoinField),
        eq(VALUE_FORMAT),
        eq(CONTEXT_STACKER)
    );
  }

  @Test
  public void shouldNotAllowStreamToTableOuterJoin() {
    // Given:
    setupStream(left, leftSchemaKStream);
    setupTable(right, rightSchemaKTable);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.OUTER,
        left,
        right,
        LEFT_JOIN_FIELD_REF,
        RIGHT_JOIN_FIELD_REF,
        Optional.empty()
    );

    // Then:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage(
        "Full outer joins between streams and tables are not supported."
    );

    // When:
    joinNode.buildStream(kaypherStreamBuilder);
  }

  @Test
  public void shouldNotPerformStreamToTableJoinIfJoinWindowIsSpecified() {
    // Given:
    when(left.getDataSourceType()).thenReturn(DataSourceType.KSTREAM);
    when(right.getDataSourceType()).thenReturn(DataSourceType.KTABLE);

    final WithinExpression withinExpression = new WithinExpression(10, TimeUnit.SECONDS);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.OUTER,
        left,
        right,
        LEFT_JOIN_FIELD_REF,
        RIGHT_JOIN_FIELD_REF,
        Optional.of(withinExpression)
    );

    // Then:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage(
        "A window definition was provided for a Stream-Table join."
    );

    // When:
    joinNode.buildStream(kaypherStreamBuilder);
  }

  @Test
  public void shouldFailTableTableJoinIfLeftCriteriaColumnIsNotKey() {
    // Given:
    setupTable(left, leftSchemaKTable);
    setupTable(right, rightSchemaKTable);

    final ColumnRef leftCriteriaColumn = getNonKeyColumn(LEFT_SOURCE_SCHEMA, LEFT_ALIAS,
        LEFT_JOIN_FIELD_REF);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.LEFT,
        left,
        right,
        leftCriteriaColumn,
        RIGHT_JOIN_FIELD_REF,
        Optional.empty()
    );

    // Then:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage(String.format(
        "Source table (%s) key column (%s) is not the column used in the join criteria (%s). "
            + "Only the table's key column or 'ROWKEY' is supported in the join criteria.",
        LEFT_ALIAS.toString(FormatOptions.noEscape()),
        LEFT_JOIN_FIELD_REF.toString(FormatOptions.noEscape()),
        leftCriteriaColumn.toString(FormatOptions.noEscape())
    ));

    // When:
    joinNode.buildStream(kaypherStreamBuilder);
  }

  @Test
  public void shouldFailTableTableJoinIfRightCriteriaColumnIsNotKey() {
    // Given:
    setupTable(left, leftSchemaKTable);
    setupTable(right, rightSchemaKTable);

    final ColumnRef rightCriteriaColumn =
        getNonKeyColumn(RIGHT_SOURCE_SCHEMA, RIGHT_ALIAS, RIGHT_JOIN_FIELD_REF);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.LEFT,
        left,
        right,
        LEFT_JOIN_FIELD_REF,
        rightCriteriaColumn,
        Optional.empty()
    );

    // Then:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage(String.format(
        "Source table (%s) key column (%s) is not the column used in the join criteria (%s). "
            + "Only the table's key column or 'ROWKEY' is supported in the join criteria.",
        RIGHT_ALIAS.toString(FormatOptions.noEscape()),
        RIGHT_JOIN_FIELD_REF.toString(FormatOptions.noEscape()),
        rightCriteriaColumn.toString(FormatOptions.noEscape())
    ));

    // When:
    joinNode.buildStream(kaypherStreamBuilder);
  }

  @Test
  public void shouldPerformTableToTableInnerJoin() {
    // Given:
    setupTable(left, leftSchemaKTable);
    setupTable(right, rightSchemaKTable);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.INNER,
        left,
        right,
        LEFT_JOIN_FIELD_REF,
        RIGHT_JOIN_FIELD_REF,
        Optional.empty()
    );

    // When:
    joinNode.buildStream(kaypherStreamBuilder);

    // Then:
    verify(leftSchemaKTable).join(
        eq(rightSchemaKTable),
        eq(JOIN_SCHEMA),
        eq(leftJoinField),
        eq(CONTEXT_STACKER));
  }

  @Test
  public void shouldPerformTableToTableLeftJoin() {
    // Given:
    setupTable(left, leftSchemaKTable);
    setupTable(right, rightSchemaKTable);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.LEFT,
        left,
        right,
        LEFT_JOIN_FIELD_REF,
        RIGHT_JOIN_FIELD_REF,
        Optional.empty()
    );

    // When:
    joinNode.buildStream(kaypherStreamBuilder);

    // Then:
    verify(leftSchemaKTable).leftJoin(
        eq(rightSchemaKTable),
        eq(JOIN_SCHEMA),
        eq(leftJoinField),
        eq(CONTEXT_STACKER));
  }

  @Test
  public void shouldPerformTableToTableOuterJoin() {
    // Given:
    setupTable(left, leftSchemaKTable);
    setupTable(right, rightSchemaKTable);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.OUTER,
        left,
        right,
        LEFT_JOIN_FIELD_REF,
        RIGHT_JOIN_FIELD_REF,
        Optional.empty()
    );

    // When:
    joinNode.buildStream(kaypherStreamBuilder);

    // Then:
    verify(leftSchemaKTable).outerJoin(
        eq(rightSchemaKTable),
        eq(JOIN_SCHEMA),
        eq(KeyField.none()),
        eq(CONTEXT_STACKER));
  }

  @Test
  public void shouldNotPerformTableToTableJoinIfJoinWindowIsSpecified() {
    // Given:
    when(left.getDataSourceType()).thenReturn(DataSourceType.KTABLE);
    when(right.getDataSourceType()).thenReturn(DataSourceType.KTABLE);

    final WithinExpression withinExpression = new WithinExpression(10, TimeUnit.SECONDS);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.OUTER,
        left,
        right,
        LEFT_JOIN_FIELD_REF,
        RIGHT_JOIN_FIELD_REF,
        Optional.of(withinExpression)
    );

    // Then:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage(
        "A window definition was provided for a Table-Table join."
    );

    // When:
    joinNode.buildStream(kaypherStreamBuilder);
  }

  @Test
  public void shouldHaveFullyQualifiedJoinSchema() {
    // When:
    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.OUTER,
        left,
        right,
        LEFT_JOIN_FIELD_REF,
        RIGHT_JOIN_FIELD_REF,
        Optional.empty()
    );

    // When:
    assertThat(joinNode.getSchema(), is(LogicalSchema.builder()
        .valueColumn(LEFT_ALIAS, ColumnName.of("ROWTIME"), SqlTypes.BIGINT)
        .valueColumn(LEFT_ALIAS, ColumnName.of("ROWKEY"), SqlTypes.STRING)
        .valueColumn(LEFT_ALIAS, ColumnName.of("C0"), SqlTypes.BIGINT)
        .valueColumn(LEFT_ALIAS, ColumnName.of("L1"), SqlTypes.STRING)
        .valueColumn(RIGHT_ALIAS, ColumnName.of("ROWTIME"), SqlTypes.BIGINT)
        .valueColumn(RIGHT_ALIAS, ColumnName.of("ROWKEY"), SqlTypes.STRING)
        .valueColumn(RIGHT_ALIAS, ColumnName.of("C0"), SqlTypes.BIGINT)
        .valueColumn(RIGHT_ALIAS, ColumnName.of("R1"), SqlTypes.STRING)
        .build()
    ));
  }

  @Test
  public void shouldSelectLeftKeyField() {
    // Given:
    setupStream(left, leftSchemaKStream);
    setupStream(right, rightSchemaKStream);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.OUTER,
        left,
        right,
        LEFT_JOIN_FIELD_REF,
        RIGHT_JOIN_FIELD_REF,
        WITHIN_EXPRESSION
    );

    // When:
    joinNode.buildStream(kaypherStreamBuilder);

    // Then:
    verify(leftSchemaKStream).selectKey(
        eq(LEFT_JOIN_FIELD_REF),
        anyBoolean(),
        any()
    );
  }

  @Test
  public void shouldNotUseSourceSerdeOptionsForInternalTopics() {
    // Given:
    setupStream(left, leftSchemaKStream);
    setupStream(right, rightSchemaKStream);

    final JoinNode joinNode = new JoinNode(
        nodeId,
        Collections.emptyList(),
        JoinNode.JoinType.LEFT,
        left,
        right,
        LEFT_JOIN_FIELD_REF,
        RIGHT_JOIN_FIELD_REF,
        WITHIN_EXPRESSION
    );

    // When:
    joinNode.buildStream(kaypherStreamBuilder);

    // Then:
    verify(leftSource, never()).getSerdeOptions();
    verify(rightSource, never()).getSerdeOptions();
  }

  @SuppressWarnings("unchecked")
  private void setupTable(
      final DataSourceNode node,
      final SchemaKTable table
  ) {
    when(node.buildStream(kaypherStreamBuilder)).thenReturn(table);
    final LogicalSchema schema = node.getSchema();
    when(table.getSchema()).thenReturn(schema);
    when(node.getDataSourceType()).thenReturn(DataSourceType.KTABLE);
  }

  private void setupTable(
      final DataSourceNode node,
      final SchemaKTable table,
      final Optional<ColumnRef> keyFieldName
  ) {
    setupTable(node, table);

    when(table.getKeyField()).thenReturn(KeyField.of(keyFieldName));
  }

  @SuppressWarnings("unchecked")
  private void setupStream(
      final DataSourceNode node,
      final SchemaKStream stream
  ) {
    when(node.buildStream(kaypherStreamBuilder)).thenReturn(stream);
    final LogicalSchema schema = node.getSchema();
    when(stream.getSchema()).thenReturn(schema);
    when(stream.selectKey(any(), eq(true), any())).thenReturn(stream);
    when(node.getDataSourceType()).thenReturn(DataSourceType.KSTREAM);
  }

  @SuppressWarnings("Duplicates")
  private static LogicalSchema joinSchema() {
    return LogicalSchema.builder()
        .keyColumn(SchemaUtil.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumns(LEFT_NODE_SCHEMA.value())
        .valueColumns(RIGHT_NODE_SCHEMA.value())
        .build();
  }

  private void buildJoin() {
    buildJoin(
        "SELECT t1.col1, t2.col1, t2.col4, col5, t2.col2 "
            + "FROM test1 t1 LEFT JOIN test2 t2 "
            + "ON t1.col1 = t2.col0 EMIT CHANGES;"
    );
  }

  private void buildJoin(final String queryString) {
    buildJoinNode(queryString);
    final SchemaKStream stream = joinNode.buildStream(kaypherStreamBuilder);
    if (stream instanceof SchemaKTable) {
      final SchemaKTable table = (SchemaKTable) stream;
      table.getSourceTableStep().build(new KSPlanBuilder(kaypherStreamBuilder));
    } else {
      stream.getSourceStep().build(new KSPlanBuilder(kaypherStreamBuilder));
    }
  }

  private void buildJoinNode(final String queryString) {
    final MetaStore metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());

    final KaypherBareOutputNode planNode =
        (KaypherBareOutputNode) AnalysisTestUtil.buildLogicalPlan(kaypherConfig, queryString, metaStore);

    joinNode = (JoinNode) ((ProjectNode) planNode.getSource()).getSource();
  }

  private void setupTopicClientExpectations(final int streamPartitions, final int tablePartitions) {
    final Node node = new Node(0, "localhost", 9091);

    final List<TopicPartitionInfo> streamPartitionInfoList =
        IntStream.range(0, streamPartitions)
            .mapToObj(
                p -> new TopicPartitionInfo(p, node, Collections.emptyList(),
                    Collections.emptyList()))
            .collect(Collectors.toList());

    when(mockKafkaTopicClient.describeTopic("test1"))
        .thenReturn(new TopicDescription("test1", false, streamPartitionInfoList));

    final List<TopicPartitionInfo> tablePartitionInfoList =
        IntStream.range(0, tablePartitions)
            .mapToObj(
                p -> new TopicPartitionInfo(p, node, Collections.emptyList(),
                    Collections.emptyList()))
            .collect(Collectors.toList());

    when(mockKafkaTopicClient.describeTopic("test2"))
        .thenReturn(new TopicDescription("test2", false, tablePartitionInfoList));
  }

  private static Optional<Column> getColumn(final LogicalSchema schema, final Predicate<ColumnName> filter) {
    return schema.value().stream()
        .filter(col -> filter.test(col.name()))
        .findFirst();
  }

  private static ColumnRef getNonKeyColumn(
      final LogicalSchema schema,
      final SourceName alias,
      final ColumnRef keyName
  ) {
    final ImmutableList<ColumnName> blackList = ImmutableList.of(
        SchemaUtil.ROWKEY_NAME,
        SchemaUtil.ROWTIME_NAME,
        keyName.name()
    );

    final Column column =
        getColumn(schema, s -> !blackList.contains(s))
            .orElseThrow(AssertionError::new);

    final Column field = schema.findValueColumn(column.ref()).get();
    return ColumnRef.of(alias, field.name());
  }

  @SuppressWarnings("unchecked")
  private static void setUpSource(
      final DataSourceNode node,
      final ValueFormat valueFormat,
      final DataSource<?> dataSource,
      final String name
  ) {
    when(dataSource.getName()).thenReturn(SourceName.of(name));
    when(node.getDataSource()).thenReturn((DataSource)dataSource);

    final KaypherTopic kaypherTopic = mock(KaypherTopic.class);
    when(kaypherTopic.getValueFormat()).thenReturn(valueFormat);
    when(dataSource.getKaypherTopic()).thenReturn(kaypherTopic);
  }
}
