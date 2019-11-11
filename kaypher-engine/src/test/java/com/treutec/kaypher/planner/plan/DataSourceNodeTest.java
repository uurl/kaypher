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
import static com.treutec.kaypher.planner.plan.PlanTestUtil.getNodeByName;
import static com.treutec.kaypher.planner.plan.PlanTestUtil.verifyProcessorNode;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.treutec.kaypher.GenericRow;
import com.treutec.kaypher.execution.builder.KaypherQueryBuilder;
import com.treutec.kaypher.execution.context.QueryContext;
import com.treutec.kaypher.execution.ddl.commands.KaypherTopic;
import com.treutec.kaypher.execution.plan.StreamSource;
import com.treutec.kaypher.execution.streams.KSPlanBuilder;
import com.treutec.kaypher.function.FunctionRegistry;
import com.treutec.kaypher.metastore.model.DataSource;
import com.treutec.kaypher.metastore.model.KeyField;
import com.treutec.kaypher.metastore.model.KaypherStream;
import com.treutec.kaypher.metastore.model.KaypherTable;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.schema.kaypher.ColumnRef;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.schema.kaypher.types.SqlTypes;
import com.treutec.kaypher.serde.Format;
import com.treutec.kaypher.serde.FormatInfo;
import com.treutec.kaypher.serde.KeyFormat;
import com.treutec.kaypher.serde.KeySerde;
import com.treutec.kaypher.serde.SerdeOption;
import com.treutec.kaypher.serde.ValueFormat;
import com.treutec.kaypher.structured.SchemaKStream;
import com.treutec.kaypher.structured.SchemaKTable;
import com.treutec.kaypher.util.KaypherConfig;
import com.treutec.kaypher.util.SchemaUtil;
import com.treutec.kaypher.util.timestamp.LongColumnTimestampExtractionPolicy;
import com.treutec.kaypher.util.timestamp.TimestampExtractionPolicy;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.TopologyDescription;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DataSourceNodeTest {

  private static final ColumnRef TIMESTAMP_FIELD
      = ColumnRef.withoutSource(ColumnName.of("timestamp"));
  private static final PlanNodeId PLAN_NODE_ID = new PlanNodeId("0");

  private final KaypherConfig realConfig = new KaypherConfig(Collections.emptyMap());
  private SchemaKStream realStream;
  private StreamsBuilder realBuilder;

  private static final ColumnName FIELD1 = ColumnName.of("field1");
  private static final ColumnName FIELD2 = ColumnName.of("field2");
  private static final ColumnName FIELD3 = ColumnName.of("field3");

  private static final LogicalSchema REAL_SCHEMA = LogicalSchema.builder()
      .valueColumn(FIELD1, SqlTypes.STRING)
      .valueColumn(FIELD2, SqlTypes.STRING)
      .valueColumn(FIELD3, SqlTypes.STRING)
      .valueColumn(TIMESTAMP_FIELD.name(), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("key"), SqlTypes.STRING)
      .build();
  private static final KeyField KEY_FIELD
      = KeyField.of(ColumnRef.withoutSource(ColumnName.of("field1")));

  private static final Optional<AutoOffsetReset> OFFSET_RESET = Optional.of(AutoOffsetReset.LATEST);

  private final KaypherStream<String> SOME_SOURCE = new KaypherStream<>(
      "sqlExpression",
      SourceName.of("datasource"),
      REAL_SCHEMA,
      SerdeOption.none(),
      KeyField.of(ColumnRef.withoutSource(ColumnName.of("key"))),
      new LongColumnTimestampExtractionPolicy(ColumnRef.withoutSource(ColumnName.of("timestamp"))),
      new KaypherTopic(
          "topic",
          KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA)),
          ValueFormat.of(FormatInfo.of(Format.JSON)),
          false
      )
  );

  private final DataSourceNode node = new DataSourceNode(
      PLAN_NODE_ID,
      SOME_SOURCE,
      SOME_SOURCE.getName(),
      Collections.emptyList()
  );

  @Mock
  private DataSource<?> dataSource;
  @Mock
  private TimestampExtractionPolicy timestampExtractionPolicy;
  @Mock
  private KaypherTopic kaypherTopic;
  @Mock
  private Serde<GenericRow> rowSerde;
  @Mock
  private KeySerde<String> keySerde;
  @Mock
  private KaypherQueryBuilder kaypherStreamBuilder;
  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private DataSourceNode.SchemaKStreamFactory schemaKStreamFactory;
  @Captor
  private ArgumentCaptor<QueryContext.Stacker> stackerCaptor;
  @Mock
  private SchemaKStream stream;
  @Mock
  private SchemaKTable table;

  @Before
  @SuppressWarnings("unchecked")
  public void before() {
    realBuilder = new StreamsBuilder();

    when(kaypherStreamBuilder.getKaypherConfig()).thenReturn(realConfig);
    when(kaypherStreamBuilder.getStreamsBuilder()).thenReturn(realBuilder);
    when(kaypherStreamBuilder.buildNodeContext(any())).thenAnswer(inv ->
        new QueryContext.Stacker()
            .push(inv.getArgument(0).toString()));

    when(kaypherStreamBuilder.buildKeySerde(any(), any(), any()))
        .thenReturn((KeySerde)keySerde);
    when(kaypherStreamBuilder.buildValueSerde(any(), any(), any())).thenReturn(rowSerde);
    when(kaypherStreamBuilder.getFunctionRegistry()).thenReturn(functionRegistry);

    when(rowSerde.serializer()).thenReturn(mock(Serializer.class));
    when(rowSerde.deserializer()).thenReturn(mock(Deserializer.class));

    when(dataSource.getKaypherTopic()).thenReturn(kaypherTopic);
    when(dataSource.getDataSourceType()).thenReturn(DataSourceType.KTABLE);
    when(dataSource.getTimestampExtractionPolicy()).thenReturn(timestampExtractionPolicy);
    when(kaypherTopic.getKeyFormat()).thenReturn(KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA)));
    when(kaypherTopic.getValueFormat()).thenReturn(ValueFormat.of(FormatInfo.of(Format.JSON)));
    when(timestampExtractionPolicy.timestampField()).thenReturn(TIMESTAMP_FIELD);
    when(schemaKStreamFactory.create(any(), any(), any(), any(), anyInt(), any(), any()))
        .thenReturn(stream);
    when(stream.toTable(any(), any(), any())).thenReturn(table);
  }

  @Test
  public void shouldBuildSourceNode() {
    // When:
    realStream = buildStream(node);

    // Then:
    final TopologyDescription.Source node = (TopologyDescription.Source) getNodeByName(realBuilder.build(), PlanTestUtil.SOURCE_NODE);
    final List<String> successors = node.successors().stream().map(TopologyDescription.Node::name).collect(Collectors.toList());
    assertThat(node.predecessors(), equalTo(Collections.emptySet()));
    assertThat(successors, equalTo(Collections.singletonList(PlanTestUtil.TRANSFORM_NODE)));
    assertThat(node.topicSet(), equalTo(ImmutableSet.of("topic")));
  }

  @Test
  public void shouldBuildTransformNode() {
    // When:
    realStream = buildStream(node);

    // Then:
    final TopologyDescription.Processor node = (TopologyDescription.Processor) getNodeByName(
        realBuilder.build(), PlanTestUtil.TRANSFORM_NODE);
    verifyProcessorNode(node, Collections.singletonList(PlanTestUtil.SOURCE_NODE), Collections.emptyList());
  }

  @Test
  public void shouldBeOfTypeSchemaKStreamWhenDataSourceIsKaypherStream() {
    // When:
    realStream = buildStream(node);

    // Then:
    assertThat(realStream.getClass(), equalTo(SchemaKStream.class));
  }

  @Test
  public void shouldBuildStreamWithSameKeyField() {
    // When:
    final SchemaKStream<?> stream = buildStream(node);

    // Then:
    assertThat(stream.getKeyField(), is(node.getKeyField()));
  }

  @Test
  public void shouldBuildSchemaKTableWhenKTableSource() {
    final KaypherTable<String> table = new KaypherTable<>("sqlExpression",
        SourceName.of("datasource"),
        REAL_SCHEMA,
        SerdeOption.none(),
        KeyField.of(ColumnRef.withoutSource(ColumnName.of("field1"))),
        new LongColumnTimestampExtractionPolicy(TIMESTAMP_FIELD),
        new KaypherTopic(
            "topic2",
            KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA)),
            ValueFormat.of(FormatInfo.of(Format.JSON)),
            false
        )
    );

    final DataSourceNode node = new DataSourceNode(
        PLAN_NODE_ID,
        table,
        table.getName(),
        Collections.emptyList());

    final SchemaKStream result = buildStream(node);
    assertThat(result.getClass(), equalTo(SchemaKTable.class));
  }

  @Test
  public void shouldHaveFullyQualifiedSchema() {
    // Given:
    final SourceName sourceName = SOME_SOURCE.getName();

    // When:
    final LogicalSchema schema = node.getSchema();

    // Then:
    assertThat(schema, is(
        LogicalSchema.builder()
            .valueColumn(SchemaUtil.ROWTIME_NAME, SqlTypes.BIGINT)
            .valueColumn(SchemaUtil.ROWKEY_NAME, SqlTypes.STRING)
            .valueColumn(ColumnName.of("field1"), SqlTypes.STRING)
            .valueColumn(ColumnName.of("field2"), SqlTypes.STRING)
            .valueColumn(ColumnName.of("field3"), SqlTypes.STRING)
            .valueColumn(TIMESTAMP_FIELD.name(), SqlTypes.BIGINT)
            .valueColumn(ColumnName.of("key"), SqlTypes.STRING)
            .build().withAlias(sourceName)));
  }

  @Test
  public void shouldBuildSourceStreamWithCorrectTimestampIndex() {
    // Given:
    reset(timestampExtractionPolicy);
    when(timestampExtractionPolicy.timestampField()).thenReturn(ColumnRef.withoutSource(FIELD2));
    final DataSourceNode node = buildNodeWithMockSource();

    // When:
    node.buildStream(kaypherStreamBuilder);

    // Then:
    verify(schemaKStreamFactory).create(any(), any(), any(), any(), eq(1), any(), any());
  }

  // should this even be possible? if you are using a timestamp extractor then shouldn't the name
  // should be unqualified
  @Test
  public void shouldBuildSourceStreamWithCorrectTimestampIndexForQualifiedFieldName() {
    // Given:
    reset(timestampExtractionPolicy);
    when(timestampExtractionPolicy.timestampField())
        .thenReturn(ColumnRef.withoutSource(ColumnName.of("field2")));
    final DataSourceNode node = buildNodeWithMockSource();

    // When:
    node.buildStream(kaypherStreamBuilder);

    // Then:
    verify(schemaKStreamFactory).create(any(), any(), any(), any(), eq(1), any(), any());
  }

  @Test
  public void shouldBuildSourceStreamWithCorrectParams() {
    // Given:
    when(dataSource.getDataSourceType()).thenReturn(DataSourceType.KSTREAM);
    final DataSourceNode node = buildNodeWithMockSource();

    // When:
    final SchemaKStream returned = node.buildStream(kaypherStreamBuilder);

    // Then:
    assertThat(returned, is(stream));
    verify(schemaKStreamFactory).create(
        same(kaypherStreamBuilder),
        same(dataSource),
        eq(StreamSource.getSchemaWithMetaAndKeyFields(SourceName.of("name"), REAL_SCHEMA)),
        stackerCaptor.capture(),
        eq(3),
        eq(OFFSET_RESET),
        same(node.getKeyField())
    );
    assertThat(
        stackerCaptor.getValue().getQueryContext().getContext(),
        equalTo(ImmutableList.of("0", "source"))
    );
  }

  @Test
  public void shouldBuildSourceStreamWithCorrectParamsWhenBuildingTable() {
    // Given:
    final DataSourceNode node = buildNodeWithMockSource();

    // When:
    node.buildStream(kaypherStreamBuilder);

    // Then:
    verify(schemaKStreamFactory).create(
        same(kaypherStreamBuilder),
        same(dataSource),
        eq(StreamSource.getSchemaWithMetaAndKeyFields(SourceName.of("name"), REAL_SCHEMA)),
        stackerCaptor.capture(),
        eq(3),
        eq(OFFSET_RESET),
        same(node.getKeyField())
    );
    assertThat(
        stackerCaptor.getValue().getQueryContext().getContext(),
        equalTo(ImmutableList.of("0", "source"))
    );
  }

  @Test
  public void shouldBuildTableByConvertingFromStream() {
    // Given:
    final DataSourceNode node = buildNodeWithMockSource();

    // When:
    final SchemaKStream returned = node.buildStream(kaypherStreamBuilder);

    // Then:
    verify(stream).toTable(any(), any(), any());
    assertThat(returned, is(table));
  }

  @Test
  public void shouldBuildTableWithCorrectContext() {
    // Given:
    final DataSourceNode node = buildNodeWithMockSource();

    // When:
    node.buildStream(kaypherStreamBuilder);

    // Then:
    verify(stream).toTable(any(), any(), stackerCaptor.capture());
    assertThat(
        stackerCaptor.getValue().getQueryContext().getContext(),
        equalTo(ImmutableList.of("0", "reduce")));
  }

  private DataSourceNode buildNodeWithMockSource() {
    when(dataSource.getSchema()).thenReturn(REAL_SCHEMA);
    when(dataSource.getKeyField()).thenReturn(KEY_FIELD);
    return new DataSourceNode(
        PLAN_NODE_ID,
        dataSource,
        SourceName.of("name"),
        Collections.emptyList(),
        schemaKStreamFactory
    );
  }

  private SchemaKStream buildStream(final DataSourceNode node) {
    final SchemaKStream stream = node.buildStream(kaypherStreamBuilder);
    if (stream instanceof SchemaKTable) {
      final SchemaKTable table = (SchemaKTable) stream;
      table.getSourceTableStep().build(new KSPlanBuilder(kaypherStreamBuilder));
    } else {
      stream.getSourceStep().build(new KSPlanBuilder(kaypherStreamBuilder));
    }
    return stream;
  }
}
