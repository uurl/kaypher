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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.treutec.kaypher.execution.builder.KaypherQueryBuilder;
import com.treutec.kaypher.execution.context.QueryContext;
import com.treutec.kaypher.execution.ddl.commands.KaypherTopic;
import com.treutec.kaypher.metastore.model.DataSource.DataSourceType;
import com.treutec.kaypher.metastore.model.KeyField;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.query.QueryId;
import com.treutec.kaypher.query.id.QueryIdGenerator;
import com.treutec.kaypher.schema.kaypher.ColumnRef;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.schema.kaypher.types.SqlTypes;
import com.treutec.kaypher.serde.Format;
import com.treutec.kaypher.serde.FormatInfo;
import com.treutec.kaypher.serde.SerdeOption;
import com.treutec.kaypher.serde.ValueFormat;
import com.treutec.kaypher.structured.SchemaKStream;
import com.treutec.kaypher.util.timestamp.LongColumnTimestampExtractionPolicy;
import java.util.Optional;
import java.util.OptionalInt;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("OptionalGetWithoutIsPresent")
@RunWith(MockitoJUnitRunner.class)
public class KaypherStructuredDataOutputNodeTest {

  private static final String QUERY_ID_VALUE = "output-test";
  private static final QueryId QUERY_ID = new QueryId(QUERY_ID_VALUE);

  private static final String SINK_KAFKA_TOPIC_NAME = "output_kafka";

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("field1"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("field2"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("field3"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("timestamp"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("key"), SqlTypes.STRING)
      .build();

  private static final KeyField KEY_FIELD = KeyField
      .of(ColumnRef.withoutSource(ColumnName.of("key")));

  private static final PlanNodeId PLAN_NODE_ID = new PlanNodeId("0");
  private static final ValueFormat JSON_FORMAT = ValueFormat.of(FormatInfo.of(Format.JSON));

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private QueryIdGenerator queryIdGenerator;
  @Mock
  private KaypherQueryBuilder kaypherStreamBuilder;
  @Mock
  private PlanNode sourceNode;
  @Mock
  private SchemaKStream<String> sourceStream;
  @Mock
  private SchemaKStream<String> resultStream;
  @Mock
  private SchemaKStream<?> sinkStream;
  @Mock
  private SchemaKStream<?> resultWithKeySelected;
  @Mock
  private SchemaKStream<?> sinkStreamWithKeySelected;
  @Mock
  private KaypherTopic kaypherTopic;
  @Captor
  private ArgumentCaptor<QueryContext.Stacker> stackerCaptor;

  private KaypherStructuredDataOutputNode outputNode;
  private LogicalSchema schema;
  private Optional<ColumnRef> partitionBy;
  private boolean createInto;

  @SuppressWarnings("unchecked")
  @Before
  public void before() {
    schema = SCHEMA;
    partitionBy = Optional.empty();
    createInto = true;

    when(queryIdGenerator.getNext()).thenReturn(QUERY_ID_VALUE);

    when(sourceNode.getNodeOutputType()).thenReturn(DataSourceType.KSTREAM);
    when(sourceNode.buildStream(kaypherStreamBuilder)).thenReturn((SchemaKStream) sourceStream);

    when(sourceStream.into(any(), any(), any(), any(), any()))
        .thenReturn((SchemaKStream) sinkStream);
    when(sourceStream.selectKey(any(), anyBoolean(), any()))
        .thenReturn((SchemaKStream) resultWithKeySelected);
    when(resultWithKeySelected.into(any(), any(), any(), any(), any()))
        .thenReturn((SchemaKStream) sinkStreamWithKeySelected);

    when(kaypherStreamBuilder.buildNodeContext(any())).thenAnswer(inv ->
        new QueryContext.Stacker()
            .push(inv.getArgument(0).toString()));
    when(kaypherTopic.getKafkaTopicName()).thenReturn(SINK_KAFKA_TOPIC_NAME);
    when(kaypherTopic.getValueFormat()).thenReturn(JSON_FORMAT);

    buildNode();
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfPartitionByAndKeyFieldNone() {
    // When:
    new KaypherStructuredDataOutputNode(
        new PlanNodeId("0"),
        sourceNode,
        SCHEMA,
        new LongColumnTimestampExtractionPolicy(ColumnRef.withoutSource(ColumnName.of("timestamp"))),
        KeyField.none(),
        kaypherTopic,
        Optional.of(ColumnRef.withoutSource(ColumnName.of("something"))),
        OptionalInt.empty(),
        false,
        SerdeOption.none(),
        SourceName.of("0"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfPartitionByDoesNotMatchKeyField() {
    // When:
    new KaypherStructuredDataOutputNode(
        new PlanNodeId("0"),
        sourceNode,
        SCHEMA,
        new LongColumnTimestampExtractionPolicy(ColumnRef.withoutSource(ColumnName.of("timestamp"))),
        KeyField.of(Optional.of(ColumnRef.withoutSource(ColumnName.of("something else")))),
        kaypherTopic,
        Optional.of(ColumnRef.withoutSource(ColumnName.of("something"))),
        OptionalInt.empty(),
        false,
        SerdeOption.none(),
        SourceName.of("0"));
  }

  @Test
  public void shouldBuildSourceNode() {
    // When:
    outputNode.buildStream(kaypherStreamBuilder);

    // Then:
    verify(sourceNode).buildStream(kaypherStreamBuilder);
  }

  @Test
  public void shouldPartitionByFieldNameInPartitionByProperty() {
    // Given:
    givenNodePartitioningByKey("key");

    // When:
    final SchemaKStream<?> result = outputNode.buildStream(kaypherStreamBuilder);

    // Then:
    verify(sourceStream).selectKey(
        KEY_FIELD.ref().get(),
        false,
        new QueryContext.Stacker().push(PLAN_NODE_ID.toString())
    );

    assertThat(result, is(sameInstance(sinkStreamWithKeySelected)));
  }

  @Test
  public void shouldPartitionByRowKey() {
    // Given:
    givenNodePartitioningByKey("ROWKEY");

    // When:
    final SchemaKStream<?> result = outputNode.buildStream(kaypherStreamBuilder);

    // Then:
    verify(sourceStream).selectKey(
        ColumnRef.withoutSource(ColumnName.of("ROWKEY")),
        false,
        new QueryContext.Stacker().push(PLAN_NODE_ID.toString())
    );

    assertThat(result, is(sameInstance(sinkStreamWithKeySelected)));
  }

  @Test
  public void shouldPartitionByRowTime() {
    // Given:
    givenNodePartitioningByKey("ROWTIME");

    // When:
    final SchemaKStream<?> result = outputNode.buildStream(kaypherStreamBuilder);

    // Then:
    verify(sourceStream).selectKey(
        ColumnRef.withoutSource(ColumnName.of("ROWTIME")),
        false,
        new QueryContext.Stacker().push(PLAN_NODE_ID.toString())
    );

    assertThat(result, is(sameInstance(sinkStreamWithKeySelected)));
  }

  @Test
  public void shouldComputeQueryIdCorrectlyForStream() {
    // When:
    final QueryId queryId = outputNode.getQueryId(queryIdGenerator);

    // Then:
    verify(queryIdGenerator, times(1)).getNext();
    assertThat(queryId, equalTo(new QueryId("CSAS_0_" + QUERY_ID_VALUE)));
  }

  @Test
  public void shouldComputeQueryIdCorrectlyForTable() {
    // Given:
    when(sourceNode.getNodeOutputType()).thenReturn(DataSourceType.KTABLE);
    givenNodeWithSchema(SCHEMA);

    // When:
    final QueryId queryId = outputNode.getQueryId(queryIdGenerator);

    // Then:
    verify(queryIdGenerator, times(1)).getNext();
    assertThat(queryId, equalTo(new QueryId("CTAS_0_" + QUERY_ID_VALUE)));
  }

  @Test
  public void shouldComputeQueryIdCorrectlyForInsertInto() {
    // Given:
    givenInsertIntoNode();

    // When:
    final QueryId queryId = outputNode.getQueryId(queryIdGenerator);

    // Then:
    verify(queryIdGenerator, times(1)).getNext();
    assertThat(queryId, equalTo(new QueryId("InsertQuery_" + QUERY_ID_VALUE)));
  }

  @Test
  public void shouldBuildOutputNodeForInsertIntoAvroFromNonAvro() {
    // Given:
    givenInsertIntoNode();

    final ValueFormat valueFormat = ValueFormat.of(FormatInfo.of(Format.AVRO, Optional.of("name"),
        Optional.empty()));

    when(kaypherTopic.getValueFormat()).thenReturn(valueFormat);

    // When/Then (should not throw):
    outputNode.buildStream(kaypherStreamBuilder);

    // Then:
    verify(sourceStream).into(any(), any(), eq(valueFormat), any(), any());
  }

  @Test
  public void shouldCallInto() {
    // When:
    final SchemaKStream result = outputNode.buildStream(kaypherStreamBuilder);

    // Then:
    verify(sourceStream).into(
        eq(SINK_KAFKA_TOPIC_NAME),
        eq(SCHEMA),
        eq(JSON_FORMAT),
        eq(SerdeOption.none()),
        stackerCaptor.capture()
    );
    assertThat(
        stackerCaptor.getValue().getQueryContext().getContext(),
        equalTo(ImmutableList.of("0"))
    );
    assertThat(result, sameInstance(sinkStream));
  }

  private void givenInsertIntoNode() {
    this.createInto = false;
    buildNode();
  }

  private void givenNodePartitioningByKey(final String field) {
    this.partitionBy = Optional.of(ColumnRef.withoutSource(ColumnName.of(field)));
    buildNode();
  }

  private void givenNodeWithSchema(final LogicalSchema schema) {
    this.schema = schema;
    buildNode();
  }

  private void buildNode() {
    outputNode = new KaypherStructuredDataOutputNode(
        PLAN_NODE_ID,
        sourceNode,
        schema,
        new LongColumnTimestampExtractionPolicy(ColumnRef.withoutSource(ColumnName.of("timestamp"))),
        KEY_FIELD,
        kaypherTopic,
        partitionBy,
        OptionalInt.empty(),
        createInto,
        SerdeOption.none(),
        SourceName.of(PLAN_NODE_ID.toString()));
  }
}