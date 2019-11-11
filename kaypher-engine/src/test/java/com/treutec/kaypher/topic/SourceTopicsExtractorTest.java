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
package com.treutec.kaypher.topic;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

import com.treutec.kaypher.engine.KaypherEngine;
import com.treutec.kaypher.engine.KaypherEngineTestUtil;
import com.treutec.kaypher.execution.ddl.commands.KaypherTopic;
import com.treutec.kaypher.function.InternalFunctionRegistry;
import com.treutec.kaypher.metastore.MetaStoreImpl;
import com.treutec.kaypher.metastore.MutableMetaStore;
import com.treutec.kaypher.metastore.model.KeyField;
import com.treutec.kaypher.metastore.model.KaypherStream;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.parser.tree.Statement;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.schema.kaypher.types.SqlTypes;
import com.treutec.kaypher.serde.Format;
import com.treutec.kaypher.serde.FormatInfo;
import com.treutec.kaypher.serde.KeyFormat;
import com.treutec.kaypher.serde.SerdeOption;
import com.treutec.kaypher.serde.ValueFormat;
import com.treutec.kaypher.services.KafkaTopicClient;
import com.treutec.kaypher.services.ServiceContext;
import com.treutec.kaypher.util.KaypherException;
import com.treutec.kaypher.util.timestamp.MetadataTimestampExtractionPolicy;
import org.apache.kafka.clients.admin.TopicDescription;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SourceTopicsExtractorTest {

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("F1"), SqlTypes.STRING)
      .build();

  private static final String STREAM_TOPIC_1 = "s1";
  private static final String STREAM_TOPIC_2 = "s2";

  @Mock
  private ServiceContext serviceContext;
  @Mock
  private KafkaTopicClient kafkaTopicClient;
  @Mock
  private TopicDescription TOPIC_1;
  @Mock
  private TopicDescription TOPIC_2;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private SourceTopicsExtractor extractor;
  private KaypherEngine kaypherEngine;
  private MutableMetaStore metaStore;

  @Before
  public void setUp() {
    metaStore = new MetaStoreImpl(new InternalFunctionRegistry());
    kaypherEngine = KaypherEngineTestUtil.createKaypherEngine(serviceContext, metaStore);
    extractor = new SourceTopicsExtractor(metaStore);

    givenTopic("topic1", TOPIC_1);
    givenStreamWithTopic(STREAM_TOPIC_1, TOPIC_1);

    givenTopic("topic2", TOPIC_2);
    givenStreamWithTopic(STREAM_TOPIC_2, TOPIC_2);
  }

  @After
  public void closeEngine() {
    kaypherEngine.close();
  }

  private Statement givenStatement(final String sql) {
    return kaypherEngine.prepare(kaypherEngine.parse(sql).get(0)).getStatement();
  }

  @Test
  public void shouldExtractTopicFromSimpleSelect() {
    // Given:
    final Statement statement = givenStatement("SELECT * FROM " + STREAM_TOPIC_1 + ";");

    // When:
    extractor.process(statement, null);

    // Then:
    assertThat(extractor.getPrimaryKafkaTopicName(), is(TOPIC_1.name()));
  }

  @Test
  public void shouldExtractPrimaryTopicFromJoinSelect() {
    // Given:
    final Statement statement = givenStatement(String.format(
        "SELECT * FROM %s A JOIN %s B ON A.F1 = B.F1;", STREAM_TOPIC_1, STREAM_TOPIC_2
    ));

    // When:
    extractor.process(statement, null);

    // Then:
    assertThat(extractor.getPrimaryKafkaTopicName(), is(TOPIC_1.name()));
  }

  @Test
  public void shouldExtractJoinTopicsFromJoinSelect() {
    // Given:
    final Statement statement = givenStatement(String.format(
        "SELECT * FROM %s A JOIN %s B ON A.F1 = B.F1;", STREAM_TOPIC_1, STREAM_TOPIC_2
    ));

    // When:
    extractor.process(statement, null);

    // Then:
    assertThat(extractor.getSourceTopics(), contains(TOPIC_1.name(), TOPIC_2.name()));
  }

  @Test
  public void shouldFailIfSourceTopicNotInMetastore() {
    // Given:
    final Statement statement = givenStatement("SELECT * FROM " + STREAM_TOPIC_1 + ";");
    metaStore.deleteSource(SourceName.of(STREAM_TOPIC_1.toUpperCase()));

    // Then:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage(STREAM_TOPIC_1.toUpperCase() + " does not exist.");

    // When:
    extractor.process(statement, null);
  }

  private void givenStreamWithTopic(
      final String streamName,
      final TopicDescription topicDescription
  ) {
    final KaypherTopic sourceTopic = new KaypherTopic(
        topicDescription.name(),
        KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA)),
        ValueFormat.of(FormatInfo.of(Format.JSON)),
        false
    );

    final KaypherStream<?> streamSource = new KaypherStream<>(
        "",
        SourceName.of(streamName.toUpperCase()),
        SCHEMA,
        SerdeOption.none(),
        KeyField.none(),
        new MetadataTimestampExtractionPolicy(),
        sourceTopic
    );

    metaStore.putSource(streamSource);
  }

  private static void givenTopic(final String topicName, final TopicDescription topicDescription) {
    when(topicDescription.name()).thenReturn(topicName);
  }
}
