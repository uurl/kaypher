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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import com.treutec.kaypher.execution.ddl.commands.KaypherTopic;
import com.treutec.kaypher.metastore.MutableMetaStore;
import com.treutec.kaypher.metastore.model.DataSource;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.parser.KaypherParser.PreparedStatement;
import com.treutec.kaypher.parser.tree.DropStream;
import com.treutec.kaypher.parser.tree.ListProperties;
import com.treutec.kaypher.parser.tree.Statement;
import com.treutec.kaypher.serde.Format;
import com.treutec.kaypher.serde.FormatInfo;
import com.treutec.kaypher.serde.ValueFormat;
import com.treutec.kaypher.services.KafkaTopicClient;
import com.treutec.kaypher.statement.ConfiguredStatement;
import com.treutec.kaypher.util.KaypherConfig;
import com.treutec.kaypher.util.KaypherConstants;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TopicDeleteInjectorTest {

  private static final SourceName SOURCE_NAME = SourceName.of("SOMETHING");
  private static final String TOPIC_NAME = "something";
  private static final ConfiguredStatement<DropStream> DROP_WITH_DELETE_TOPIC = givenStatement(
      "DROP STREAM SOMETHING DELETE TOPIC",
      new DropStream(SOURCE_NAME, false, true));
  private static final ConfiguredStatement<DropStream> DROP_WITHOUT_DELETE_TOPIC = givenStatement(
      "DROP STREAM SOMETHING",
      new DropStream(SOURCE_NAME, false, false));

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private MutableMetaStore metaStore;
  @Mock
  private DataSource<?> source;
  @Mock
  private KaypherTopic topic;
  @Mock
  private SchemaRegistryClient registryClient;
  @Mock
  private KafkaTopicClient topicClient;

  private TopicDeleteInjector deleteInjector;

  @Before
  public void setUp() {
    deleteInjector = new TopicDeleteInjector(metaStore, topicClient, registryClient);

    when(metaStore.getSource(SOURCE_NAME)).thenAnswer(inv -> source);
    when(source.getName()).thenReturn(SOURCE_NAME);
    when(source.getKafkaTopicName()).thenReturn(TOPIC_NAME);
    when(source.getKaypherTopic()).thenReturn(topic);
    when(topic.getValueFormat()).thenReturn(ValueFormat.of(FormatInfo.of(Format.JSON)));
  }

  @Test
  public void shouldDoNothingForNonDropStatements() {
    // Given:
    final ConfiguredStatement<ListProperties> listProperties =
        givenStatement("LIST", new ListProperties(Optional.empty()));

    // When:
    final ConfiguredStatement<ListProperties> injected = deleteInjector.inject(listProperties);

    // Then:
    assertThat(injected, is(sameInstance(listProperties)));
  }

  @Test
  public void shouldDoNothingIfNoDeleteTopic() {
    // When:
    final ConfiguredStatement<DropStream> injected = deleteInjector.inject(DROP_WITHOUT_DELETE_TOPIC);

    // Then:
    assertThat(injected, is(sameInstance(DROP_WITHOUT_DELETE_TOPIC)));
    verifyNoMoreInteractions(topicClient, registryClient);
  }

  @Test
  public void shouldDropTheDeleteTopicClause() {
    // When:
    final ConfiguredStatement<DropStream> injected = deleteInjector.inject(DROP_WITH_DELETE_TOPIC);

    // Then:
    assertThat(injected.getStatementText(), is("DROP STREAM SOMETHING;"));
    assertThat("expected !isDeleteTopic", !injected.getStatement().isDeleteTopic());
  }

  @Test
  public void shouldDeleteTopic() {
    // When:
    deleteInjector.inject(DROP_WITH_DELETE_TOPIC);

    // Then:
    verify(topicClient).deleteTopics(ImmutableList.of("something"));
  }

  @Test
  public void shouldDeleteSchemaInSR() throws IOException, RestClientException {
    // Given:
    when(topic.getValueFormat()).thenReturn(ValueFormat.of(FormatInfo.of(Format.AVRO)));

    // When:
    deleteInjector.inject(DROP_WITH_DELETE_TOPIC);

    // Then:
    verify(registryClient).deleteSubject("something" + KaypherConstants.SCHEMA_REGISTRY_VALUE_SUFFIX);
  }

  @Test
  public void shouldNotDeleteSchemaInSRIfNotAvro() throws IOException, RestClientException {
    // When:
    deleteInjector.inject(DROP_WITH_DELETE_TOPIC);

    // Then:
    verify(registryClient, never()).deleteSubject(any());
  }

  @Test
  public void shouldThrowExceptionIfSourceDoesNotExist() {
    // Given:
    final ConfiguredStatement<DropStream> dropStatement = givenStatement(
        "DROP SOMETHING", new DropStream(SourceName.of("SOMETHING_ELSE"), true, true));

    // Expect:
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("Could not find source to delete topic for");

    // When:
    deleteInjector.inject(dropStatement);
  }

  @Test
  public void shouldNotThrowIfNoOtherSourcesUsingTopic() {
    // Given:
    final ConfiguredStatement<DropStream> dropStatement = givenStatement(
        "DROP SOMETHING DELETE TOPIC;",
        new DropStream(SOURCE_NAME,
            true,
            true)
    );
    final DataSource<?> other1 = givenSource(SourceName.of("OTHER"), "other");
    final Map<SourceName, DataSource<?>> sources = new HashMap<>();
    sources.put(SOURCE_NAME, source);
    sources.put(SourceName.of("OTHER"), other1);
    when(metaStore.getAllDataSources()).thenReturn(sources);

    // When:
    deleteInjector.inject(dropStatement);
  }

  @Test
  public void shouldThrowExceptionIfOtherSourcesUsingTopic() {
    // Given:
    final ConfiguredStatement<DropStream> dropStatement = givenStatement(
        "DROP SOMETHING DELETE TOPIC;",
        new DropStream(SOURCE_NAME,
            true,
            true)
    );
    final DataSource<?> other1 = givenSource(SourceName.of("OTHER1"), TOPIC_NAME);
    final DataSource<?> other2 = givenSource(SourceName.of("OTHER2"), TOPIC_NAME);
    final Map<SourceName, DataSource<?>> sources = new HashMap<>();
    sources.put(SOURCE_NAME, source);
    sources.put(SourceName.of("OTHER1"), other1);
    sources.put(SourceName.of("OTHER2"), other2);
    when(metaStore.getAllDataSources()).thenReturn(sources);

    // Expect:
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage(
        "Refusing to delete topic. "
            + "Found other data sources (OTHER1, OTHER2) using topic something");

    // When:
    deleteInjector.inject(dropStatement);
  }

  @Test
  public void shouldThrowIfTopicDoesNotExist() {
    // Given:
    final SourceName STREAM_1 = SourceName.of("stream1");
    final DataSource<?> other1 = givenSource(STREAM_1, "topicName");
    when(metaStore.getSource(STREAM_1)).thenAnswer(inv -> other1);
    when(other1.getKafkaTopicName()).thenReturn("topicName");
    final ConfiguredStatement<DropStream> dropStatement = givenStatement(
        "DROP stream1 DELETE TOPIC;",
        new DropStream(SourceName.of("stream1"), true, true)
    );
    doThrow(RuntimeException.class).when(topicClient).deleteTopics(ImmutableList.of("topicName"));

    // Expect:
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("" +
        "Could not delete the corresponding kafka topic: topicName");

    // When:
    deleteInjector.inject(dropStatement);
  }

  @Test
  public void shouldNotThrowIfSchemaIsMissing() throws IOException, RestClientException {
    // Given:
    when(topic.getValueFormat())
        .thenReturn(ValueFormat.of(FormatInfo.of(Format.AVRO, Optional.of("foo"), Optional.empty())));

    doThrow(new RestClientException("Subject not found.", 404, 40401))
            .when(registryClient).deleteSubject("something" + KaypherConstants.SCHEMA_REGISTRY_VALUE_SUFFIX);

    // When:
    deleteInjector.inject(DROP_WITH_DELETE_TOPIC);
  }

  private static DataSource<?> givenSource(final SourceName name, final String topicName) {
    final DataSource source = mock(DataSource.class);
    when(source.getName()).thenReturn(name);
    when(source.getKafkaTopicName()).thenReturn(topicName);
    return source;
  }

  private static <T extends Statement> ConfiguredStatement<T> givenStatement(
      final String text,
      final T statement
  ) {
    return ConfiguredStatement.of(
        PreparedStatement.of(text, statement),
        ImmutableMap.of(),
        new KaypherConfig(ImmutableMap.of())
    );
  }
}