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
package com.treutec.kaypher.util;

import static com.treutec.kaypher.metastore.model.DataSource.DataSourceType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.treutec.kaypher.internal.QueryStateListener;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.schema.kaypher.types.SqlTypes;
import java.util.Collections;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.Topology;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class QueryMetadataTest {

  private static final String QUERY_APPLICATION_ID = "Query1";
  private static final LogicalSchema SOME_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("f0"), SqlTypes.STRING)
      .build();
  private static final Set<SourceName> SOME_SOURCES = ImmutableSet.of(SourceName.of("s1"), SourceName.of("s2"));

  @Mock
  private Topology topoplogy;
  @Mock
  private KafkaStreams kafkaStreams;
  @Mock
  private QueryStateListener listener;
  @Mock
  private Consumer<QueryMetadata> closeCallback;
  private QueryMetadata query;

  @Before
  public void setup() {
    query = new QueryMetadata(
        "foo",
        kafkaStreams,
        SOME_SCHEMA,
        SOME_SOURCES,
        "bar",
        QUERY_APPLICATION_ID,
        topoplogy,
        Collections.emptyMap(),
        Collections.emptyMap(),
        closeCallback
    );
  }

  @Test
  public void shouldSetInitialStateWhenListenerAdd() {
    // Given:
    when(kafkaStreams.state()).thenReturn(State.CREATED);

    // When:
    query.registerQueryStateListener(listener);

    // Then:
    verify(listener).onChange(State.CREATED, State.CREATED);
  }

  @Test
  public void shouldConnectAnyListenerToStreamAppOnStart() {
    // Given:
    query.registerQueryStateListener(listener);

    // When:
    query.start();

    // Then:
    verify(kafkaStreams).setStateListener(listener);
  }

  @Test
  public void shouldCloseAnyListenerOnClose() {
    // Given:
    query.registerQueryStateListener(listener);

    // When:
    query.close();

    // Then:
    verify(listener).close();
  }

  @Test
  public void shouldReturnStreamState() {
    // Given:
    when(kafkaStreams.state()).thenReturn(State.PENDING_SHUTDOWN);

    // When:
    final String state = query.getState();

    // Then:
    assertThat(state, is("PENDING_SHUTDOWN"));
  }

  @Test
  public void shouldCloseKStreamsAppOnCloseThenCloseCallback() {
    // When:
    query.close();

    // Then:
    final InOrder inOrder = inOrder(kafkaStreams, closeCallback);
    inOrder.verify(kafkaStreams).close();
    inOrder.verify(closeCallback).accept(query);
  }

  @Test
  public void shouldCleanUpKStreamsAppAfterCloseOnClose() {
    // When:
    query.close();

    // Then:
    final InOrder inOrder = inOrder(kafkaStreams);
    inOrder.verify(kafkaStreams).close();
    inOrder.verify(kafkaStreams).cleanUp();
  }

  @Test
  public void shouldReturnSources() {
    assertThat(query.getSourceNames(), is(SOME_SOURCES));
  }

  @Test
  public void shouldReturnSchema() {
    assertThat(query.getLogicalSchema(), is(SOME_SCHEMA));
  }
}
