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
package com.treutec.kaypher.embedded;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import com.treutec.kaypher.KaypherExecutionContext;
import com.treutec.kaypher.KaypherExecutionContext.ExecuteResult;
import com.treutec.kaypher.engine.KaypherEngine;
import com.treutec.kaypher.parser.KaypherParser.ParsedStatement;
import com.treutec.kaypher.parser.KaypherParser.PreparedStatement;
import com.treutec.kaypher.parser.SqlBaseParser.SingleStatementContext;
import com.treutec.kaypher.parser.tree.SetProperty;
import com.treutec.kaypher.parser.tree.Statement;
import com.treutec.kaypher.parser.tree.UnsetProperty;
import com.treutec.kaypher.services.ServiceContext;
import com.treutec.kaypher.statement.ConfiguredStatement;
import com.treutec.kaypher.statement.Injector;
import com.treutec.kaypher.statement.InjectorChain;
import com.treutec.kaypher.util.KaypherConfig;
import com.treutec.kaypher.util.KaypherException;
import com.treutec.kaypher.util.PersistentQueryMetadata;
import com.treutec.kaypher.util.TransientQueryMetadata;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
@RunWith(MockitoJUnitRunner.class)
public class KaypherContextTest {

  private static final KaypherConfig SOME_CONFIG = new KaypherConfig(Collections.emptyMap());
  private static final ImmutableMap<String, Object> SOME_PROPERTIES = ImmutableMap
      .of("overridden", "props");

  private final static ParsedStatement PARSED_STMT_0 = ParsedStatement
      .of("sql 0", mock(SingleStatementContext.class));

  private final static ParsedStatement PARSED_STMT_1 = ParsedStatement
      .of("sql 1", mock(SingleStatementContext.class));

  private final static PreparedStatement<?> PREPARED_STMT_0 = PreparedStatement
      .of("sql 0", mock(Statement.class));

  private final static PreparedStatement<?> PREPARED_STMT_1 = PreparedStatement
      .of("sql 1", mock(Statement.class));

  private final static ConfiguredStatement<?> CFG_STMT_0 = ConfiguredStatement.of(
      PREPARED_STMT_0, SOME_PROPERTIES, SOME_CONFIG);

  private final static ConfiguredStatement<?> CFG_STMT_1 = ConfiguredStatement.of(
      PREPARED_STMT_1, SOME_PROPERTIES, SOME_CONFIG);

  private final static ConfiguredStatement<?> STMT_0_WITH_SCHEMA = ConfiguredStatement.of(
      PREPARED_STMT_0, SOME_PROPERTIES, SOME_CONFIG);

  private final static ConfiguredStatement<?> STMT_1_WITH_SCHEMA = ConfiguredStatement.of(
      PREPARED_STMT_1, SOME_PROPERTIES, SOME_CONFIG);

  private final static ConfiguredStatement<?> STMT_0_WITH_TOPIC = ConfiguredStatement.of(
      PREPARED_STMT_0, SOME_PROPERTIES, SOME_CONFIG);

  private final static ConfiguredStatement<?> STMT_1_WITH_TOPIC = ConfiguredStatement.of(
      PREPARED_STMT_1, SOME_PROPERTIES, SOME_CONFIG);

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private ServiceContext serviceContext;
  @Mock
  private KaypherEngine kaypherEngine;
  @Mock
  private KaypherExecutionContext sandbox;
  @Mock
  private PersistentQueryMetadata persistentQuery;
  @Mock
  private TransientQueryMetadata transientQuery;
  @Mock
  private Injector schemaInjector;
  @Mock
  private Injector topicInjector;

  private KaypherContext kaypherContext;

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    when(kaypherEngine.parse(any())).thenReturn(ImmutableList.of(PARSED_STMT_0));

    when(kaypherEngine.prepare(PARSED_STMT_0)).thenReturn((PreparedStatement) PREPARED_STMT_0);
    when(kaypherEngine.prepare(PARSED_STMT_1)).thenReturn((PreparedStatement) PREPARED_STMT_1);

    when(kaypherEngine.execute(any(), any())).thenReturn(ExecuteResult.of("success"));

    when(kaypherEngine.createSandbox(any())).thenReturn(sandbox);

    when(kaypherEngine.getServiceContext()).thenReturn(serviceContext);

    when(sandbox.prepare(PARSED_STMT_0)).thenReturn((PreparedStatement) PREPARED_STMT_0);
    when(sandbox.prepare(PARSED_STMT_1)).thenReturn((PreparedStatement) PREPARED_STMT_1);

    when(schemaInjector.inject(any())).thenAnswer(inv -> inv.getArgument(0));

    when(topicInjector.inject(any())).thenAnswer(inv -> inv.getArgument(0));

    kaypherContext = new KaypherContext(
        serviceContext,
        SOME_CONFIG,
        kaypherEngine,
        (ec, sc) -> InjectorChain.of(schemaInjector, topicInjector));

  }

  @Test
  public void shouldParseStatements() {
    // When:
    kaypherContext.sql("Some SQL", SOME_PROPERTIES);

    // Then:
    verify(kaypherEngine).parse("Some SQL");
  }

  @Test
  public void shouldOnlyPrepareNextStatementOncePreviousStatementHasBeenExecuted() {
    // Given:
    when(kaypherEngine.parse(any())).thenReturn(
        ImmutableList.of(PARSED_STMT_0, PARSED_STMT_1));

    // When:
    kaypherContext.sql("Some SQL", SOME_PROPERTIES);

    // Then:
    final InOrder inOrder = inOrder(kaypherEngine);
    inOrder.verify(kaypherEngine).prepare(PARSED_STMT_0);
    inOrder.verify(kaypherEngine).execute(serviceContext, STMT_0_WITH_SCHEMA);
    inOrder.verify(kaypherEngine).prepare(PARSED_STMT_1);
    inOrder.verify(kaypherEngine).execute(serviceContext, STMT_1_WITH_SCHEMA);
  }

  @Test
  public void shouldTryExecuteStatementsReturnedByParserBeforeExecute() {
    // Given:
    when(kaypherEngine.parse(any())).thenReturn(
        ImmutableList.of(PARSED_STMT_0, PARSED_STMT_1));

    // When:
    kaypherContext.sql("Some SQL", SOME_PROPERTIES);

    // Then:
    final InOrder inOrder = inOrder(kaypherEngine, sandbox);
    inOrder.verify(sandbox).execute(sandbox.getServiceContext(), STMT_0_WITH_SCHEMA);
    inOrder.verify(sandbox).execute(sandbox.getServiceContext(), STMT_1_WITH_SCHEMA);
    inOrder.verify(kaypherEngine).execute(kaypherEngine.getServiceContext(), STMT_0_WITH_SCHEMA);
    inOrder.verify(kaypherEngine).execute(kaypherEngine.getServiceContext(), STMT_1_WITH_SCHEMA);
  }

  @Test
  public void shouldThrowIfParseFails() {
    // Given:
    when(kaypherEngine.parse(any()))
        .thenThrow(new KaypherException("Bad tings happen"));

    // Expect
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage("Bad tings happen");

    // When:
    kaypherContext.sql("Some SQL", SOME_PROPERTIES);
  }

  @Test
  public void shouldThrowIfSandboxExecuteThrows() {
    // Given:
    when(sandbox.execute(any(), any()))
        .thenThrow(new KaypherException("Bad tings happen"));

    // Expect
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage("Bad tings happen");

    // When:
    kaypherContext.sql("Some SQL", SOME_PROPERTIES);
  }

  @Test
  public void shouldThrowIfExecuteThrows() {
    // Given:
    when(kaypherEngine.execute(any(), any()))
        .thenThrow(new KaypherException("Bad tings happen"));

    // Expect
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage("Bad tings happen");

    // When:
    kaypherContext.sql("Some SQL", SOME_PROPERTIES);
  }

  @Test
  public void shouldNotExecuteAnyStatementsIfTryExecuteThrows() {
    // Given:
    when(sandbox.execute(any(), any()))
        .thenThrow(new KaypherException("Bad tings happen"));

    // When:
    try {
      kaypherContext.sql("Some SQL", SOME_PROPERTIES);
    } catch (final KaypherException e) {
      // expected
    }

    // Then:
    verify(kaypherEngine, never()).execute(any(), any());
  }

  @Test
  public void shouldStartPersistentQueries() {
    // Given:
    when(kaypherEngine.execute(any(), any()))
        .thenReturn(ExecuteResult.of(persistentQuery));

    // When:
    kaypherContext.sql("Some SQL", SOME_PROPERTIES);

    // Then:
    verify(persistentQuery).start();
  }

  @Test
  public void shouldNotBlowUpOnSqlThatDoesNotResultInPersistentQueries() {
    // Given:
    when(kaypherEngine.execute(any(), any()))
        .thenReturn(ExecuteResult.of(transientQuery));

    // When:
    kaypherContext.sql("Some SQL", SOME_PROPERTIES);

    // Then:
    // Did not blow up.
  }

  @Test
  public void shouldCloseEngineBeforeServiceContextOnClose() {
    // When:
    kaypherContext.close();

    // Then:
    final InOrder inOrder = inOrder(kaypherEngine, serviceContext);
    inOrder.verify(kaypherEngine).close();
    inOrder.verify(serviceContext).close();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldInferSchema() {
    // Given:
    when(schemaInjector.inject(any())).thenReturn((ConfiguredStatement) CFG_STMT_0);

    // When:
    kaypherContext.sql("Some SQL", SOME_PROPERTIES);

    // Then:
    verify(kaypherEngine).execute(eq(serviceContext), eq(STMT_0_WITH_SCHEMA));
  }

  @Test
  public void shouldThrowIfFailedToInferSchema() {
    // Given:
    when(schemaInjector.inject(any()))
        .thenThrow(new RuntimeException("Boom"));

    // Then:
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("Boom");

    // When:
    kaypherContext.sql("Some SQL", SOME_PROPERTIES);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldInferTopic() {
    // Given:
    when(topicInjector.inject(any()))
        .thenReturn((ConfiguredStatement) STMT_0_WITH_TOPIC);

    // When:
    kaypherContext.sql("Some SQL", SOME_PROPERTIES);

    // Then:
    verify(kaypherEngine).execute(eq(serviceContext), eq(STMT_0_WITH_TOPIC));
  }

  @Test
  public void shouldInferTopicWithValidArgs() {
    // Given:
    when(schemaInjector.inject(any())).thenAnswer(inv -> inv.getArgument(0));

    // When:
    kaypherContext.sql("Some SQL", SOME_PROPERTIES);

    // Then:
    verify(topicInjector, times(2) /* once to validate, once to execute */)
        .inject(CFG_STMT_0);
  }

  @Test
  public void shouldThrowIfFailedToInferTopic() {
    // Given:
    when(topicInjector.inject(any()))
        .thenThrow(new RuntimeException("Boom"));

    // Then:
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("Boom");

    // When:
    kaypherContext.sql("Some SQL", SOME_PROPERTIES);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldInferTopicAfterInferringSchema() {
    // Given:
    when(schemaInjector.inject(any())).thenReturn((ConfiguredStatement) STMT_1_WITH_SCHEMA);
    when(topicInjector.inject(eq(CFG_STMT_1))).thenReturn((ConfiguredStatement) STMT_1_WITH_TOPIC);

    // When:
    kaypherContext.sql("Some SQL", SOME_PROPERTIES);

    // Then:
    verify(kaypherEngine).execute(serviceContext, STMT_1_WITH_TOPIC);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldSetProperty() {
    // Given:
    when(kaypherEngine.parse(any())).thenReturn(ImmutableList.of(PARSED_STMT_0, PARSED_STMT_0));

    final PreparedStatement<SetProperty> set = PreparedStatement.of(
        "SET SOMETHING",
        new SetProperty(Optional.empty(), ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    );

    when(kaypherEngine.prepare(any()))
        .thenReturn((PreparedStatement) set)
        .thenReturn(PREPARED_STMT_0);

    // When:
    kaypherContext.sql("SQL;", ImmutableMap.of());

    // Then:
    verify(kaypherEngine).execute(
        serviceContext,
        ConfiguredStatement.of(
            PREPARED_STMT_0, ImmutableMap.of(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
            ),
            SOME_CONFIG
        ));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldSetPropertyOnlyOnCommandsFollowingTheSetStatement() {
    // Given:
    when(kaypherEngine.parse(any())).thenReturn(ImmutableList.of(PARSED_STMT_0, PARSED_STMT_0));

    final PreparedStatement<SetProperty> set = PreparedStatement.of(
        "SET SOMETHING",
        new SetProperty(Optional.empty(), ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    );

    when(kaypherEngine.prepare(any()))
        .thenReturn((PreparedStatement) PREPARED_STMT_0)
        .thenReturn(set);

    // When:
    kaypherContext.sql("SQL;", ImmutableMap.of());

    // Then:
    verify(kaypherEngine).execute(
        serviceContext,
        ConfiguredStatement.of(
            PREPARED_STMT_0, ImmutableMap.of(), SOME_CONFIG
        ));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldUnsetProperty() {
    // Given:
    when(kaypherEngine.parse(any())).thenReturn(ImmutableList.of(PARSED_STMT_0, PARSED_STMT_0));

    final Map<String, Object> properties = ImmutableMap
        .of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    final PreparedStatement<UnsetProperty> unset = PreparedStatement.of(
        "UNSET SOMETHING",
        new UnsetProperty(Optional.empty(), ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));

    when(kaypherEngine.prepare(any()))
        .thenReturn((PreparedStatement) unset)
        .thenReturn(PREPARED_STMT_0);

    // When:
    kaypherContext.sql("SQL;", properties);

    // Then:
    verify(kaypherEngine).execute(
        serviceContext,
        ConfiguredStatement.of(
            PREPARED_STMT_0, ImmutableMap.of(), SOME_CONFIG
        ));
  }
}
