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
package com.treutec.kaypher.ddl.commands;

import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.treutec.kaypher.execution.ddl.commands.CreateStreamCommand;
import com.treutec.kaypher.execution.ddl.commands.CreateTableCommand;
import com.treutec.kaypher.execution.ddl.commands.DdlCommand;
import com.treutec.kaypher.execution.ddl.commands.DropSourceCommand;
import com.treutec.kaypher.execution.ddl.commands.DropTypeCommand;
import com.treutec.kaypher.execution.ddl.commands.RegisterTypeCommand;
import com.treutec.kaypher.execution.expression.tree.Literal;
import com.treutec.kaypher.execution.expression.tree.StringLiteral;
import com.treutec.kaypher.execution.expression.tree.Type;
import com.treutec.kaypher.metastore.MetaStore;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.parser.DropType;
import com.treutec.kaypher.parser.properties.with.CreateSourceProperties;
import com.treutec.kaypher.parser.tree.CreateStream;
import com.treutec.kaypher.parser.tree.CreateTable;
import com.treutec.kaypher.parser.tree.DdlStatement;
import com.treutec.kaypher.parser.tree.DropStream;
import com.treutec.kaypher.parser.tree.DropTable;
import com.treutec.kaypher.parser.tree.ExecutableDdlStatement;
import com.treutec.kaypher.parser.tree.RegisterType;
import com.treutec.kaypher.parser.tree.TableElement;
import com.treutec.kaypher.parser.tree.TableElement.Namespace;
import com.treutec.kaypher.parser.tree.TableElements;
import com.treutec.kaypher.properties.with.CommonCreateConfigs;
import com.treutec.kaypher.schema.kaypher.SqlBaseType;
import com.treutec.kaypher.schema.kaypher.types.SqlPrimitiveType;
import com.treutec.kaypher.schema.kaypher.types.SqlStruct;
import com.treutec.kaypher.schema.kaypher.types.SqlTypes;
import com.treutec.kaypher.serde.SerdeOption;
import com.treutec.kaypher.services.KafkaTopicClient;
import com.treutec.kaypher.services.ServiceContext;
import com.treutec.kaypher.util.KaypherConfig;
import com.treutec.kaypher.util.KaypherException;
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
public class CommandFactoriesTest {

  private static final SourceName SOME_NAME = SourceName.of("bob");
  private static final SourceName TABLE_NAME = SourceName.of("tablename");
  private static final String sqlExpression = "sqlExpression";
  private static final TableElement ELEMENT1 =
      tableElement(Namespace.VALUE, "bob", new Type(SqlTypes.STRING));
  private static final TableElements SOME_ELEMENTS = TableElements.of(ELEMENT1);
  private static final String TOPIC_NAME = "some topic";
  private static final Map<String, Literal> MINIMIM_PROPS = ImmutableMap.of(
      CommonCreateConfigs.VALUE_FORMAT_PROPERTY, new StringLiteral("JSON"),
      CommonCreateConfigs.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral(TOPIC_NAME)
  );
  private static final String SOME_TYPE_NAME = "newtype";
  private static final Map<String, Object> OVERRIDES = ImmutableMap.of(
      KaypherConfig.KAYPHER_WRAP_SINGLE_VALUES, !defaultConfigValue(KaypherConfig.KAYPHER_WRAP_SINGLE_VALUES)
  );

  @Mock
  private KafkaTopicClient topicClient;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private MetaStore metaStore;
  @Mock
  private CreateSourceFactory createSourceFactory;
  @Mock
  private DropSourceFactory dropSourceFactory;
  @Mock
  private RegisterTypeFactory registerTypeFactory;
  @Mock
  private DropTypeFactory dropTypeFactory;
  @Mock
  private CreateStreamCommand createStreamCommand;
  @Mock
  private CreateTableCommand createTableCommand;
  @Mock
  private DropSourceCommand dropSourceCommand;
  @Mock
  private RegisterTypeCommand registerTypeCommand;
  @Mock
  private DropTypeCommand dropTypeCommand;

  private CommandFactories commandFactories;
  private KaypherConfig kaypherConfig = new KaypherConfig(ImmutableMap.of());
  private final CreateSourceProperties withProperties =
      CreateSourceProperties.from(MINIMIM_PROPS);

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  @SuppressWarnings("unchecked")
  public void before() {
    when(serviceContext.getTopicClient()).thenReturn(topicClient);
    when(topicClient.isTopicExists(any())).thenReturn(true);
    when(createSourceFactory.createStreamCommand(any(), any(), any()))
        .thenReturn(createStreamCommand);
    when(createSourceFactory.createTableCommand(any(), any(), any()))
        .thenReturn(createTableCommand);
    when(dropSourceFactory.create(any(DropStream.class))).thenReturn(dropSourceCommand);
    when(dropSourceFactory.create(any(DropTable.class))).thenReturn(dropSourceCommand);
    when(registerTypeFactory.create(any())).thenReturn(registerTypeCommand);
    when(dropTypeFactory.create(any())).thenReturn(dropTypeCommand);

    givenCommandFactoriesWithMocks();
  }

  private void givenCommandFactories() {
    commandFactories = new CommandFactories(
        serviceContext,
        metaStore
    );
  }

  private void givenCommandFactoriesWithMocks() {
    commandFactories = new CommandFactories(
        createSourceFactory,
        dropSourceFactory,
        registerTypeFactory,
        dropTypeFactory
    );
  }

  @Test
  public void shouldCreateCommandForCreateStream() {
    // Given:
    final CreateStream statement = new CreateStream(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // When:
    final DdlCommand result = commandFactories
        .create(sqlExpression, statement, kaypherConfig, emptyMap());

    assertThat(result, is(createStreamCommand));
    verify(createSourceFactory).createStreamCommand(sqlExpression, statement, kaypherConfig);
  }

  @Test
  public void shouldCreateCommandForStreamWithOverriddenProperties() {
    // Given:
    final CreateStream statement = new CreateStream(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // When:
    commandFactories.create(sqlExpression, statement, kaypherConfig, OVERRIDES);

    verify(createSourceFactory).createStreamCommand(
        sqlExpression,
        statement,
        kaypherConfig.cloneWithPropertyOverwrite(OVERRIDES));
  }

  @Test
  public void shouldCreateCommandForCreateTable() {
    // Given:
    final CreateTable statement = new CreateTable(SOME_NAME,
        TableElements.of(
            tableElement(Namespace.VALUE, "COL1", new Type(SqlTypes.BIGINT)),
            tableElement(Namespace.VALUE, "COL2", new Type(SqlTypes.STRING))),
        true, withProperties);

    // When:
    final DdlCommand result = commandFactories
        .create(sqlExpression, statement, kaypherConfig, emptyMap());

    // Then:
    assertThat(result, is(createTableCommand));
    verify(createSourceFactory).createTableCommand(sqlExpression, statement, kaypherConfig);
  }

  @Test
  public void shouldCreateCommandForCreateTableWithOverriddenProperties() {
    // Given:
    final CreateTable statement = new CreateTable(SOME_NAME,
        TableElements.of(
            tableElement(Namespace.VALUE, "COL1", new Type(SqlTypes.BIGINT)),
            tableElement(Namespace.VALUE, "COL2", new Type(SqlTypes.STRING))),
        true, withProperties);

    // When:
    commandFactories.create(sqlExpression, statement, kaypherConfig, OVERRIDES);

    // Then:
    verify(createSourceFactory).createTableCommand(
        sqlExpression,
        statement,
        kaypherConfig.cloneWithPropertyOverwrite(OVERRIDES)
    );
  }

  @Test
  public void shouldCreateCommandForDropStream() {
    // Given:
    final DropStream ddlStatement = new DropStream(SOME_NAME, true, true);

    // When:
    final DdlCommand result = commandFactories
        .create(sqlExpression, ddlStatement, kaypherConfig, emptyMap());

    // Then:
    assertThat(result, is(dropSourceCommand));
    verify(dropSourceFactory).create(ddlStatement);
  }

  @Test
  public void shouldCreateCommandForDropTable() {
    // Given:
    final DropTable ddlStatement = new DropTable(TABLE_NAME, true, true);

    // When:
    final DdlCommand result = commandFactories
        .create(sqlExpression, ddlStatement, kaypherConfig, emptyMap());

    // Then:
    assertThat(result, is(dropSourceCommand));
    verify(dropSourceFactory).create(ddlStatement);
  }

  @Test
  public void shouldCreateCommandForRegisterType() {
    // Given:
    final RegisterType ddlStatement = new RegisterType(
        Optional.empty(),
        "alias",
        new Type(SqlStruct.builder().field("foo", SqlPrimitiveType.of(SqlBaseType.STRING)).build())
    );

    // When:
    final DdlCommand result = commandFactories.create(
        sqlExpression, ddlStatement, kaypherConfig, emptyMap());

    // Then:
    assertThat(result, is(registerTypeCommand));
    verify(registerTypeFactory).create(ddlStatement);
  }

  @Test
  public void shouldCreateDropType() {
    // Given:
    final DropType dropType = new DropType(Optional.empty(), SOME_TYPE_NAME);

    // When:
    final DropTypeCommand cmd = (DropTypeCommand) commandFactories.create(
        "sqlExpression",
        dropType,
        kaypherConfig,
        emptyMap()
    );

    // Then:
    assertThat(cmd, is(dropTypeCommand));
    verify(dropTypeFactory).create(dropType);
  }

  @Test(expected = KaypherException.class)
  public void shouldThrowOnUnsupportedStatementType() {
    // Given:
    final ExecutableDdlStatement ddlStatement = new ExecutableDdlStatement() {
    };

    // Then:
    commandFactories.create(sqlExpression, ddlStatement, kaypherConfig, emptyMap());
  }

  @Test
  public void shouldCreateStreamCommandWithSingleValueWrappingFromOverridesNotConfig() {
    // Given:
    givenCommandFactories();
    kaypherConfig = new KaypherConfig(ImmutableMap.of(
        KaypherConfig.KAYPHER_WRAP_SINGLE_VALUES, true
    ));

    final ImmutableMap<String, Object> overrides = ImmutableMap.of(
        KaypherConfig.KAYPHER_WRAP_SINGLE_VALUES, false
    );

    final DdlStatement statement =
        new CreateStream(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // When:
    final DdlCommand cmd = commandFactories
        .create(sqlExpression, statement, kaypherConfig, overrides);

    // Then:
    assertThat(cmd, is(instanceOf(CreateStreamCommand.class)));
    assertThat(((CreateStreamCommand) cmd).getSerdeOptions(),
        contains(SerdeOption.UNWRAP_SINGLE_VALUES));
  }

  @Test
  public void shouldCreateTableCommandWithSingleValueWrappingFromOverridesNotConfig() {
    // Given:
    givenCommandFactories();
    kaypherConfig = new KaypherConfig(ImmutableMap.of(
        KaypherConfig.KAYPHER_WRAP_SINGLE_VALUES, true
    ));

    final ImmutableMap<String, Object> overrides = ImmutableMap.of(
        KaypherConfig.KAYPHER_WRAP_SINGLE_VALUES, false
    );

    final DdlStatement statement =
        new CreateTable(SOME_NAME, SOME_ELEMENTS, true, withProperties);

    // When:
    final DdlCommand cmd = commandFactories
        .create(sqlExpression, statement, kaypherConfig, overrides);

    // Then:
    assertThat(cmd, is(instanceOf(CreateTableCommand.class)));
    assertThat(((CreateTableCommand) cmd).getSerdeOptions(),
        contains(SerdeOption.UNWRAP_SINGLE_VALUES));
  }

  private static TableElement tableElement(
      final Namespace namespace,
      final String name,
      final Type type
  ) {
    final TableElement te = mock(TableElement.class, name);
    when(te.getName()).thenReturn(ColumnName.of(name));
    when(te.getType()).thenReturn(type);
    when(te.getNamespace()).thenReturn(namespace);
    return te;
  }

  private static boolean defaultConfigValue(final String config) {
    return new KaypherConfig(emptyMap()).getBoolean(config);
  }
}