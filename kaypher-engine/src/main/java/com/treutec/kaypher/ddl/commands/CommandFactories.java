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

import com.google.common.annotations.VisibleForTesting;
import com.treutec.kaypher.execution.ddl.commands.CreateStreamCommand;
import com.treutec.kaypher.execution.ddl.commands.CreateTableCommand;
import com.treutec.kaypher.execution.ddl.commands.DdlCommand;
import com.treutec.kaypher.execution.ddl.commands.DropSourceCommand;
import com.treutec.kaypher.execution.ddl.commands.DropTypeCommand;
import com.treutec.kaypher.execution.ddl.commands.RegisterTypeCommand;
import com.treutec.kaypher.metastore.MetaStore;
import com.treutec.kaypher.parser.DropType;
import com.treutec.kaypher.parser.tree.CreateStream;
import com.treutec.kaypher.parser.tree.CreateTable;
import com.treutec.kaypher.parser.tree.DdlStatement;
import com.treutec.kaypher.parser.tree.DropStream;
import com.treutec.kaypher.parser.tree.DropTable;
import com.treutec.kaypher.parser.tree.RegisterType;
import com.treutec.kaypher.services.ServiceContext;
import com.treutec.kaypher.util.HandlerMaps;
import com.treutec.kaypher.util.HandlerMaps.ClassHandlerMapR2;
import com.treutec.kaypher.util.KaypherConfig;
import com.treutec.kaypher.util.KaypherException;
import java.util.Map;
import java.util.Objects;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class CommandFactories implements DdlCommandFactory {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final ClassHandlerMapR2<DdlStatement, CommandFactories, CallInfo, DdlCommand>
      FACTORIES = HandlerMaps
      .forClass(DdlStatement.class)
      .withArgTypes(CommandFactories.class, CallInfo.class)
      .withReturnType(DdlCommand.class)
      .put(CreateStream.class, CommandFactories::handleCreateStream)
      .put(CreateTable.class, CommandFactories::handleCreateTable)
      .put(DropStream.class, CommandFactories::handleDropStream)
      .put(DropTable.class, CommandFactories::handleDropTable)
      .put(RegisterType.class, CommandFactories::handleRegisterType)
      .put(DropType.class, CommandFactories::handleDropType)
      .build();

  private final CreateSourceFactory createSourceFactory;
  private final DropSourceFactory dropSourceFactory;
  private final RegisterTypeFactory registerTypeFactory;
  private final DropTypeFactory dropTypeFactory;

  public CommandFactories(final ServiceContext serviceContext, final MetaStore metaStore) {
    this(
        new CreateSourceFactory(serviceContext),
        new DropSourceFactory(metaStore),
        new RegisterTypeFactory(),
        new DropTypeFactory()
    );
  }

  @VisibleForTesting
  CommandFactories(
      final CreateSourceFactory createSourceFactory,
      final DropSourceFactory dropSourceFactory,
      final RegisterTypeFactory registerTypeFactory,
      final DropTypeFactory dropTypeFactory
  ) {
    this.createSourceFactory =
        Objects.requireNonNull(createSourceFactory, "createSourceFactory");
    this.dropSourceFactory = Objects.requireNonNull(dropSourceFactory, "dropSourceFactory");
    this.registerTypeFactory =
        Objects.requireNonNull(registerTypeFactory, "registerTypeFactory");
    this.dropTypeFactory = Objects.requireNonNull(dropTypeFactory, "dropTypeFactory");
  }

  @Override
  public DdlCommand create(
      final String sqlExpression,
      final DdlStatement ddlStatement,
      final KaypherConfig kaypherConfig,
      final Map<String, Object> properties
  ) {
    return FACTORIES
        .getOrDefault(ddlStatement.getClass(), (statement, cf, ci) -> {
          throw new KaypherException(
              "Unable to find ddl command factory for statement:"
                  + statement.getClass()
                  + " valid statements:"
                  + FACTORIES.keySet()
          );
        })
        .handle(
            this,
            new CallInfo(sqlExpression, kaypherConfig, properties),
            ddlStatement);
  }

  private CreateStreamCommand handleCreateStream(
      final CallInfo callInfo,
      final CreateStream statement
  ) {
    return createSourceFactory.createStreamCommand(
        callInfo.sqlExpression,
        statement,
        callInfo.kaypherConfig
    );
  }

  private CreateTableCommand handleCreateTable(
      final CallInfo callInfo,
      final CreateTable statement
  ) {
    return createSourceFactory.createTableCommand(
        callInfo.sqlExpression,
        statement,
        callInfo.kaypherConfig
    );
  }

  private DropSourceCommand handleDropStream(final DropStream statement) {
    return dropSourceFactory.create(statement);
  }

  private DropSourceCommand handleDropTable(final DropTable statement) {
    return dropSourceFactory.create(statement);
  }

  @SuppressWarnings("MethodMayBeStatic")
  private RegisterTypeCommand handleRegisterType(final RegisterType statement) {
    return registerTypeFactory.create(statement);
  }

  @SuppressWarnings("MethodMayBeStatic")
  private DropTypeCommand handleDropType(final DropType statement) {
    return dropTypeFactory.create(statement);
  }

  private static final class CallInfo {

    final String sqlExpression;
    final KaypherConfig kaypherConfig;
    final Map<String, Object> properties;

    private CallInfo(
        final String sqlExpression,
        final KaypherConfig kaypherConfig,
        final Map<String, Object> properties
    ) {
      this.sqlExpression = Objects.requireNonNull(sqlExpression, "sqlExpression");
      this.properties = Objects.requireNonNull(properties, "properties");
      this.kaypherConfig = Objects.requireNonNull(kaypherConfig, "kaypherConfig")
          .cloneWithPropertyOverwrite(properties);
    }
  }
}
