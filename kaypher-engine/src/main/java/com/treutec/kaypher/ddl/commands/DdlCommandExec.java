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

import com.treutec.kaypher.execution.ddl.commands.CreateStreamCommand;
import com.treutec.kaypher.execution.ddl.commands.CreateTableCommand;
import com.treutec.kaypher.execution.ddl.commands.DdlCommand;
import com.treutec.kaypher.execution.ddl.commands.DdlCommandResult;
import com.treutec.kaypher.execution.ddl.commands.DropSourceCommand;
import com.treutec.kaypher.execution.ddl.commands.DropTypeCommand;
import com.treutec.kaypher.execution.ddl.commands.RegisterTypeCommand;
import com.treutec.kaypher.metastore.MutableMetaStore;
import com.treutec.kaypher.metastore.model.DataSource;
import com.treutec.kaypher.metastore.model.KeyField;
import com.treutec.kaypher.metastore.model.KaypherStream;
import com.treutec.kaypher.metastore.model.KaypherTable;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.schema.kaypher.ColumnRef;
import com.treutec.kaypher.schema.kaypher.types.SqlType;
import java.util.Objects;
import java.util.Optional;

/**
 * Execute DDL Commands
 */
public class DdlCommandExec {

  private final MutableMetaStore metaStore;

  public DdlCommandExec(final MutableMetaStore metaStore) {
    this.metaStore = metaStore;
  }

  /**
   * execute on metaStore
   */
  public DdlCommandResult execute(final DdlCommand ddlCommand) {
    return execute(ddlCommand, Optional.empty());
  }

  public DdlCommandResult execute(
      final DdlCommand ddlCommand,
      final Optional<KeyField> keyFieldOverride) {
    return new Executor(keyFieldOverride).execute(ddlCommand);
  }

  private final class Executor implements com.treutec.kaypher.execution.ddl.commands.Executor {
    final Optional<KeyField> keyFieldOverride;

    private Executor(final Optional<KeyField> keyFieldOverride) {
      this.keyFieldOverride = Objects.requireNonNull(keyFieldOverride, "keyFieldOverride");
    }

    @Override
    public DdlCommandResult executeCreateStream(final CreateStreamCommand createStream) {
      final KaypherStream<?> kaypherStream = new KaypherStream<>(
          createStream.getSqlExpression(),
          createStream.getSourceName(),
          createStream.getSchema(),
          createStream.getSerdeOptions(),
          getKeyField(createStream.getKeyField()),
          createStream.getTimestampExtractionPolicy(),
          createStream.getTopic()
      );
      metaStore.putSource(kaypherStream);
      return new DdlCommandResult(true, "Stream created");
    }

    @Override
    public DdlCommandResult executeCreateTable(final CreateTableCommand createTable) {
      final KaypherTable<?> kaypherTable = new KaypherTable<>(
          createTable.getSqlExpression(),
          createTable.getSourceName(),
          createTable.getSchema(),
          createTable.getSerdeOptions(),
          getKeyField(createTable.getKeyField()),
          createTable.getTimestampExtractionPolicy(),
          createTable.getTopic()
      );
      metaStore.putSource(kaypherTable);
      return new DdlCommandResult(true, "Table created");
    }

    @Override
    public DdlCommandResult executeDropSource(final DropSourceCommand dropSource) {
      final SourceName sourceName = dropSource.getSourceName();
      final DataSource<?> dataSource = metaStore.getSource(sourceName);
      if (dataSource == null) {
        return new DdlCommandResult(true, "Source " + sourceName + " does not exist.");
      }
      metaStore.deleteSource(sourceName);
      return new DdlCommandResult(true,
          "Source " + sourceName + " (topic: " + dataSource.getKafkaTopicName() + ") was dropped.");
    }

    @Override
    public DdlCommandResult executeRegisterType(final RegisterTypeCommand registerType) {
      final String name = registerType.getName();
      final SqlType type = registerType.getType();
      metaStore.registerType(name, type);
      return new DdlCommandResult(
          true,
          "Registered custom type with name '" + name + "' and SQL type " + type
      );
    }

    @Override
    public DdlCommandResult executeDropType(final DropTypeCommand dropType) {
      final String typeName = dropType.getTypeName();
      final boolean wasDeleted = metaStore.deleteType(typeName);
      return wasDeleted
          ? new DdlCommandResult(true, "Dropped type '" + typeName + "'")
          : new DdlCommandResult(true, "Type '" + typeName + "' does not exist");
    }

    private KeyField getKeyField(final Optional<ColumnName> keyFieldName) {
      return keyFieldOverride
          .orElseGet(() -> keyFieldName
              .map(columnName -> KeyField.of(ColumnRef.withoutSource(columnName)))
              .orElseGet(KeyField::none));
    }
  }
}
