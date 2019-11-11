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
import com.treutec.kaypher.execution.ddl.commands.DropSourceCommand;
import com.treutec.kaypher.metastore.MetaStore;
import com.treutec.kaypher.metastore.model.DataSource;
import com.treutec.kaypher.metastore.model.DataSource.DataSourceType;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.parser.tree.DropStream;
import com.treutec.kaypher.parser.tree.DropTable;
import com.treutec.kaypher.util.KaypherException;
import java.util.Objects;

public final class DropSourceFactory {
  private final MetaStore metaStore;

  @VisibleForTesting
  DropSourceFactory(final MetaStore metaStore) {
    this.metaStore = Objects.requireNonNull(metaStore, "metaStore");
  }

  public DropSourceCommand create(final DropStream statement) {
    return create(
        statement.getName(),
        statement.getIfExists(),
        DataSourceType.KSTREAM
    );
  }

  public DropSourceCommand create(final DropTable statement) {
    return create(
        statement.getName(),
        statement.getIfExists(),
        DataSourceType.KTABLE
    );
  }

  private DropSourceCommand create(
      final SourceName sourceName,
      final boolean ifExists,
      final DataSourceType dataSourceType) {
    final DataSource<?> dataSource = metaStore.getSource(sourceName);
    if (dataSource == null) {
      if (ifExists) {
        throw new KaypherException("Source " + sourceName.name() + " does not exist.");
      }
    } else if (dataSource.getDataSourceType() != dataSourceType) {
      throw new KaypherException(String.format(
          "Incompatible data source type is %s, but statement was DROP %s",
          dataSource.getDataSourceType() == DataSourceType.KSTREAM ? "STREAM" : "TABLE",
          dataSourceType == DataSourceType.KSTREAM ? "STREAM" : "TABLE"
      ));
    }
    return new DropSourceCommand(sourceName);
  }
}
