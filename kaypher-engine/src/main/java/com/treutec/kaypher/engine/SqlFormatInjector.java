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

package com.treutec.kaypher.engine;

import com.treutec.kaypher.KaypherExecutionContext;
import com.treutec.kaypher.parser.KaypherParser.PreparedStatement;
import com.treutec.kaypher.parser.SqlFormatter;
import com.treutec.kaypher.parser.tree.Statement;
import com.treutec.kaypher.statement.ConfiguredStatement;
import com.treutec.kaypher.statement.Injector;
import com.treutec.kaypher.util.KaypherException;
import java.util.Objects;

public class SqlFormatInjector implements Injector {

  private final KaypherExecutionContext executionContext;

  public SqlFormatInjector(final KaypherExecutionContext executionContext) {
    this.executionContext = Objects.requireNonNull(executionContext, "executionContext");
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends Statement> ConfiguredStatement<T> inject(
      final ConfiguredStatement<T> statement
  ) {
    try {
      final Statement node = statement.getStatement();
      final String sql = SqlFormatter.formatSql(node);
      final String sqlWithSemiColon = sql.endsWith(";") ? sql : sql + ";";
      final PreparedStatement<?> prepare = executionContext
          .prepare(executionContext.parse(sqlWithSemiColon).get(0));

      return statement.withStatement(sql, (T) prepare.getStatement());
    } catch (final Exception e) {
      throw new KaypherException("Unable to format statement! This is bad because "
          + "it means we cannot persist it onto the command topic: " + statement, e);
    }
  }
}