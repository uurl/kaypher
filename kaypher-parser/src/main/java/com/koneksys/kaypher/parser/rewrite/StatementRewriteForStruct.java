/*
 * Copyright 2019 Koneksys
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

package com.koneksys.kaypher.parser.rewrite;

import com.google.common.collect.ImmutableList;
import com.koneksys.kaypher.parser.tree.DereferenceExpression;
import com.koneksys.kaypher.parser.tree.Expression;
import com.koneksys.kaypher.parser.tree.FunctionCall;
import com.koneksys.kaypher.parser.tree.Node;
import com.koneksys.kaypher.parser.tree.QualifiedName;
import com.koneksys.kaypher.parser.tree.QualifiedNameReference;
import com.koneksys.kaypher.parser.tree.Query;
import com.koneksys.kaypher.parser.tree.QueryContainer;
import com.koneksys.kaypher.parser.tree.Statement;
import com.koneksys.kaypher.parser.tree.StringLiteral;
import com.koneksys.kaypher.util.DataSourceExtractor;
import java.util.Objects;

public class StatementRewriteForStruct {

  private final Statement statement;

  public StatementRewriteForStruct(
      final Statement statement,
      final DataSourceExtractor dataSourceExtractor
  ) {
    this.statement = Objects.requireNonNull(statement, "statement");
  }

  public Statement rewriteForStruct() {
    return (Statement) new RewriteWithStructFieldExtractors().process(statement, null);
  }

  public static boolean requiresRewrite(final Statement statement) {
    return statement instanceof Query
        || statement instanceof QueryContainer;
  }


  private static class RewriteWithStructFieldExtractors extends StatementRewriter {

    @Override
    protected Node visitDereferenceExpression(
        final DereferenceExpression node,
        final Object context
    ) {
      return createFetchFunctionNodeIfNeeded(node, context);
    }

    private Expression createFetchFunctionNodeIfNeeded(
        final DereferenceExpression dereferenceExpression,
        final Object context
    ) {
      if (dereferenceExpression.getBase() instanceof QualifiedNameReference) {
        return getNewDereferenceExpression(dereferenceExpression, context);
      }
      return getNewFunctionCall(dereferenceExpression, context);
    }

    private FunctionCall getNewFunctionCall(
        final DereferenceExpression dereferenceExpression,
        final Object context
    ) {
      final Expression createFunctionResult
          = (Expression) process(dereferenceExpression.getBase(), context);
      final String fieldName = dereferenceExpression.getFieldName();
      return new FunctionCall(
          QualifiedName.of("FETCH_FIELD_FROM_STRUCT"),
          ImmutableList.of(createFunctionResult, new StringLiteral(fieldName)));
    }

    private DereferenceExpression getNewDereferenceExpression(
        final DereferenceExpression dereferenceExpression,
        final Object context
    ) {
      return new DereferenceExpression(
          dereferenceExpression.getLocation(),
          (Expression) process(dereferenceExpression.getBase(), context),
          dereferenceExpression.getFieldName());
    }
  }

}