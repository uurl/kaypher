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
package com.treutec.kaypher.analyzer;

import com.treutec.kaypher.execution.expression.tree.ArithmeticBinaryExpression;
import com.treutec.kaypher.execution.expression.tree.Cast;
import com.treutec.kaypher.execution.expression.tree.ColumnReferenceExp;
import com.treutec.kaypher.execution.expression.tree.ComparisonExpression;
import com.treutec.kaypher.execution.expression.tree.DereferenceExpression;
import com.treutec.kaypher.execution.expression.tree.Expression;
import com.treutec.kaypher.execution.expression.tree.FunctionCall;
import com.treutec.kaypher.execution.expression.tree.IsNotNullPredicate;
import com.treutec.kaypher.execution.expression.tree.IsNullPredicate;
import com.treutec.kaypher.execution.expression.tree.LikePredicate;
import com.treutec.kaypher.execution.expression.tree.LogicalBinaryExpression;
import com.treutec.kaypher.execution.expression.tree.NotExpression;
import com.treutec.kaypher.execution.expression.tree.VisitParentExpressionVisitor;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.schema.kaypher.ColumnRef;
import com.treutec.kaypher.schema.kaypher.FormatOptions;
import com.treutec.kaypher.util.KaypherException;
import com.treutec.kaypher.util.SchemaUtil;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Searches through the AST for any column references and throws if they are unknown or ambiguous.
 */
class ExpressionAnalyzer {

  private final SourceSchemas sourceSchemas;

  ExpressionAnalyzer(final SourceSchemas sourceSchemas) {
    this.sourceSchemas = Objects.requireNonNull(sourceSchemas, "sourceSchemas");
  }

  void analyzeExpression(final Expression expression, final boolean allowWindowMetaFields) {
    final Visitor visitor = new Visitor(allowWindowMetaFields);
    visitor.process(expression, null);
  }

  private final class Visitor extends VisitParentExpressionVisitor<Object, Object> {

    private final boolean allowWindowMetaFields;

    Visitor(final boolean allowWindowMetaFields) {
      this.allowWindowMetaFields = allowWindowMetaFields;
    }

    public Object visitLikePredicate(final LikePredicate node, final Object context) {
      process(node.getValue(), null);
      return null;
    }

    public Object visitFunctionCall(final FunctionCall node, final Object context) {
      for (final Expression argExpr : node.getArguments()) {
        process(argExpr, null);
      }
      return null;
    }

    public Object visitArithmeticBinary(
        final ArithmeticBinaryExpression node,
        final Object context) {
      process(node.getLeft(), null);
      process(node.getRight(), null);
      return null;
    }

    public Object visitIsNotNullPredicate(final IsNotNullPredicate node, final Object context) {
      return process(node.getValue(), context);
    }

    public Object visitIsNullPredicate(final IsNullPredicate node, final Object context) {
      return process(node.getValue(), context);
    }

    public Object visitLogicalBinaryExpression(
        final LogicalBinaryExpression node,
        final Object context) {
      process(node.getLeft(), null);
      process(node.getRight(), null);
      return null;
    }

    @Override
    public Object visitComparisonExpression(
        final ComparisonExpression node,
        final Object context) {
      process(node.getLeft(), null);
      process(node.getRight(), null);
      return null;
    }

    @Override
    public Object visitNotExpression(final NotExpression node, final Object context) {
      return process(node.getValue(), null);
    }

    @Override
    public Object visitCast(final Cast node, final Object context) {
      process(node.getExpression(), context);
      return null;
    }

    @Override
    public Object visitColumnReference(
        final ColumnReferenceExp node,
        final Object context
    ) {
      throwOnUnknownOrAmbiguousColumn(node.getReference());
      return null;
    }

    @Override
    public Object visitDereferenceExpression(
        final DereferenceExpression node,
        final Object context
    ) {
      process(node.getBase(), context);
      return null;
    }

    private void throwOnUnknownOrAmbiguousColumn(final ColumnRef name) {
      final Set<SourceName> sourcesWithField = sourceSchemas.sourcesWithField(name);

      if (sourcesWithField.isEmpty()) {
        if (allowWindowMetaFields && name.name().equals(SchemaUtil.WINDOWSTART_NAME)) {
          return;
        }

        throw new KaypherException("Column '" + name.toString(FormatOptions.noEscape())
            + "' cannot be resolved.");
      }

      if (sourcesWithField.size() > 1) {
        final String possibilities = sourcesWithField.stream()
            .map(source -> SchemaUtil.buildAliasedFieldName(source.name(), name.name().name()))
            .sorted()
            .collect(Collectors.joining(", "));

        throw new KaypherException("Column '" + name.name().name() + "' is ambiguous. "
            + "Could be any of: " + possibilities);
      }
    }
  }
}
