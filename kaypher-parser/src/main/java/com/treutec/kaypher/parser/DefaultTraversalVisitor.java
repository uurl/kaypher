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
package com.treutec.kaypher.parser;

import com.treutec.kaypher.parser.tree.AliasedRelation;
import com.treutec.kaypher.parser.tree.AstVisitor;
import com.treutec.kaypher.parser.tree.CreateStreamAsSelect;
import com.treutec.kaypher.parser.tree.CreateTableAsSelect;
import com.treutec.kaypher.parser.tree.Explain;
import com.treutec.kaypher.parser.tree.GroupBy;
import com.treutec.kaypher.parser.tree.GroupingElement;
import com.treutec.kaypher.parser.tree.InsertInto;
import com.treutec.kaypher.parser.tree.Join;
import com.treutec.kaypher.parser.tree.Query;
import com.treutec.kaypher.parser.tree.Select;
import com.treutec.kaypher.parser.tree.SelectItem;
import com.treutec.kaypher.parser.tree.SimpleGroupBy;
import com.treutec.kaypher.parser.tree.SingleColumn;
import com.treutec.kaypher.parser.tree.Statements;

public abstract class DefaultTraversalVisitor<R, C> extends AstVisitor<R, C> {

  @Override
  protected R visitStatements(final Statements node, final C context) {
    node.getStatements()
        .forEach(stmt -> process(stmt, context));
    return visitNode(node, context);
  }

  @Override
  protected R visitQuery(final Query node, final C context) {
    process(node.getSelect(), context);
    process(node.getFrom(), context);

    if (node.getGroupBy().isPresent()) {
      process(node.getGroupBy().get(), context);
    }
    return null;
  }

  @Override
  protected R visitSelect(final Select node, final C context) {
    for (final SelectItem item : node.getSelectItems()) {
      process(item, context);
    }

    return null;
  }

  @Override
  protected R visitSingleColumn(final SingleColumn node, final C context) {
    return null;
  }

  @Override
  protected R visitAliasedRelation(final AliasedRelation node, final C context) {
    return process(node.getRelation(), context);
  }

  @Override
  protected R visitJoin(final Join node, final C context) {
    process(node.getLeft(), context);
    process(node.getRight(), context);

    return null;
  }

  @Override
  protected R visitGroupBy(final GroupBy node, final C context) {
    for (final GroupingElement groupingElement : node.getGroupingElements()) {
      process(groupingElement, context);
    }

    return null;
  }

  @Override
  protected R visitGroupingElement(final GroupingElement node, final C context) {
    return null;
  }

  @Override
  protected R visitSimpleGroupBy(final SimpleGroupBy node, final C context) {
    visitGroupingElement(node, context);

    return null;
  }

  @Override
  protected R visitInsertInto(final InsertInto node, final C context) {
    process(node.getQuery(), context);
    return null;
  }

  @Override
  protected R visitCreateStreamAsSelect(final CreateStreamAsSelect node, final C context) {
    process(node.getQuery(), context);
    return null;
  }

  @Override
  protected R visitCreateTableAsSelect(final CreateTableAsSelect node, final C context) {
    process(node.getQuery(), context);
    return null;
  }

  @Override
  protected R visitExplain(final Explain node, final C context) {
    node.getStatement().ifPresent(s -> process(s, context));
    return null;
  }
}
