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

import static com.google.common.collect.Iterables.getOnlyElement;

import com.google.common.base.Strings;
import com.treutec.kaypher.execution.expression.formatter.ExpressionFormatter;
import com.treutec.kaypher.execution.expression.tree.Expression;
import com.treutec.kaypher.name.Name;
import com.treutec.kaypher.parser.tree.AliasedRelation;
import com.treutec.kaypher.parser.tree.AllColumns;
import com.treutec.kaypher.parser.tree.AstNode;
import com.treutec.kaypher.parser.tree.AstVisitor;
import com.treutec.kaypher.parser.tree.CreateAsSelect;
import com.treutec.kaypher.parser.tree.CreateSource;
import com.treutec.kaypher.parser.tree.CreateStream;
import com.treutec.kaypher.parser.tree.CreateStreamAsSelect;
import com.treutec.kaypher.parser.tree.CreateTable;
import com.treutec.kaypher.parser.tree.CreateTableAsSelect;
import com.treutec.kaypher.parser.tree.DropStatement;
import com.treutec.kaypher.parser.tree.DropStream;
import com.treutec.kaypher.parser.tree.DropTable;
import com.treutec.kaypher.parser.tree.Explain;
import com.treutec.kaypher.parser.tree.InsertInto;
import com.treutec.kaypher.parser.tree.InsertValues;
import com.treutec.kaypher.parser.tree.Join;
import com.treutec.kaypher.parser.tree.JoinCriteria;
import com.treutec.kaypher.parser.tree.JoinOn;
import com.treutec.kaypher.parser.tree.ListFunctions;
import com.treutec.kaypher.parser.tree.ListStreams;
import com.treutec.kaypher.parser.tree.ListTables;
import com.treutec.kaypher.parser.tree.Query;
import com.treutec.kaypher.parser.tree.RegisterType;
import com.treutec.kaypher.parser.tree.Relation;
import com.treutec.kaypher.parser.tree.Select;
import com.treutec.kaypher.parser.tree.SelectItem;
import com.treutec.kaypher.parser.tree.SetProperty;
import com.treutec.kaypher.parser.tree.ShowColumns;
import com.treutec.kaypher.parser.tree.SingleColumn;
import com.treutec.kaypher.parser.tree.Table;
import com.treutec.kaypher.parser.tree.TableElement;
import com.treutec.kaypher.parser.tree.TableElement.Namespace;
import com.treutec.kaypher.parser.tree.TerminateQuery;
import com.treutec.kaypher.parser.tree.UnsetProperty;
import com.treutec.kaypher.schema.kaypher.FormatOptions;
import com.treutec.kaypher.util.IdentifierUtil;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

public final class SqlFormatter {

  private static final String INDENT = "   ";
  private static final Pattern NAME_PATTERN = Pattern.compile("[a-z_][a-z0-9_]*");
  private static final FormatOptions FORMAT_OPTIONS = FormatOptions.of(IdentifierUtil::needsQuotes);

  private SqlFormatter() {
  }

  public static String formatSql(final AstNode root) {
    return formatSql(root, true);
  }

  private static String formatSql(final AstNode root, final boolean unmangleNames) {
    final StringBuilder builder = new StringBuilder();
    new Formatter(builder, unmangleNames).process(root, 0);
    return StringUtils.stripEnd(builder.toString(), "\n");
  }

  private static final class Formatter extends AstVisitor<Void, Integer> {

    private final StringBuilder builder;
    private final boolean unmangledNames;

    private Formatter(final StringBuilder builder, final boolean unmangleNames) {
      this.builder = builder;
      this.unmangledNames = unmangleNames;
    }

    @Override
    protected Void visitNode(final AstNode node, final Integer indent) {
      throw new UnsupportedOperationException("not yet implemented: " + node);
    }

    @Override
    protected Void visitQuery(final Query node, final Integer indent) {

      process(node.getSelect(), indent);

      append(indent, "FROM ");
      processRelation(node.getFrom(), indent);
      builder.append('\n');

      if (node.getWindow().isPresent()) {
        append(indent, "WINDOW" + node.getWindow().get().getKaypherWindowExpression().toString())
            .append('\n');
      }

      if (node.getWhere().isPresent()) {
        append(
            indent,
            "WHERE " + ExpressionFormatterUtil.formatExpression(node.getWhere().get())
        ).append('\n');
      }

      if (node.getGroupBy().isPresent()) {
        append(indent, "GROUP BY "
            + ExpressionFormatterUtil
            .formatGroupBy(node.getGroupBy().get().getGroupingElements()))
            .append('\n');
      }

      if (node.getHaving().isPresent()) {
        append(indent, "HAVING "
            + ExpressionFormatterUtil.formatExpression(node.getHaving().get()))
            .append('\n');
      }

      if (!node.isStatic()) {
        append(indent, "EMIT ");
        append(indent, node.getResultMaterialization().toString())
            .append('\n');
      }

      if (node.getLimit().isPresent()) {
        append(indent, "LIMIT " + node.getLimit().getAsInt())
                .append('\n');
      }

      return null;
    }

    @Override
    protected Void visitSelect(final Select node, final Integer indent) {
      append(indent, "SELECT");

      final List<SelectItem> selectItems = node.getSelectItems();

      if (selectItems.size() > 1) {
        boolean first = true;
        for (final SelectItem item : selectItems) {
          builder.append(first ? "" : ",")
                 .append("\n  ")
                 .append(indentString(indent));

          process(item, indent);
          first = false;
        }
      } else {
        builder.append(' ');
        process(getOnlyElement(selectItems), indent);
      }

      builder.append('\n');

      return null;
    }

    @Override
    protected Void visitSingleColumn(final SingleColumn node, final Integer indent) {
      builder.append(ExpressionFormatterUtil.formatExpression(node.getExpression()));
      if (node.getAlias().isPresent()) {
        builder.append(' ')
            // for backwards compatibility, we always quote with `""` here
            .append(node.getAlias().get().toString(FormatOptions.of(IdentifierUtil::needsQuotes)));
      }
      return null;
    }

    @Override
    protected Void visitAllColumns(final AllColumns node, final Integer context) {
      node.getSource()
          .ifPresent(source ->
              builder.append(escapedName(source))
                  .append("."));

      builder.append("*");

      return null;
    }

    @Override
    protected Void visitTable(final Table node, final Integer indent) {
      builder.append(escapedName(node.getName()));
      return null;
    }

    @Override
    protected Void visitJoin(final Join node, final Integer indent) {
      final String type = node.getType().getFormatted();
      process(node.getLeft(), indent);

      builder.append('\n');
      append(indent, type).append(" JOIN ");

      process(node.getRight(), indent);

      final JoinCriteria criteria = node.getCriteria();

      node.getWithinExpression().map((e) -> builder.append(e.toString()));
      final JoinOn on = (JoinOn) criteria;
      builder.append(" ON (")
          .append(ExpressionFormatterUtil.formatExpression(on.getExpression()))
          .append(")");

      return null;
    }

    @Override
    protected Void visitAliasedRelation(final AliasedRelation node, final Integer indent) {
      process(node.getRelation(), indent);

      builder.append(' ')
              .append(escapedName(node.getAlias()));

      return null;
    }

    @Override
    protected Void visitCreateStream(final CreateStream node, final Integer indent) {
      builder.append("CREATE STREAM ");
      formatCreate(node);
      return null;
    }

    @Override
    protected Void visitCreateTable(final CreateTable node, final Integer indent) {
      builder.append("CREATE TABLE ");
      formatCreate(node);
      return null;
    }

    @Override
    protected Void visitExplain(final Explain node, final Integer indent) {
      builder.append("EXPLAIN ");
      builder.append("\n");

      node.getQueryId().ifPresent(queryId -> append(indent, queryId));

      node.getStatement().ifPresent(stmt -> process(stmt, indent));

      return null;
    }

    @Override
    protected Void visitShowColumns(final ShowColumns node, final Integer context) {
      builder.append("DESCRIBE ")
              .append(escapedName(node.getTable()));
      return null;
    }

    @Override
    protected Void visitShowFunctions(final ListFunctions node, final Integer context) {
      builder.append("SHOW FUNCTIONS");

      return null;
    }

    @Override
    protected Void visitCreateStreamAsSelect(
        final CreateStreamAsSelect node,
        final Integer indent
    ) {
      builder.append("CREATE STREAM ");
      formatCreateAs(node, indent);
      return null;
    }

    @Override
    protected Void visitCreateTableAsSelect(
        final CreateTableAsSelect node,
        final Integer indent
    ) {
      builder.append("CREATE TABLE ");
      formatCreateAs(node, indent);
      return null;
    }

    private static String formatName(final String name) {
      if (NAME_PATTERN.matcher(name).matches()) {
        return name;
      }
      return "\"" + name + "\"";
    }

    @Override
    protected Void visitInsertInto(final InsertInto node, final Integer indent) {
      builder.append("INSERT INTO ");
      builder.append(escapedName(node.getTarget()));
      builder.append(" ");
      process(node.getQuery(), indent);
      processPartitionBy(node.getPartitionByColumn(), indent);
      return null;
    }

    @Override
    protected Void visitDropStream(final DropStream node, final Integer context) {
      visitDrop(node, "STREAM");
      return null;
    }

    @Override
    protected Void visitInsertValues(final InsertValues node, final Integer context) {
      builder.append("INSERT INTO ");
      builder.append(escapedName(node.getTarget()));
      builder.append(" ");

      if (!node.getColumns().isEmpty()) {
        builder.append(node.getColumns()
            .stream()
            .map(SqlFormatter::escapedName)
            .collect(Collectors.joining(", ", "(", ") ")));
      }

      builder.append("VALUES ");

      builder.append("(");
      builder.append(
          node.getValues()
              .stream()
              .map(exp -> ExpressionFormatterUtil.formatExpression(exp, unmangledNames))
              .collect(Collectors.joining(", ")));
      builder.append(")");

      return null;
    }

    @Override
    protected Void visitDropTable(final DropTable node, final Integer context) {
      visitDrop(node, "TABLE");
      return null;
    }

    @Override
    protected Void visitTerminateQuery(final TerminateQuery node, final Integer context) {
      builder.append("TERMINATE ");
      builder.append(node.getQueryId().getId());
      return null;
    }

    @Override
    protected Void visitListStreams(final ListStreams node, final Integer context) {
      builder.append("SHOW STREAMS");
      if (node.getShowExtended()) {
        visitExtended();
      }
      return null;
    }

    @Override
    protected Void visitListTables(final ListTables node, final Integer context) {
      builder.append("SHOW TABLES");
      if (node.getShowExtended()) {
        visitExtended();
      }
      return null;
    }

    @Override
    protected Void visitUnsetProperty(final UnsetProperty node, final Integer context) {
      builder.append("UNSET '");
      builder.append(node.getPropertyName());
      builder.append("'");

      return null;
    }

    @Override
    protected Void visitSetProperty(final SetProperty node, final Integer context) {
      builder.append("SET '");
      builder.append(node.getPropertyName());
      builder.append("'='");
      builder.append(node.getPropertyValue());
      builder.append("'");

      return null;
    }

    private void visitExtended() {
      builder.append(" EXTENDED");
    }

    @Override
    public Void visitRegisterType(final RegisterType node, final Integer context) {
      builder.append("CREATE TYPE ");
      builder.append(FORMAT_OPTIONS.escape(node.getName()));
      builder.append(" AS ");
      builder.append(ExpressionFormatterUtil.formatExpression(node.getType()));
      builder.append(";");
      return null;
    }

    private void visitDrop(final DropStatement node, final String sourceType) {
      builder.append("DROP ");
      builder.append(sourceType);
      builder.append(" ");
      if (node.getIfExists()) {
        builder.append("IF EXISTS ");
      }
      builder.append(escapedName(node.getName()));
      if (node.isDeleteTopic()) {
        builder.append(" DELETE TOPIC");
      }
    }

    private void processRelation(final Relation relation, final Integer indent) {
      if (relation instanceof Table) {
        builder.append("TABLE ")
                .append(escapedName(((Table) relation).getName()))
                .append('\n');
      } else {
        process(relation, indent);
      }
    }

    private void processPartitionBy(
        final Optional<Expression> partitionByColumn,
        final Integer indent
    ) {
      partitionByColumn.ifPresent(partitionBy -> append(indent, "PARTITION BY "
              + ExpressionFormatterUtil.formatExpression(partitionBy))
          .append('\n'));
    }

    private StringBuilder append(final int indent, final String value) {
      return builder.append(indentString(indent))
              .append(value);
    }

    private static String indentString(final int indent) {
      return Strings.repeat(INDENT, indent);
    }

    private void formatCreate(final CreateSource node) {
      if (node.isNotExists()) {
        builder.append("IF NOT EXISTS ");
      }

      builder.append(escapedName(node.getName()));

      final String elements = node.getElements().stream()
          .map(Formatter::formatTableElement)
          .collect(Collectors.joining(", "));

      if (!elements.isEmpty()) {
        builder
            .append(" (")
            .append(elements)
            .append(")");
      }

      final String tableProps = node.getProperties().toString();
      if (!tableProps.isEmpty()) {
        builder
            .append(" WITH (")
            .append(tableProps)
            .append(")");
      }

      builder.append(";");
    }

    private void formatCreateAs(final CreateAsSelect node, final Integer indent) {
      if (node.isNotExists()) {
        builder.append("IF NOT EXISTS ");
      }

      builder.append(escapedName(node.getName()));

      final String tableProps = node.getProperties().toString();
      if (!tableProps.isEmpty()) {
        builder
            .append(" WITH (")
            .append(tableProps)
            .append(")");
      }

      builder.append(" AS ");

      process(node.getQuery(), indent);
      processPartitionBy(node.getPartitionByColumn(), indent);
    }

    private static String formatTableElement(final TableElement e) {
      return escapedName(e.getName())
          + " "
          + ExpressionFormatter.formatExpression(
              e.getType(), true, FormatOptions.of(IdentifierUtil::needsQuotes))
          + (e.getNamespace() == Namespace.KEY ? " KEY" : "");
    }
  }

  private static String escapedName(final Name name) {
    return name.toString(FORMAT_OPTIONS);
  }
}
