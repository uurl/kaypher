package com.treutec.kaypher.testutils;

import com.treutec.kaypher.metastore.MetaStore;
import com.treutec.kaypher.parser.DefaultKaypherParser;
import com.treutec.kaypher.parser.KaypherParser;
import com.treutec.kaypher.parser.KaypherParser.ParsedStatement;
import com.treutec.kaypher.parser.KaypherParser.PreparedStatement;
import com.treutec.kaypher.execution.expression.tree.Expression;
import com.treutec.kaypher.parser.tree.Query;
import com.treutec.kaypher.parser.tree.SingleColumn;

public final class ExpressionParseTestUtil {
  public static Expression parseExpression(final String asText, final MetaStore metaStore) {
    final KaypherParser parser = new DefaultKaypherParser();
    final String kaypher = String.format("SELECT %s FROM test1;", asText);

    final ParsedStatement parsedStatement = parser.parse(kaypher).get(0);
    final PreparedStatement preparedStatement = parser.prepare(parsedStatement, metaStore);
    final SingleColumn singleColumn = (SingleColumn) ((Query)preparedStatement.getStatement())
        .getSelect()
        .getSelectItems()
        .get(0);
    return singleColumn.getExpression();
  }
}
