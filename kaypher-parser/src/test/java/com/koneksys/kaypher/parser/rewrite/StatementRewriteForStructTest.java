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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

import com.koneksys.kaypher.function.TestFunctionRegistry;
import com.koneksys.kaypher.metastore.MetaStore;
import com.koneksys.kaypher.parser.KaypherParserTestUtil;
import com.koneksys.kaypher.parser.tree.CreateStreamAsSelect;
import com.koneksys.kaypher.parser.tree.CreateTable;
import com.koneksys.kaypher.parser.tree.CreateTableAsSelect;
import com.koneksys.kaypher.parser.tree.DereferenceExpression;
import com.koneksys.kaypher.parser.tree.Expression;
import com.koneksys.kaypher.parser.tree.FunctionCall;
import com.koneksys.kaypher.parser.tree.InsertInto;
import com.koneksys.kaypher.parser.tree.Query;
import com.koneksys.kaypher.parser.tree.SingleColumn;
import com.koneksys.kaypher.parser.tree.Statement;
import com.koneksys.kaypher.util.MetaStoreFixture;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

public class StatementRewriteForStructTest {

  private MetaStore metaStore;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore(new TestFunctionRegistry());
  }

  @Test
  public void shouldCreateCorrectFunctionCallExpression() {
    final String simpleQuery = "SELECT iteminfo->category->name, address->state FROM orders;";
    final Statement statement = KaypherParserTestUtil.buildSingleAst(simpleQuery, metaStore)
        .getStatement();

    final Query query = getQuery(statement);
    assertThat(query.getSelect().getSelectItems().size(), equalTo(2));
    final Expression col0 = ((SingleColumn) query.getSelect().getSelectItems().get(0))
        .getExpression();
    final Expression col1 = ((SingleColumn) query.getSelect().getSelectItems().get(1))
        .getExpression();

    assertThat(col0, instanceOf(FunctionCall.class));
    assertThat(col1, instanceOf(FunctionCall.class));

    assertThat(col0.toString(), equalTo(
        "FETCH_FIELD_FROM_STRUCT(FETCH_FIELD_FROM_STRUCT(ORDERS.ITEMINFO, 'CATEGORY'), 'NAME')"));
    assertThat(col1.toString(), equalTo("FETCH_FIELD_FROM_STRUCT(ORDERS.ADDRESS, 'STATE')"));

  }

  @Test
  public void shouldNotCreateFunctionCallIfNotNeeded() {
    final String simpleQuery = "SELECT orderid FROM orders;";
    final Statement statement = KaypherParserTestUtil.buildSingleAst(simpleQuery, metaStore)
        .getStatement();

    final Query query = getQuery(statement);
    assertThat(query.getSelect().getSelectItems().size(), equalTo(1));
    final Expression col0 = ((SingleColumn) query.getSelect().getSelectItems().get(0))
        .getExpression();

    assertThat(col0, instanceOf(DereferenceExpression.class));
    assertThat(col0.toString(), equalTo("ORDERS.ORDERID"));
  }


  @Test
  public void shouldCreateCorrectFunctionCallExpressionWithSubscript() {
    final String simpleQuery = "SELECT arraycol[0]->name as n0, mapcol['key']->name as n1 FROM nested_stream;";
    final Statement statement = KaypherParserTestUtil.buildSingleAst(simpleQuery, metaStore)
        .getStatement();

    final Query query = getQuery(statement);
    assertThat(query.getSelect().getSelectItems().size(), equalTo(2));
    final Expression col0 = ((SingleColumn) query.getSelect().getSelectItems().get(0))
        .getExpression();
    final Expression col1 = ((SingleColumn) query.getSelect().getSelectItems().get(1))
        .getExpression();

    assertThat(col0, instanceOf(FunctionCall.class));
    assertThat(col1, instanceOf(FunctionCall.class));

    assertThat(col0.toString(),
        equalTo("FETCH_FIELD_FROM_STRUCT(NESTED_STREAM.ARRAYCOL[0], 'NAME')"));
    assertThat(col1.toString(),
        equalTo("FETCH_FIELD_FROM_STRUCT(NESTED_STREAM.MAPCOL['key'], 'NAME')"));
  }

  @Test
  public void shouldCreateCorrectFunctionCallExpressionWithSubscriptWithExpressionIndex() {
    final String simpleQuery = "SELECT arraycol[CAST (item->id AS INTEGER)]->name as n0, mapcol['key']->name as n1 FROM nested_stream;";
    final Statement statement = KaypherParserTestUtil.buildSingleAst(simpleQuery, metaStore)
        .getStatement();

    final Query query = getQuery(statement);
    assertThat(query.getSelect().getSelectItems().size(), equalTo(2));
    final Expression col0 = ((SingleColumn) query.getSelect().getSelectItems().get(0))
        .getExpression();
    final Expression col1 = ((SingleColumn) query.getSelect().getSelectItems().get(1))
        .getExpression();

    assertThat(col0, instanceOf(FunctionCall.class));
    assertThat(col1, instanceOf(FunctionCall.class));

    assertThat(col0.toString(), equalTo(
        "FETCH_FIELD_FROM_STRUCT(NESTED_STREAM.ARRAYCOL[CAST(FETCH_FIELD_FROM_STRUCT(NESTED_STREAM.ITEM, 'ID') AS INTEGER)], 'NAME')"));
    assertThat(col1.toString(),
        equalTo("FETCH_FIELD_FROM_STRUCT(NESTED_STREAM.MAPCOL['key'], 'NAME')"));
  }

  @Test
  public void shouldEnsureRewriteRequirementCorrectly() {
    assertThat("Query should be valid for rewrite for struct.", StatementRewriteForStruct.requiresRewrite(EasyMock.mock(Query.class)));
    assertThat("CSAS should be valid for rewrite for struct.", StatementRewriteForStruct.requiresRewrite(EasyMock.mock(CreateStreamAsSelect.class)));
    assertThat("CTAS should be valid for rewrite for struct.", StatementRewriteForStruct.requiresRewrite(EasyMock.mock(CreateTableAsSelect.class)));
    assertThat("Insert Into should be valid for rewrite for struct.", StatementRewriteForStruct.requiresRewrite(EasyMock.mock(InsertInto.class)));
  }

  @Test
  public void shouldFailTestIfStatementShouldBeRewritten() {
    assertThat("Incorrect rewrite requirement enforcement.", !StatementRewriteForStruct.requiresRewrite(EasyMock.mock(CreateTable.class)));
  }

  private static Query getQuery(final Statement statement) {
    assertThat(statement, instanceOf(Query.class));
    return (Query) statement;
  }
}