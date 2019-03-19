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

package com.koneksys.kaypher.parser;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.koneksys.kaypher.ddl.DdlConfig;
import com.koneksys.kaypher.function.TestFunctionRegistry;
import com.koneksys.kaypher.metastore.MutableMetaStore;
import com.koneksys.kaypher.metastore.model.KaypherStream;
import com.koneksys.kaypher.metastore.model.KaypherTable;
import com.koneksys.kaypher.metastore.model.KaypherTopic;
import com.koneksys.kaypher.parser.KaypherParser.PreparedStatement;
import com.koneksys.kaypher.parser.tree.AliasedRelation;
import com.koneksys.kaypher.parser.tree.ComparisonExpression;
import com.koneksys.kaypher.parser.tree.CreateStream;
import com.koneksys.kaypher.parser.tree.Join;
import com.koneksys.kaypher.parser.tree.JoinCriteria;
import com.koneksys.kaypher.parser.tree.JoinOn;
import com.koneksys.kaypher.parser.tree.PrimitiveType;
import com.koneksys.kaypher.parser.tree.QualifiedName;
import com.koneksys.kaypher.parser.tree.Statement;
import com.koneksys.kaypher.parser.tree.StringLiteral;
import com.koneksys.kaypher.parser.tree.Table;
import com.koneksys.kaypher.parser.tree.TableElement;
import com.koneksys.kaypher.parser.tree.Type.SqlType;
import com.koneksys.kaypher.parser.tree.WithinExpression;
import com.koneksys.kaypher.serde.json.KaypherJsonTopicSerDe;
import com.koneksys.kaypher.util.MetaStoreFixture;
import com.koneksys.kaypher.util.timestamp.MetadataTimestampExtractionPolicy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;

public class SqlFormatterTest {

  private AliasedRelation leftAlias;
  private AliasedRelation rightAlias;
  private JoinCriteria criteria;

  private MutableMetaStore metaStore;

  private static final Schema addressSchema = SchemaBuilder.struct()
      .field("NUMBER", Schema.OPTIONAL_INT64_SCHEMA)
      .field("STREET", Schema.OPTIONAL_STRING_SCHEMA)
      .field("CITY", Schema.OPTIONAL_STRING_SCHEMA)
      .field("STATE", Schema.OPTIONAL_STRING_SCHEMA)
      .field("ZIPCODE", Schema.OPTIONAL_INT64_SCHEMA)
      .optional().build();

  private static final Schema categorySchema = SchemaBuilder.struct()
      .field("ID", Schema.OPTIONAL_INT64_SCHEMA)
      .field("NAME", Schema.OPTIONAL_STRING_SCHEMA)
      .optional().build();

  private static final Schema itemInfoSchema = SchemaBuilder.struct()
      .field("ITEMID", Schema.INT64_SCHEMA)
      .field("NAME", Schema.STRING_SCHEMA)
      .field("CATEGORY", categorySchema)
      .optional().build();

  private static final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
  private static final Schema schemaBuilderOrders = schemaBuilder
      .field("ORDERTIME", Schema.INT64_SCHEMA)
      .field("ORDERID", Schema.OPTIONAL_INT64_SCHEMA)
      .field("ITEMID", Schema.OPTIONAL_STRING_SCHEMA)
      .field("ITEMINFO", itemInfoSchema)
      .field("ORDERUNITS", Schema.INT32_SCHEMA)
      .field("ARRAYCOL",SchemaBuilder.array(Schema.FLOAT64_SCHEMA).optional().build())
      .field("MAPCOL", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.FLOAT64_SCHEMA).optional().build())
      .field("ADDRESS", addressSchema)
      .build();


  @Before
  public void setUp() {
    final Table left = new Table(QualifiedName.of(Collections.singletonList("left")));
    final Table right = new Table(QualifiedName.of(Collections.singletonList("right")));
    leftAlias = new AliasedRelation(left, "l");
    rightAlias = new AliasedRelation(right, "r");

    criteria = new JoinOn(new ComparisonExpression(ComparisonExpression.Type.EQUAL,
                                                   new StringLiteral("left.col0"),
                                                   new StringLiteral("right.col0")));

    metaStore = MetaStoreFixture.getNewMetaStore(new TestFunctionRegistry());



    final KaypherTopic
        kaypherTopicOrders =
        new KaypherTopic("ADDRESS_TOPIC", "orders_topic", new KaypherJsonTopicSerDe(), false);

    final KaypherStream kaypherStreamOrders = new KaypherStream<>(
        "sqlexpression",
        "ADDRESS",
        schemaBuilderOrders,
        Optional.of(schemaBuilderOrders.field("ORDERTIME")),
        new MetadataTimestampExtractionPolicy(),
        kaypherTopicOrders,
        Serdes::String);

    metaStore.putTopic(kaypherTopicOrders);
    metaStore.putSource(kaypherStreamOrders);

    final KaypherTopic
        kaypherTopicItems =
        new KaypherTopic("ITEMS_TOPIC", "item_topic", new KaypherJsonTopicSerDe(), false);
    final KaypherTable<String> kaypherTableOrders = new KaypherTable<>(
        "sqlexpression",
        "ITEMID",
        itemInfoSchema,
        Optional.ofNullable(itemInfoSchema.field("ITEMID")),
        new MetadataTimestampExtractionPolicy(),
        kaypherTopicItems,
        Serdes::String);
    metaStore.putTopic(kaypherTopicItems);
    metaStore.putSource(kaypherTableOrders);
  }

  @Test
  public void testFormatSql() {

    final ArrayList<TableElement> tableElements = new ArrayList<>();
    tableElements.add(new TableElement("GROUP", PrimitiveType.of(SqlType.STRING)));
    tableElements.add(new TableElement("NOLIT", PrimitiveType.of(SqlType.STRING)));
    tableElements.add(new TableElement("Having", PrimitiveType.of(SqlType.STRING)));

    final CreateStream createStream = new CreateStream(
        QualifiedName.of("TEST"),
        tableElements,
        false,
        Collections.singletonMap(
            DdlConfig.TOPIC_NAME_PROPERTY,
            new StringLiteral("topic_test")
        ));
    final String sql = SqlFormatter.formatSql(createStream);
    assertThat("literal escaping failure", sql, containsString("`GROUP` STRING"));
    assertThat("not literal escaping failure", sql, containsString("NOLIT STRING"));
    assertThat("lowercase literal escaping failure", sql, containsString("`Having` STRING"));
    final List<PreparedStatement<?>> statements = KaypherParserTestUtil.buildAst(sql,
        MetaStoreFixture.getNewMetaStore(new TestFunctionRegistry()));
    assertFalse("formatted sql parsing error", statements.isEmpty());
  }

  @Test
  public void shouldFormatCreateWithEmptySchema() {
    final CreateStream createStream = new CreateStream(
        QualifiedName.of("TEST"),
        Collections.emptyList(),
        false,
        Collections.singletonMap(
            DdlConfig.KAFKA_TOPIC_NAME_PROPERTY,
            new StringLiteral("topic_test")
        ));
    final String sql = SqlFormatter.formatSql(createStream);
    final String expectedSql = "CREATE STREAM TEST  WITH (KAFKA_TOPIC='topic_test');";
    assertThat(sql, equalTo(expectedSql));
  }

  @Test
  public void shouldFormatLeftJoinWithWithin() {
    final Join join = new Join(Join.Type.LEFT, leftAlias, rightAlias,
                         criteria,
                         Optional.of(new WithinExpression(10, TimeUnit.SECONDS)));

    final String expected = "left L\nLEFT OUTER JOIN right R WITHIN 10 SECONDS ON "
                            + "(('left.col0' = 'right.col0'))";
    assertEquals(expected, SqlFormatter.formatSql(join));
  }

  @Test
  public void shouldFormatLeftJoinWithoutJoinWindow() {
    final Join join = new Join(Join.Type.LEFT, leftAlias, rightAlias,
                               criteria, Optional.empty());

    final String result = SqlFormatter.formatSql(join);
    final String expected = "left L\nLEFT OUTER JOIN right R ON (('left.col0' = 'right.col0'))";
    assertEquals(expected, result);
  }

  @Test
  public void shouldFormatInnerJoin() {
    final Join join = new Join(Join.Type.INNER, leftAlias, rightAlias,
                               criteria,
                               Optional.of(new WithinExpression(10, TimeUnit.SECONDS)));

    final String expected = "left L\nINNER JOIN right R WITHIN 10 SECONDS ON "
                            + "(('left.col0' = 'right.col0'))";
    assertEquals(expected, SqlFormatter.formatSql(join));
  }

  @Test
  public void shouldFormatInnerJoinWithoutJoinWindow() {
    final Join join = new Join(Join.Type.INNER, leftAlias, rightAlias,
                               criteria,
                               Optional.empty());

    final String expected = "left L\nINNER JOIN right R ON (('left.col0' = 'right.col0'))";
    assertEquals(expected, SqlFormatter.formatSql(join));
  }


  @Test
  public void shouldFormatOuterJoin() {
    final Join join = new Join(Join.Type.OUTER, leftAlias, rightAlias,
                               criteria,
                               Optional.of(new WithinExpression(10, TimeUnit.SECONDS)));

    final String expected = "left L\nFULL OUTER JOIN right R WITHIN 10 SECONDS ON"
                            + " (('left.col0' = 'right.col0'))";
    assertEquals(expected, SqlFormatter.formatSql(join));
  }


  @Test
  public void shouldFormatOuterJoinWithoutJoinWindow() {
    final Join join = new Join(Join.Type.OUTER, leftAlias, rightAlias,
                               criteria,
                               Optional.empty());

    final String expected = "left L\nFULL OUTER JOIN right R ON (('left.col0' = 'right.col0'))";
    assertEquals(expected, SqlFormatter.formatSql(join));
  }

  @Test
  public void shouldFormatSelectQueryCorrectly() {
    final String statementString =
        "CREATE STREAM S AS SELECT a.address->city FROM address a;";
    final Statement statement = KaypherParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();
    assertThat(SqlFormatter.formatSql(statement), equalTo("CREATE STREAM S AS SELECT FETCH_FIELD_FROM_STRUCT(A.ADDRESS, 'CITY') \"ADDRESS__CITY\"\n"
        + "FROM ADDRESS A"));
  }

  @Test
  public void shouldFormatSelectStarCorrectly() {
    final String statementString = "CREATE STREAM S AS SELECT * FROM address;";
    final Statement statement = KaypherParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();
    assertThat(SqlFormatter.formatSql(statement),
        equalTo("CREATE STREAM S AS SELECT *\n"
            + "FROM ADDRESS ADDRESS"));
  }

  @Test
  public void shouldFormatSelectStarCorrectlyWithOtherFields() {
    final String statementString = "CREATE STREAM S AS SELECT *, address AS city FROM address;";
    final Statement statement = KaypherParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();
    assertThat(SqlFormatter.formatSql(statement),
        equalTo("CREATE STREAM S AS SELECT\n"
            + "  *\n"
            + ", ADDRESS.ADDRESS \"CITY\"\n"
            + "FROM ADDRESS ADDRESS"));
  }

  @Test
  public void shouldFormatSelectStarCorrectlyWithJoin() {
    final String statementString = "CREATE STREAM S AS SELECT address.*, itemid.* "
        + "FROM address INNER JOIN itemid ON address.address = itemid.address->address;";
    final Statement statement = KaypherParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();
    assertThat(SqlFormatter.formatSql(statement),
        equalTo("CREATE STREAM S AS SELECT\n"
            + "  ADDRESS.*\n"
            + ", ITEMID.*\n"
            + "FROM ADDRESS ADDRESS\n"
            + "INNER JOIN ITEMID ITEMID ON ((ADDRESS.ADDRESS = ITEMID.ADDRESS->ADDRESS))"));
  }

  @Test
  public void shouldFormatSelectStarCorrectlyWithJoinOneSidedStar() {
    final String statementString = "CREATE STREAM S AS SELECT address.*, itemid.ordertime "
        + "FROM address INNER JOIN itemid ON address.address = itemid.address->address;";
    final Statement statement = KaypherParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();
    assertThat(SqlFormatter.formatSql(statement),
        equalTo("CREATE STREAM S AS SELECT\n"
            + "  ADDRESS.*\n"
            + ", ITEMID.ORDERTIME \"ORDERTIME\"\n"
            + "FROM ADDRESS ADDRESS\n"
            + "INNER JOIN ITEMID ITEMID ON ((ADDRESS.ADDRESS = ITEMID.ADDRESS->ADDRESS))"));
  }

  @Test
  public void shouldFormatSelectCorrectlyWithDuplicateFields() {
    final String statementString = "CREATE STREAM S AS SELECT address AS one, address AS two FROM address;";
    final Statement statement = KaypherParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();
    assertThat(SqlFormatter.formatSql(statement),
        equalTo("CREATE STREAM S AS SELECT\n"
            + "  ADDRESS.ADDRESS \"ONE\"\n"
            + ", ADDRESS.ADDRESS \"TWO\"\n"
            + "FROM ADDRESS ADDRESS"));
  }

  @Test
  public void shouldFormatCsasWithClause() {
    final String statementString = "CREATE STREAM S WITH(partitions=4) AS SELECT * FROM address;";
    final Statement statement = KaypherParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();

    final String result = SqlFormatter.formatSql(statement);

    assertThat(result, startsWith("CREATE STREAM S WITH (PARTITIONS = 4) AS SELECT"));
  }

  @Test
  public void shouldFormatCtasWithClause() {
    final String statementString = "CREATE TABLE S WITH(partitions=4) AS SELECT * FROM address;";
    final Statement statement = KaypherParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();

    final String result = SqlFormatter.formatSql(statement);

    assertThat(result, startsWith("CREATE TABLE S WITH (PARTITIONS = 4) AS SELECT"));
  }

  @Test
  public void shouldFormatCsasPartitionBy() {
    final String statementString = "CREATE STREAM S AS SELECT * FROM ADDRESS PARTITION BY ADDRESS;";
    final Statement statement = KaypherParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();

    final String result = SqlFormatter.formatSql(statement);

    assertThat(result, startsWith("CREATE STREAM S AS SELECT *\n"
        + "FROM ADDRESS ADDRESS\n"
        + "PARTITION BY ADDRESS"));
  }

  @Test
  public void shouldFormatInsertIntoPartitionBy() {
    final String statementString = "INSERT INTO ADDRESS SELECT * FROM ADDRESS PARTITION BY ADDRESS;";
    final Statement statement = KaypherParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();

    final String result = SqlFormatter.formatSql(statement);

    assertThat(result, startsWith("INSERT INTO ADDRESS SELECT *\n"
        + "FROM ADDRESS ADDRESS\n"
        + "PARTITION BY ADDRESS"));
  }

  @Test
  public void shouldFormatExplainQuery() {
    final String statementString = "EXPLAIN foo;";
    final Statement statement = KaypherParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();

    final String result = SqlFormatter.formatSql(statement);

    assertThat(result, is("EXPLAIN \nfoo"));
  }

  @Test
  public void shouldFormatExplainStatement() {
    final String statementString = "EXPLAIN SELECT * FROM ADDRESS;";
    final Statement statement = KaypherParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();

    final String result = SqlFormatter.formatSql(statement);

    assertThat(result, is("EXPLAIN \nSELECT *\nFROM ADDRESS ADDRESS"));
  }
}

