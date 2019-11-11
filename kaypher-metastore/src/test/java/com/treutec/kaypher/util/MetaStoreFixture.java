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
package com.treutec.kaypher.util;

import com.treutec.kaypher.execution.ddl.commands.KaypherTopic;
import com.treutec.kaypher.function.FunctionRegistry;
import com.treutec.kaypher.metastore.MetaStoreImpl;
import com.treutec.kaypher.metastore.MutableMetaStore;
import com.treutec.kaypher.metastore.model.KeyField;
import com.treutec.kaypher.metastore.model.KaypherStream;
import com.treutec.kaypher.metastore.model.KaypherTable;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.schema.kaypher.ColumnRef;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.schema.kaypher.types.SqlType;
import com.treutec.kaypher.schema.kaypher.types.SqlTypes;
import com.treutec.kaypher.serde.Format;
import com.treutec.kaypher.serde.FormatInfo;
import com.treutec.kaypher.serde.KeyFormat;
import com.treutec.kaypher.serde.SerdeOption;
import com.treutec.kaypher.serde.ValueFormat;
import com.treutec.kaypher.util.timestamp.MetadataTimestampExtractionPolicy;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public final class MetaStoreFixture {

  private MetaStoreFixture() {
  }

  public static MutableMetaStore getNewMetaStore(final FunctionRegistry functionRegistry) {
    return getNewMetaStore(functionRegistry, ValueFormat.of(FormatInfo.of(Format.JSON)));
  }

  public static MutableMetaStore getNewMetaStore(
      final FunctionRegistry functionRegistry,
      final ValueFormat valueFormat
  ) {
    final MetadataTimestampExtractionPolicy timestampExtractionPolicy
        = new MetadataTimestampExtractionPolicy();

    final MutableMetaStore metaStore = new MetaStoreImpl(functionRegistry);

    final KeyFormat keyFormat = KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA));

    final LogicalSchema test1Schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("COL0"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("COL1"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("COL2"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("COL3"), SqlTypes.DOUBLE)
        .valueColumn(ColumnName.of("COL4"), SqlTypes.array(SqlTypes.DOUBLE))
        .valueColumn(ColumnName.of("COL5"), SqlTypes.map(SqlTypes.DOUBLE))
        .build();

    final KaypherTopic kaypherTopic0 = new KaypherTopic(
        "test0",
        keyFormat,
        valueFormat,
        false
    );

    final KaypherStream<?> kaypherStream0 = new KaypherStream<>(
        "sqlexpression",
        SourceName.of("TEST0"),
        test1Schema,
        SerdeOption.none(),
        KeyField.of(ColumnRef.withoutSource(ColumnName.of("COL0"))),
        timestampExtractionPolicy,
        kaypherTopic0
    );

    metaStore.putSource(kaypherStream0);

    final KaypherTopic kaypherTopic1 = new KaypherTopic(
        "test1",
        keyFormat,
        valueFormat,
        false
    );

    final KaypherStream<?> kaypherStream1 = new KaypherStream<>(
        "sqlexpression",
        SourceName.of("TEST1"),
        test1Schema,
        SerdeOption.none(),
        KeyField.of(
            ColumnRef.withoutSource(ColumnName.of("COL0"))),
        timestampExtractionPolicy,
        kaypherTopic1
    );

    metaStore.putSource(kaypherStream1);

    final LogicalSchema test2Schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("COL0"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("COL1"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("COL2"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("COL3"), SqlTypes.DOUBLE)
        .valueColumn(ColumnName.of("COL4"), SqlTypes.BOOLEAN)
        .build();

    final KaypherTopic kaypherTopic2 = new KaypherTopic(
        "test2",
        keyFormat,
        valueFormat,
        false
    );
    final KaypherTable<String> kaypherTable = new KaypherTable<>(
        "sqlexpression",
        SourceName.of("TEST2"),
        test2Schema,
        SerdeOption.none(),
        KeyField.of(
            ColumnRef.withoutSource(ColumnName.of("COL0"))),
        timestampExtractionPolicy,
        kaypherTopic2
    );

    metaStore.putSource(kaypherTable);

    final SqlType addressSchema = SqlTypes.struct()
        .field("NUMBER", SqlTypes.BIGINT)
        .field("STREET", SqlTypes.STRING)
        .field("CITY", SqlTypes.STRING)
        .field("STATE", SqlTypes.STRING)
        .field("ZIPCODE", SqlTypes.BIGINT)
        .build();

    final SqlType categorySchema = SqlTypes.struct()
        .field("ID", SqlTypes.BIGINT)
        .field("NAME", SqlTypes.STRING)
        .build();

    final SqlType itemInfoSchema = SqlTypes.struct()
        .field("ITEMID", SqlTypes.BIGINT)
        .field("NAME", SqlTypes.STRING)
        .field("CATEGORY", categorySchema)
        .build();

    final LogicalSchema ordersSchema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("ORDERTIME"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("ORDERID"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("ITEMID"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("ITEMINFO"), itemInfoSchema)
        .valueColumn(ColumnName.of("ORDERUNITS"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("ARRAYCOL"), SqlTypes.array(SqlTypes.DOUBLE))
        .valueColumn(ColumnName.of("MAPCOL"), SqlTypes.map(SqlTypes.DOUBLE))
        .valueColumn(ColumnName.of("ADDRESS"), addressSchema)
        .build();

    final KaypherTopic kaypherTopicOrders = new KaypherTopic(
        "orders_topic",
        keyFormat,
        valueFormat,
        false
    );

    final KaypherStream<?> kaypherStreamOrders = new KaypherStream<>(
        "sqlexpression",
        SourceName.of("ORDERS"),
        ordersSchema,
        SerdeOption.none(),
        KeyField.of(ColumnRef.withoutSource(ColumnName.of("ORDERTIME"))),
        timestampExtractionPolicy,
        kaypherTopicOrders
    );

    metaStore.putSource(kaypherStreamOrders);

    final LogicalSchema testTable3 = LogicalSchema.builder()
        .valueColumn(ColumnName.of("COL0"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("COL1"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("COL2"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("COL3"), SqlTypes.DOUBLE)
        .valueColumn(ColumnName.of("COL4"), SqlTypes.BOOLEAN)
        .build();

    final KaypherTopic kaypherTopic3 = new KaypherTopic(
        "test3",
        keyFormat,
        valueFormat,
        false
    );
    final KaypherTable<String> kaypherTable3 = new KaypherTable<>(
        "sqlexpression",
        SourceName.of("TEST3"),
        testTable3,
        SerdeOption.none(),
        KeyField.of(
            ColumnRef.withoutSource(ColumnName.of("COL0"))),
        timestampExtractionPolicy,
        kaypherTopic3
    );

    metaStore.putSource(kaypherTable3);

    final SqlType nestedOrdersSchema = SqlTypes.struct()
        .field("ORDERTIME", SqlTypes.BIGINT)
        .field("ORDERID", SqlTypes.BIGINT)
        .field("ITEMID", SqlTypes.STRING)
        .field("ITEMINFO", itemInfoSchema)
        .field("ORDERUNITS", SqlTypes.INTEGER)
        .field("ARRAYCOL", SqlTypes.array(SqlTypes.DOUBLE))
        .field("MAPCOL", SqlTypes.map(SqlTypes.DOUBLE))
        .field("ADDRESS", addressSchema)
        .build();

    final LogicalSchema nestedArrayStructMapSchema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("ARRAYCOL"), SqlTypes.array(itemInfoSchema))
        .valueColumn(ColumnName.of("MAPCOL"), SqlTypes.map(itemInfoSchema))
        .valueColumn(ColumnName.of("NESTED_ORDER_COL"), nestedOrdersSchema)
        .valueColumn(ColumnName.of("ITEM"), itemInfoSchema)
        .build();

    final KaypherTopic nestedArrayStructMapTopic = new KaypherTopic(
        "NestedArrayStructMap_topic",
        keyFormat,
        valueFormat,
        false
    );

    final KaypherStream<?> nestedArrayStructMapOrders = new KaypherStream<>(
        "sqlexpression",
        SourceName.of("NESTED_STREAM"),
        nestedArrayStructMapSchema,
        SerdeOption.none(),
        KeyField.none(),
        timestampExtractionPolicy,
        nestedArrayStructMapTopic
    );

    metaStore.putSource(nestedArrayStructMapOrders);

    final KaypherTopic kaypherTopic4 = new KaypherTopic(
        "test4",
        keyFormat,
        valueFormat,
        false
    );

    final KaypherStream<?> kaypherStream4 = new KaypherStream<>(
        "sqlexpression4",
        SourceName.of("TEST4"),
        test1Schema,
        SerdeOption.none(),
        KeyField.none(),
        timestampExtractionPolicy,
        kaypherTopic4
    );

    metaStore.putSource(kaypherStream4);



    final LogicalSchema sensorReadingsSchema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("ID"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("SENSOR_NAME"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("ARR1"), SqlTypes.array(SqlTypes.BIGINT))
        .valueColumn(ColumnName.of("ARR2"), SqlTypes.array(SqlTypes.STRING))
        .build();

    final KaypherTopic kaypherTopicSensorReadings = new KaypherTopic(
        "sensor_readings_topic",
        keyFormat,
        valueFormat,
        false
    );

    final KaypherStream<?> kaypherStreamSensorReadings = new KaypherStream<>(
        "sqlexpression",
        SourceName.of("SENSOR_READINGS"),
        sensorReadingsSchema,
        SerdeOption.none(),
        KeyField.of(ColumnRef.withoutSource(ColumnName.of("ID"))),
        timestampExtractionPolicy,
        kaypherTopicSensorReadings
    );

    metaStore.putSource(kaypherStreamSensorReadings);

    return metaStore;
  }
}
