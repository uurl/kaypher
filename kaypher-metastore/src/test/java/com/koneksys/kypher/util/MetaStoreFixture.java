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

package com.koneksys.kaypher.util;

import com.koneksys.kaypher.function.FunctionRegistry;
import com.koneksys.kaypher.metastore.MetaStoreImpl;
import com.koneksys.kaypher.metastore.MutableMetaStore;
import com.koneksys.kaypher.metastore.model.kaypherStream;
import com.koneksys.kaypher.metastore.model.kaypherTable;
import com.koneksys.kaypher.metastore.model.kaypherTopic;
import com.koneksys.kaypher.serde.kaypherTopicSerDe;
import com.koneksys.kaypher.serde.json.kaypherJsonTopicSerDe;
import com.koneksys.kaypher.util.timestamp.MetadataTimestampExtractionPolicy;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public final class MetaStoreFixture {

  private MetaStoreFixture() {
  }

  public static MutableMetaStore getNewMetaStore(final FunctionRegistry functionRegistry) {
    return getNewMetaStore(functionRegistry, kaypherJsonTopicSerDe::new);
  }

  public static MutableMetaStore getNewMetaStore(final FunctionRegistry functionRegistry,
                                          final Supplier<kaypherTopicSerDe> serde) {

    final MetadataTimestampExtractionPolicy timestampExtractionPolicy
        = new MetadataTimestampExtractionPolicy();
    final MutableMetaStore metaStore = new MetaStoreImpl(functionRegistry);

    final Schema test1Schema = SchemaBuilder.struct()
        .field("ROWTIME", Schema.OPTIONAL_INT64_SCHEMA)
        .field("ROWKEY", Schema.OPTIONAL_INT64_SCHEMA)
        .field("COL0", Schema.OPTIONAL_INT64_SCHEMA)
        .field("COL1", Schema.OPTIONAL_STRING_SCHEMA)
        .field("COL2", Schema.OPTIONAL_STRING_SCHEMA)
        .field("COL3", Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field("COL4", SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
        .field("COL5", SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
        .build();

    final kaypherTopic
        kaypherTopic0 =
        new kaypherTopic("TEST0", "test0", serde.get(), false);

    final kaypherStream<?> kaypherStream0 = new kaypherStream<>(
        "sqlexpression",
        "TEST0",
        test1Schema,
        Optional.of(test1Schema.field("COL0")),
        timestampExtractionPolicy,
        kaypherTopic0,
        Serdes::String);

    metaStore.putTopic(kaypherTopic0);
    metaStore.putSource(kaypherStream0);

    final kaypherTopic
        kaypherTopic1 =
        new kaypherTopic("TEST1", "test1", serde.get(), false);

    final kaypherStream<?> kaypherStream1 = new kaypherStream<>("sqlexpression",
        "TEST1",
        test1Schema,
        Optional.of(test1Schema.field("COL0")),
        timestampExtractionPolicy,
        kaypherTopic1,
        Serdes::String);

    metaStore.putTopic(kaypherTopic1);
    metaStore.putSource(kaypherStream1);

    final Schema test2Schema = SchemaBuilder.struct()
        .field("ROWTIME", Schema.OPTIONAL_INT64_SCHEMA)
        .field("ROWKEY", Schema.OPTIONAL_INT64_SCHEMA)
        .field("COL0", Schema.OPTIONAL_INT64_SCHEMA)
        .field("COL1", Schema.OPTIONAL_STRING_SCHEMA)
        .field("COL2", Schema.OPTIONAL_STRING_SCHEMA)
        .field("COL3", Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field("COL4", Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .build();

    final kaypherTopic
        kaypherTopic2 =
        new kaypherTopic("TEST2", "test2", serde.get(), false);
    final kaypherTable<String> kaypherTable = new kaypherTable<>(
        "sqlexpression",
        "TEST2",
        test2Schema,
        Optional.ofNullable(test2Schema.field("COL0")),
        timestampExtractionPolicy,
        kaypherTopic2,
        Serdes::String);

    metaStore.putTopic(kaypherTopic2);
    metaStore.putSource(kaypherTable);

    final Schema addressSchema = SchemaBuilder.struct()
        .field("NUMBER", Schema.OPTIONAL_INT64_SCHEMA)
        .field("STREET", Schema.OPTIONAL_STRING_SCHEMA)
        .field("CITY", Schema.OPTIONAL_STRING_SCHEMA)
        .field("STATE", Schema.OPTIONAL_STRING_SCHEMA)
        .field("ZIPCODE", Schema.OPTIONAL_INT64_SCHEMA)
        .optional().build();

    final Schema categorySchema = SchemaBuilder.struct()
        .field("ID", Schema.OPTIONAL_INT64_SCHEMA)
        .field("NAME", Schema.OPTIONAL_STRING_SCHEMA)
        .optional().build();

    final Schema itemInfoSchema = SchemaBuilder.struct()
        .field("ITEMID", Schema.OPTIONAL_INT64_SCHEMA)
        .field("NAME", Schema.OPTIONAL_STRING_SCHEMA)
        .field("CATEGORY", categorySchema)
        .optional().build();

    final Schema ordersSchema = SchemaBuilder.struct()
        .field("ORDERTIME", Schema.OPTIONAL_INT64_SCHEMA)
        .field("ORDERID", Schema.OPTIONAL_INT64_SCHEMA)
        .field("ITEMID", Schema.OPTIONAL_STRING_SCHEMA)
        .field("ITEMINFO", itemInfoSchema)
        .field("ORDERUNITS", Schema.INT32_SCHEMA)
        .field("ARRAYCOL",SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
        .field("MAPCOL", SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build())
        .field("ADDRESS", addressSchema)
        .optional().build();

    final kaypherTopic
        kaypherTopicOrders =
        new kaypherTopic("ORDERS_TOPIC", "orders_topic", serde.get(), false);

    final kaypherStream<?> kaypherStreamOrders = new kaypherStream<>(
        "sqlexpression",
        "ORDERS",
        ordersSchema,
        Optional.of(ordersSchema.field("ORDERTIME")),
        timestampExtractionPolicy,
        kaypherTopicOrders,
        Serdes::String);

    metaStore.putTopic(kaypherTopicOrders);
    metaStore.putSource(kaypherStreamOrders);

    final Schema schemaBuilderTestTable3 = SchemaBuilder.struct()
        .field("ROWTIME", Schema.OPTIONAL_INT64_SCHEMA)
        .field("ROWKEY", Schema.OPTIONAL_INT64_SCHEMA)
        .field("COL0", Schema.OPTIONAL_INT64_SCHEMA)
        .field("COL1", Schema.OPTIONAL_STRING_SCHEMA)
        .field("COL2", Schema.OPTIONAL_STRING_SCHEMA)
        .field("COL3", Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field("COL4", Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .build();

    final kaypherTopic
        kaypherTopic3 =
        new kaypherTopic("TEST3", "test3", serde.get(), false);
    final kaypherTable<String> kaypherTable3 = new kaypherTable<>(
        "sqlexpression",
        "TEST3",
        schemaBuilderTestTable3,
        Optional.ofNullable(schemaBuilderTestTable3.field("COL0")),
        timestampExtractionPolicy,
        kaypherTopic3,
        Serdes::String);

    metaStore.putTopic(kaypherTopic3);
    metaStore.putSource(kaypherTable3);

    final Schema nestedArrayStructMapSchema = SchemaBuilder.struct()
        .field("ARRAYCOL", SchemaBuilder.array(itemInfoSchema).build())
        .field("MAPCOL", SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, itemInfoSchema).build())
        .field("NESTED_ORDER_COL", ordersSchema)
        .field("ITEM", itemInfoSchema)
        .optional().build();

    final kaypherTopic
        nestedArrayStructMapTopic =
        new kaypherTopic("NestedArrayStructMap", "NestedArrayStructMap_topic", serde.get(), false);

    final kaypherStream<?> nestedArrayStructMapOrders = new kaypherStream<>(
        "sqlexpression",
        "NESTED_STREAM",
        nestedArrayStructMapSchema,
        Optional.empty(),
        timestampExtractionPolicy,
        nestedArrayStructMapTopic,
        Serdes::String);

    metaStore.putTopic(nestedArrayStructMapTopic);
    metaStore.putSource(nestedArrayStructMapOrders);

    return metaStore;
  }
}
