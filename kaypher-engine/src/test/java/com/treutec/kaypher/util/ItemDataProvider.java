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

import com.treutec.kaypher.GenericRow;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.schema.kaypher.PhysicalSchema;
import com.treutec.kaypher.schema.kaypher.types.SqlTypes;
import com.treutec.kaypher.serde.SerdeOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ItemDataProvider extends TestDataProvider {

  private static final String namePrefix =
      "ITEM";

  private static final String kaypherSchemaString =
      "(ID varchar, DESCRIPTION varchar)";

  private static final String key = "ID";

  private static final LogicalSchema schema = LogicalSchema.builder()
      .valueColumn(ColumnName.of("ID"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("DESCRIPTION"), SqlTypes.STRING)
      .build();

  private static final Map<String, GenericRow> data = buildData();

  public ItemDataProvider() {
    super(namePrefix, kaypherSchemaString, key, PhysicalSchema.from(schema, SerdeOption.none()), data);
  }

  private static Map<String, GenericRow> buildData() {

    final Map<String, GenericRow> dataMap = new HashMap<>();
    dataMap.put("ITEM_1", new GenericRow(Arrays.asList("ITEM_1",  "home cinema")));
    dataMap.put("ITEM_2", new GenericRow(Arrays.asList("ITEM_2",  "clock radio")));
    dataMap.put("ITEM_3", new GenericRow(Arrays.asList("ITEM_3",  "road bike")));
    dataMap.put("ITEM_4", new GenericRow(Arrays.asList("ITEM_4",  "mountain bike")));
    dataMap.put("ITEM_5", new GenericRow(Arrays.asList("ITEM_5",  "snowboard")));
    dataMap.put("ITEM_6", new GenericRow(Arrays.asList("ITEM_6",  "iphone 10")));
    dataMap.put("ITEM_7", new GenericRow(Arrays.asList("ITEM_7",  "gopro")));
    dataMap.put("ITEM_8", new GenericRow(Arrays.asList("ITEM_8",  "cat")));

    return dataMap;
  }

}
