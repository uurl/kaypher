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
 */package com.treutec.kaypher.util;

import com.treutec.kaypher.GenericRow;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.schema.kaypher.PhysicalSchema;
import com.treutec.kaypher.schema.kaypher.types.SqlTypes;
import com.treutec.kaypher.serde.SerdeOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class UserDataProvider extends TestDataProvider {
  private static final String namePrefix = "USER";

  private static final String kaypherSchemaString = "(REGISTERTIME bigint, GENDER varchar, REGIONID varchar, USERID varchar)";

  private static final String key = "USERID";

  private static final LogicalSchema schema = LogicalSchema.builder()
      .valueColumn(ColumnName.of("REGISTERTIME"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("GENDER"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("REGIONID"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("USERID"), SqlTypes.STRING)
      .build();

  private static final Map<String, GenericRow> data = buildData();

  public UserDataProvider() {
    super(namePrefix, kaypherSchemaString, key, PhysicalSchema.from(schema, SerdeOption.none()), data);
  }

  private static Map<String, GenericRow> buildData() {
    final Map<String, GenericRow> dataMap = new HashMap<>();
    // create a records with:
    // key == user_id
    // value = (creation_time, gender, region, user_id)
    dataMap.put("USER_0", new GenericRow(Arrays.asList(0L, "FEMALE", "REGION_0", "USER_0")));
    dataMap.put("USER_1", new GenericRow(Arrays.asList(1L, "MALE", "REGION_1", "USER_1")));
    dataMap.put("USER_2", new GenericRow(Arrays.asList(2L, "FEMALE", "REGION_1", "USER_2")));
    dataMap.put("USER_3", new GenericRow(Arrays.asList(3L, "MALE", "REGION_0", "USER_3")));
    dataMap.put("USER_4", new GenericRow(Arrays.asList(4L, "MALE", "REGION_4", "USER_4")));

    return dataMap;
  }
}