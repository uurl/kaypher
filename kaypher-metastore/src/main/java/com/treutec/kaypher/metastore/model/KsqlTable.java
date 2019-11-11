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
package com.treutec.kaypher.metastore.model;

import com.google.errorprone.annotations.Immutable;
import com.treutec.kaypher.execution.ddl.commands.KaypherTopic;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.serde.SerdeOption;
import com.treutec.kaypher.util.timestamp.TimestampExtractionPolicy;
import java.util.Set;

@Immutable
public class KaypherTable<K> extends StructuredDataSource<K> {

  public KaypherTable(
      final String sqlExpression,
      final SourceName datasourceName,
      final LogicalSchema schema,
      final Set<SerdeOption> serdeOptions,
      final KeyField keyField,
      final TimestampExtractionPolicy timestampExtractionPolicy,
      final KaypherTopic kaypherTopic
  ) {
    super(
        sqlExpression,
        datasourceName,
        schema,
        serdeOptions,
        keyField,
        timestampExtractionPolicy,
        DataSourceType.KTABLE,
        kaypherTopic
    );
  }
}
