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

package com.treutec.kaypher.schema.connect;

import com.google.errorprone.annotations.Immutable;
import org.apache.kafka.connect.data.Schema;

/**
 * Formatter for Connect schema type.
 *
 * <p>Converts a Connect schema to human readable text.
 */
@Immutable
public interface SchemaFormatter {

  /**
   * Convert the supplied Connect schema into a human readable (ish) string.
   *
   * @param schema the schema to format.
   * @return the schema formatted as a human readable string.
   */
  String format(Schema schema);
}
