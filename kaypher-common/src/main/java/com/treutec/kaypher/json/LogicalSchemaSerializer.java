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

package com.treutec.kaypher.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import java.io.IOException;

/**
 * Custom Jackson JSON serializer for {@link LogicalSchema}.
 *
 * <p>The schema is serialized as a simple SQL string
 */
final class LogicalSchemaSerializer extends JsonSerializer<LogicalSchema> {

  @Override
  public void serialize(
      final LogicalSchema schema,
      final JsonGenerator gen,
      final SerializerProvider serializerProvider
  ) throws IOException {
    final String text = schema.toString();
    gen.writeString(trimArrayBrackets(text));
  }

  private static String trimArrayBrackets(final String text) {
    return text.substring(1, text.length() - 1);
  }
}
