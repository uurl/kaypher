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

package com.treutec.kaypher.serde.util;

import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import io.confluent.ksql.logging.processing.ProcessingLogMessageSchema;
import io.confluent.ksql.logging.processing.ProcessingLogMessageSchema.MessageType;
import io.confluent.ksql.util.ErrorMessageUtil;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;

public final class SerdeProcessingLogMessageFactory {
  private SerdeProcessingLogMessageFactory() {
  }

  public static Function<ProcessingLogConfig, SchemaAndValue> deserializationErrorMsg(
      final Throwable exception,
      final Optional<byte[]> record
  ) {
    Objects.requireNonNull(exception);
    return (config) -> {
      final Struct struct = new Struct(ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA);
      final Struct deserializationError = new Struct(MessageType.DESERIALIZATION_ERROR.getSchema());
      deserializationError.put(
          ProcessingLogMessageSchema.DESERIALIZATION_ERROR_FIELD_MESSAGE,
          exception.getMessage());
      final List<String> cause = ErrorMessageUtil.getErrorMessages(exception);
      cause.remove(0);
      deserializationError.put(
          ProcessingLogMessageSchema.DESERIALIZATION_ERROR_FIELD_CAUSE,
          cause
      );
      if (config.getBoolean(ProcessingLogConfig.INCLUDE_ROWS)) {
        deserializationError.put(
            ProcessingLogMessageSchema.DESERIALIZATION_ERROR_FIELD_RECORD_B64,
            record.map(Base64.getEncoder()::encodeToString).orElse(null)
        );
      }
      struct.put(ProcessingLogMessageSchema.DESERIALIZATION_ERROR, deserializationError);
      struct.put(ProcessingLogMessageSchema.TYPE, MessageType.DESERIALIZATION_ERROR.getTypeId());
      return new SchemaAndValue(ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA, struct);
    };
  }
}
