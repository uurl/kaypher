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

package com.koneksys.kaypher.errors;

import com.koneksys.kaypher.logging.processing.ProcessingLogConfig;
import com.koneksys.kaypher.logging.processing.ProcessingLogMessageSchema;
import com.koneksys.kaypher.logging.processing.ProcessingLogMessageSchema.MessageType;
import com.koneksys.kaypher.logging.processing.ProcessingLogger;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;

public final class ProductionExceptionHandlerUtil {
  public static final String KSQL_PRODUCTION_ERROR_LOGGER = "kaypher.logger.production.error";

  private ProductionExceptionHandlerUtil() {
  }

  public static Class<?> getHandler(final boolean failOnError) {
    return failOnError
        ? LogAndFailProductionExceptionHandler.class
        : LogAndContinueProductionExceptionHandler.class;
  }

  abstract static class LogAndXProductionExceptionHandler implements ProductionExceptionHandler {

    private ProcessingLogger logger;

    @Override
    public ProductionExceptionHandlerResponse handle(
        final ProducerRecord<byte[], byte[]> record, final Exception exception) {
      logger.error(productionError(exception.getMessage()));
      return getResponse();
    }

    @Override
    public void configure(final Map<String, ?> configs) {
      final Object logger = configs.get(KSQL_PRODUCTION_ERROR_LOGGER);
      if (! (logger instanceof ProcessingLogger)) {
        throw new IllegalArgumentException("Invalid value for logger: " + logger.toString());
      }
      this.logger = (ProcessingLogger) logger;
    }

    abstract ProductionExceptionHandlerResponse getResponse();
  }

  private static Function<ProcessingLogConfig, SchemaAndValue> productionError(
      final String errorMsg) {
    return (config) -> {
      final Struct struct = new Struct(ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA);
      struct.put(ProcessingLogMessageSchema.TYPE, MessageType.PRODUCTION_ERROR.getTypeId());
      final Struct productionError =
          new Struct(MessageType.PRODUCTION_ERROR.getSchema());
      struct.put(ProcessingLogMessageSchema.PRODUCTION_ERROR, productionError);
      productionError.put(
          ProcessingLogMessageSchema.PRODUCTION_ERROR_FIELD_MESSAGE,
          errorMsg);
      return new SchemaAndValue(ProcessingLogMessageSchema.PROCESSING_LOG_SCHEMA, struct);
    };
  }

  public static class LogAndFailProductionExceptionHandler
      extends LogAndXProductionExceptionHandler {

    @Override
    ProductionExceptionHandlerResponse getResponse() {
      return ProductionExceptionHandlerResponse.FAIL;
    }
  }

  public static class LogAndContinueProductionExceptionHandler
      extends LogAndXProductionExceptionHandler {

    @Override
    ProductionExceptionHandlerResponse getResponse() {
      return ProductionExceptionHandlerResponse.CONTINUE;
    }
  }
}


