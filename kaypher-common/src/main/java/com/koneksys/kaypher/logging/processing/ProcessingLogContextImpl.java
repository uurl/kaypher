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

package com.koneksys.kaypher.logging.processing;

import com.koneksys.common.logging.StructuredLoggerFactory;

public final class ProcessingLogContextImpl implements ProcessingLogContext {
  private final ProcessingLogConfig config;
  private final ProcessingLoggerFactory loggerFactory;

  ProcessingLogContextImpl(final ProcessingLogConfig config) {
    this.config = config;
    this.loggerFactory = new ProcessingLoggerFactoryImpl(
        config,
        new StructuredLoggerFactory(ProcessingLogConstants.PREFIX)
    );
  }

  public ProcessingLogConfig getConfig() {
    return config;
  }

  @Override
  public ProcessingLoggerFactory getLoggerFactory() {
    return loggerFactory;
  }
}
