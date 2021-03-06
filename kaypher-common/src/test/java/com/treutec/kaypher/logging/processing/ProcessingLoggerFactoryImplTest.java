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

package com.treutec.kaypher.logging.processing;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.common.logging.StructuredLogger;
import io.confluent.common.logging.StructuredLoggerFactory;
import java.util.Collection;
import java.util.function.BiFunction;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ProcessingLoggerFactoryImplTest {
  @Mock
  private StructuredLoggerFactory innerFactory;
  @Mock
  private StructuredLogger innerLogger;
  @Mock
  private ProcessingLogConfig config;
  @Mock
  private BiFunction<ProcessingLogConfig, StructuredLogger, ProcessingLogger> loggerFactory;
  @Mock
  private ProcessingLogger logger;

  private final Collection<String> loggers = ImmutableList.of("logger1", "logger2");

  private ProcessingLoggerFactoryImpl factory;

  @Before
  public void setup() {
    when(innerFactory.getLogger(anyString())).thenReturn(innerLogger);
    when(innerFactory.getLoggers()).thenReturn(loggers);
    when(loggerFactory.apply(config, innerLogger)).thenReturn(logger);
    factory = new ProcessingLoggerFactoryImpl(config, innerFactory, loggerFactory);
  }

  @Test
  public void shouldCreateLogger() {
    // When:
    final ProcessingLogger logger = factory.getLogger("foo.bar");

    // Then:
    assertThat(logger, is(this.logger));
    verify(innerFactory).getLogger("foo.bar");
    verify(loggerFactory).apply(config, innerLogger);
  }

  @Test
  public void shouldGetLoggers() {
    // When:
    final Collection<String> loggers = factory.getLoggers();

    // Then:
    assertThat(loggers, equalTo(this.loggers));
  }
}