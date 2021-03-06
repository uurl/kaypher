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

package com.treutec.kaypher.metrics;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.common.utils.Time;
import com.treutec.kaypher.metrics.TopicSensors.SensorMetric;
import com.treutec.kaypher.metrics.TopicSensors.Stat;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Sensor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TopicSensorsTest {

  @Mock
  private Sensor sensor;
  @Mock
  private KafkaMetric metric;
  @Mock
  private Time time;

  @Test
  public void shouldFormatTimestampInUnambiguousFormatAndUTC() {
    // Given:
    final Stat stat = new Stat("stat", 5.0, 1538842403035L);

    // When:
    final String timestamp = stat.timestamp();

    // Then:
    assertThat(timestamp, is("2018-10-06T16:13:23.035Z"));
  }

  @Test
  public void shouldGetMetricValueCorrectly() {
    // Given:
    final SensorMetric<?> sensorMetric = new SensorMetric<>(sensor, metric, time, false);

    // When:
    when(metric.metricValue()).thenReturn(1.2345);

    // Then:
    assertThat(sensorMetric.value(), equalTo(1.2345));
    verify(metric).metricValue();
  }
}