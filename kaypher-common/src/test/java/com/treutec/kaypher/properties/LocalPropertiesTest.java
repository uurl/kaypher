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

package com.treutec.kaypher.properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.treutec.kaypher.config.PropertyParser;
import com.treutec.kaypher.util.KaypherConfig;
import com.treutec.kaypher.util.KaypherException;
import java.util.Map;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LocalPropertiesTest {

  private static final Map<String, Object> INITIAL = ImmutableMap.of(
      "prop-1", "initial-val-1",
      "prop-2", "initial-val-2"
  );

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private PropertyParser parser;
  private LocalProperties propsWithMockParser;
  private LocalProperties realProps;

  @Before
  public void setUp() {
    when(parser.parse(any(), any()))
        .thenAnswer(inv -> "parsed-" + inv.getArgument(1));

    propsWithMockParser = new LocalProperties(INITIAL, parser);

    realProps = new LocalProperties(ImmutableMap.of());
  }

  @Test
  public void shouldValidateInitialPropsByParsing() {
    assertThat(propsWithMockParser.toMap().get("prop-1"), is("parsed-initial-val-1"));
    assertThat(propsWithMockParser.toMap().get("prop-2"), is("parsed-initial-val-2"));
  }

  @Test
  public void shouldThrowInInitialPropsInvalid() {
    // Given:
    final Map<String, Object> invalid = ImmutableMap.of(
        "this.is.not.valid", "value"
    );

    // Then:
    expectedException.expect(KaypherException.class);
    expectedException.expectMessage("invalid property found");
    expectedException.expectMessage("'this.is.not.valid'");

    // When:
    new LocalProperties(invalid);
  }

  @Test
  public void shouldUnsetInitialValue() {
    // When:
    final Object oldValue = propsWithMockParser.unset("prop-1");

    // Then:
    assertThat(oldValue, is("parsed-initial-val-1"));
    assertThat(propsWithMockParser.toMap().get("prop-1"), is(nullValue()));
    assertThat(propsWithMockParser.toMap().get("prop-2"), is("parsed-initial-val-2"));
  }

  @Test
  public void shouldOverrideInitialValue() {
    // When:
    final Object oldValue = propsWithMockParser.set("prop-1", "new-val");

    // Then:
    assertThat(oldValue, is("parsed-initial-val-1"));
    assertThat(propsWithMockParser.toMap().get("prop-1"), is("parsed-new-val"));
    assertThat(propsWithMockParser.toMap().get("prop-2"), is("parsed-initial-val-2"));
  }

  @Test
  public void shouldUnsetOverriddenValue() {
    // Given:
    propsWithMockParser.set("prop-1", "new-val");

    // When:
    final Object oldValue = propsWithMockParser.unset("prop-1");

    // Then:
    assertThat(oldValue, is("parsed-new-val"));
    assertThat(propsWithMockParser.toMap().get("prop-1"), is(nullValue()));
    assertThat(propsWithMockParser.toMap().get("prop-2"), is("parsed-initial-val-2"));
  }

  @Test
  public void shouldReturnOnlyKnownKeys() {
    assertThat(propsWithMockParser.toMap().keySet(), containsInAnyOrder("prop-1", "prop-2"));
  }

  @Test
  public void shouldSetNewValue() {
    // When:
    final Object oldValue = propsWithMockParser.set("new-prop", "new-val");

    // Then:
    assertThat(oldValue, is(nullValue()));
    assertThat(propsWithMockParser.toMap().get("new-prop"), is("parsed-new-val"));
    assertThat(propsWithMockParser.toMap().get("prop-2"), is("parsed-initial-val-2"));
  }

  @Test
  public void shouldUnsetNewValue() {
    // Given:
    propsWithMockParser.set("new-prop", "new-val");

    // When:
    final Object oldValue = propsWithMockParser.unset("new-prop");

    // Then:
    assertThat(oldValue, is("parsed-new-val"));
    assertThat(propsWithMockParser.toMap().get("new-prop"), is(nullValue()));
    assertThat(propsWithMockParser.toMap().get("prop-2"), is("parsed-initial-val-2"));
  }

  @Test
  public void shouldInvokeParserCorrectly() {
    // Given:
    when(parser.parse("prop-1", "new-val")).thenReturn("parsed-new-val");

    // When:
    propsWithMockParser.set("prop-1", "new-val");

    // Then:
    assertThat(propsWithMockParser.toMap().get("prop-1"), is("parsed-new-val"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfParserThrows() {
    // Given:
    when(parser.parse("prop-1", "new-val"))
        .thenThrow(new IllegalArgumentException("Boom"));

    // When:
    propsWithMockParser.set("prop-1", "new-val");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldNotAllowUnknownPropertyToBeSet() {
    realProps.set("some.unknown.prop", "some.value");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldNotAllowUnknownConsumerPropertyToBeSet() {
    realProps.set(StreamsConfig.CONSUMER_PREFIX + "some.unknown.prop", "some.value");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldNotAllowUnknownProducerPropertyToBeSet() {
    realProps.set(StreamsConfig.PRODUCER_PREFIX + "some.unknown.prop", "some.value");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldNotAllowUnknownStreamsConfigToBeSet() {
    realProps.set(KaypherConfig.KAYPHER_STREAMS_PREFIX + "some.unknown.prop", "some.value");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldNotAllowUnknownKaypherConfigToBeSet() {
    realProps.set(KaypherConfig.KAYPHER_CONFIG_PROPERTY_PREFIX + "some.unknown.prop", "some.value");
  }

}