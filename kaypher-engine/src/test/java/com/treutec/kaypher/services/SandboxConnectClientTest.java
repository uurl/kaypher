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

package com.treutec.kaypher.services;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.treutec.kaypher.metastore.model.MetaStoreMatchers.OptionalMatchers;
import com.treutec.kaypher.services.ConnectClient.ConnectResponse;
import java.util.List;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.junit.Before;
import org.junit.Test;

public class SandboxConnectClientTest {

  private ConnectClient sandboxClient;

  @Before
  public void setUp() {
    sandboxClient = SandboxConnectClient.createProxy();
  }

  @Test
  public void shouldReturnErrorOnCreate() {
    // When:
    final ConnectResponse<ConnectorInfo> foo = sandboxClient.create("foo", ImmutableMap.of());

    // Then:
    assertThat(foo.error(), OptionalMatchers.of(is("sandbox")));
    assertThat("expected no datum", !foo.datum().isPresent());
  }

  @Test
  public void shouldReturnEmptyListOnList() {
    // When:
    final ConnectResponse<List<String>> foo = sandboxClient.connectors();

    // Then:
    assertThat(foo.datum(), OptionalMatchers.of(is(ImmutableList.of())));
    assertThat("expected no error", !foo.error().isPresent());
  }

  @Test
  public void shouldReturnErrorOnDescribe() {
    // When:
    final ConnectResponse<ConnectorInfo> foo = sandboxClient.describe("foo");

    // Then:
    assertThat(foo.error(), OptionalMatchers.of(is("sandbox")));
    assertThat("expected no datum", !foo.datum().isPresent());
  }

  @Test
  public void shouldReturnErrorOnStatus() {
    // When:
    final ConnectResponse<ConnectorStateInfo> foo = sandboxClient.status("foo");

    // Then:
    assertThat(foo.error(), OptionalMatchers.of(is("sandbox")));
    assertThat("expected no datum", !foo.datum().isPresent());
  }

}