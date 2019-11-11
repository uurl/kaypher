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

import static com.treutec.kaypher.util.LimitedProxyBuilder.methodParams;

import com.google.common.collect.ImmutableList;
import com.treutec.kaypher.services.ConnectClient.ConnectResponse;
import com.treutec.kaypher.util.LimitedProxyBuilder;
import java.util.Map;
import org.apache.http.HttpStatus;

/**
 * Supplies {@link ConnectClient}s to use that do not make any
 * state changes to the external connect clusters.
 */
final class SandboxConnectClient {

  private SandboxConnectClient() { }

  public static ConnectClient createProxy() {
    return LimitedProxyBuilder.forClass(ConnectClient.class)
        .swallow("create", methodParams(String.class, Map.class),
            ConnectResponse.failure("sandbox", HttpStatus.SC_INTERNAL_SERVER_ERROR))
        .swallow("describe", methodParams(String.class),
            ConnectResponse.failure("sandbox", HttpStatus.SC_INTERNAL_SERVER_ERROR))
        .swallow("connectors", methodParams(),
            ConnectResponse.success(ImmutableList.of(), HttpStatus.SC_OK))
        .swallow("status", methodParams(String.class),
            ConnectResponse.failure("sandbox", HttpStatus.SC_INTERNAL_SERVER_ERROR))
        .swallow("delete", methodParams(String.class),
            ConnectResponse.success("sandbox", HttpStatus.SC_NO_CONTENT))
        .build();
  }
}
