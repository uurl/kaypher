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

import static com.treutec.kaypher.util.LimitedProxyBuilder.anyParams;

import com.treutec.kaypher.util.LimitedProxyBuilder;
import org.apache.kafka.clients.admin.Admin;

/**
 * An admin client to use while trying out operations.
 *
 * <p>The client will not make changes to the remote Kafka cluster.
 *
 * <p>Most operations result in a {@code UnsupportedOperationException} being thrown as they are
 * not called.
 */
final class SandboxedAdminClient {

  static Admin createProxy() {
    return LimitedProxyBuilder.forClass(Admin.class)
        .swallow("close", anyParams())
        .build();
  }

  private SandboxedAdminClient() {
  }
}
