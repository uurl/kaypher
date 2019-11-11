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
package com.treutec.kaypher.security;

import com.treutec.kaypher.util.KaypherConfig;
import java.util.Optional;

/**
 * This is the default security extension for KAYPHER. For now, this class is just a dummy
 * implementation of the {@link KaypherSecurityExtension} interface.
 */
public class KaypherDefaultSecurityExtension implements KaypherSecurityExtension {

  @Override
  public void initialize(final KaypherConfig kaypherConfig) {
  }

  @Override
  public Optional<KaypherAuthorizationProvider> getAuthorizationProvider() {
    return Optional.empty();
  }

  @Override
  public Optional<KaypherUserContextProvider> getUserContextProvider() {
    return Optional.empty();
  }

  @Override
  public void close() {
  }
}
