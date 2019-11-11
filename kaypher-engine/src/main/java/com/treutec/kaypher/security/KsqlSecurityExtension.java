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
import com.treutec.kaypher.util.KaypherException;
import java.util.Optional;

/**
 * This interface provides a security extension (or plugin) to the KAYPHER server in order to
 * protect the access to REST, Websockets, and other internal KAYPHER resources.
 * </p>
 * The type of security details provided is left to the implementation class. This class is
 * called only at specific registration points to let the implementation know when to register
 * a specific security plugin.
 */
public interface KaypherSecurityExtension extends AutoCloseable {
  /**
   * Initializes any implementation-specific resources for the security extension.
   *
   * @param kaypherConfig The KAYPHER configuration object
   */
  void initialize(KaypherConfig kaypherConfig);

  /**
   * Returns the authorization provider used to verify access permissions on KAYPHER resources.
   * </p>
   * If no authorization is required/enabled for KAYPHER, then an {@code Optional.empty()} object must
   * be returned.
   *
   * @return The (Optional) provider used for authorization requests.
   */
  Optional<KaypherAuthorizationProvider> getAuthorizationProvider();

  /**
   * Returns the {@link KaypherUserContextProvider} used to access to service clients that may be
   * run in the context of a user making REST requests.
   * </p>
   * If an empty Optional object is returned, KAYPHER marks this function as disabled.
   * </p>
   * If a non-empty object is returned, KAYPHER marks this function as enabled.
   * </p>
   * Note: This context is used only for non-persistent commands.
   **
   * @return The {@code KaypherUserContextProvider} object. The context is optional, and if an empty
   *         object is found, then KAYPHER will disable the user context functionality.
   */
  Optional<KaypherUserContextProvider> getUserContextProvider();

  /**
   * Closes the current security extension. This is called in case the implementation requires
   * to clean any security data in memory, files, and/or close connections to external security
   * services.
   *
   * @throws KaypherException If an error occurs while closing the security extension.
   */
  @Override
  void close();
}
