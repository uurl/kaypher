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

package com.treutec.kaypher.parser.tree;

import com.treutec.kaypher.parser.NodeLocation;
import java.util.Objects;
import java.util.Optional;

public class DropConnector extends Statement {

  private final String connectorName;

  public DropConnector(final Optional<NodeLocation> location, final String connectorName) {
    super(location);
    this.connectorName = connectorName;
  }

  public String getConnectorName() {
    return connectorName;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final DropConnector that = (DropConnector) o;
    return Objects.equals(connectorName, that.connectorName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(connectorName);
  }

  @Override
  public String toString() {
    return "DropConnector{"
        + "connectorName='" + connectorName + '\''
        + '}';
  }
}
