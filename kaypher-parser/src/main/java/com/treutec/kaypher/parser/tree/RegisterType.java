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

import com.treutec.kaypher.execution.expression.tree.Type;
import com.treutec.kaypher.parser.NodeLocation;
import java.util.Objects;
import java.util.Optional;

public class RegisterType extends Statement implements ExecutableDdlStatement {

  private final Type type;
  private final String name;

  public RegisterType(final Optional<NodeLocation> location, final String name, final Type type) {
    super(location);
    this.name = Objects.requireNonNull(name, "name");
    this.type = Objects.requireNonNull(type, "type");
  }

  public Type getType() {
    return type;
  }

  public String getName() {
    return name;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitRegisterType(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final RegisterType that = (RegisterType) o;
    return Objects.equals(type, that.type)
        && Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, name);
  }

  @Override
  public String toString() {
    return "RegisterType{"
        + "type=" + type
        + ", name='" + name + '\''
        + '}';
  }
}
