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
package com.treutec.kaypher.ddl.commands;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import com.treutec.kaypher.execution.ddl.commands.RegisterTypeCommand;
import com.treutec.kaypher.execution.expression.tree.Type;
import com.treutec.kaypher.parser.tree.RegisterType;
import com.treutec.kaypher.schema.kaypher.SqlBaseType;
import com.treutec.kaypher.schema.kaypher.types.SqlPrimitiveType;
import com.treutec.kaypher.schema.kaypher.types.SqlStruct;
import java.util.Optional;
import org.junit.Test;

public class RegisterTypeFactoryTest {
  private final RegisterTypeFactory factory = new RegisterTypeFactory();

  @Test
  public void shouldCreateCommandForRegisterType() {
    // Given:
    final RegisterType ddlStatement = new RegisterType(
        Optional.empty(),
        "alias",
        new Type(SqlStruct.builder().field("foo", SqlPrimitiveType.of(SqlBaseType.STRING)).build())
    );

    // When:
    final RegisterTypeCommand result = factory.create(ddlStatement);

    // Then:
    assertThat(result.getType(), equalTo(ddlStatement.getType().getSqlType()));
    assertThat(result.getName(), equalTo("alias"));
  }
}
