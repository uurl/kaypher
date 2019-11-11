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

package com.treutec.kaypher.schema.kaypher;

import com.treutec.kaypher.execution.expression.tree.Type;
import com.treutec.kaypher.metastore.TypeRegistry;
import com.treutec.kaypher.parser.CaseInsensitiveStream;
import com.treutec.kaypher.parser.NodeLocation;
import com.treutec.kaypher.parser.SqlBaseLexer;
import com.treutec.kaypher.parser.SqlBaseParser;
import com.treutec.kaypher.parser.SqlBaseParser.TypeContext;
import com.treutec.kaypher.schema.kaypher.types.SqlArray;
import com.treutec.kaypher.schema.kaypher.types.SqlDecimal;
import com.treutec.kaypher.schema.kaypher.types.SqlMap;
import com.treutec.kaypher.schema.kaypher.types.SqlPrimitiveType;
import com.treutec.kaypher.schema.kaypher.types.SqlStruct;
import com.treutec.kaypher.schema.kaypher.types.SqlType;
import com.treutec.kaypher.util.KaypherException;
import com.treutec.kaypher.util.ParserUtil;
import java.util.Objects;
import java.util.Optional;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;

public final class SqlTypeParser {

  private final TypeRegistry typeRegistry;

  public static SqlTypeParser create(final TypeRegistry typeRegistry) {
    return new SqlTypeParser(typeRegistry);
  }

  private SqlTypeParser(final TypeRegistry typeRegistry) {
    this.typeRegistry = Objects.requireNonNull(typeRegistry, "typeRegistry");
  }

  public Type parse(final String schema) {
    final TypeContext typeContext = parseTypeContext(schema);
    return getType(typeContext);
  }

  public Type getType(
      final SqlBaseParser.TypeContext type
  ) {
    final Optional<NodeLocation> location = ParserUtil.getLocation(type);
    final SqlType sqlType = getSqlType(type);
    return new Type(location, sqlType);
  }

  private SqlType getSqlType(final SqlBaseParser.TypeContext type) {
    if (type.baseType() != null) {
      final String baseType = baseTypeToString(type.baseType());
      if (SqlPrimitiveType.isPrimitiveTypeName(baseType)) {
        return SqlPrimitiveType.of(baseType);
      } else {
        return typeRegistry
            .resolveType(baseType)
            .orElseThrow(() -> new KaypherException("Cannot resolve unknown type: " + baseType));
      }
    }

    if (type.DECIMAL() != null) {
      return SqlDecimal.of(
          ParserUtil.processIntegerNumber(type.number(0), "DECIMAL(PRECISION)"),
          ParserUtil.processIntegerNumber(type.number(1), "DECIMAL(SCALE)")
      );
    }

    if (type.ARRAY() != null) {
      return SqlArray.of(getSqlType(type.type(0)));
    }

    if (type.MAP() != null) {
      return SqlMap.of(getSqlType(type.type(1)));
    }

    if (type.STRUCT() != null) {
      final SqlStruct.Builder builder = SqlStruct.builder();

      for (int i = 0; i < type.identifier().size(); i++) {
        final String fieldName = ParserUtil.getIdentifierText(type.identifier(i));
        final SqlType fieldType = getSqlType(type.type(i));
        builder.field(fieldName, fieldType);
      }
      return builder.build();
    }

    throw new IllegalArgumentException("Unsupported type specification: " + type.getText());
  }

  private static TypeContext parseTypeContext(final String schema) {
    final SqlBaseLexer lexer = new SqlBaseLexer(
        new CaseInsensitiveStream(CharStreams.fromString(schema)));
    final CommonTokenStream tokenStream = new CommonTokenStream(lexer);
    final SqlBaseParser parser = new SqlBaseParser(tokenStream);
    parser.getInterpreter().setPredictionMode(PredictionMode.LL);
    return parser.type();
  }

  private static String baseTypeToString(final SqlBaseParser.BaseTypeContext baseType) {
    if (baseType.identifier() != null) {
      return ParserUtil.getIdentifierText(baseType.identifier());
    } else {
      throw new KaypherException(
          "Base type must contain either identifier, "
              + "time with time zone, or timestamp with time zone"
      );
    }
  }
}
