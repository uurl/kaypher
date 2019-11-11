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
package com.treutec.kaypher.function.udf;

import com.treutec.kaypher.function.FunctionInvoker;
import com.treutec.kaypher.security.ExtensionSecurityManager;
import java.util.Objects;

/**
 * Class to allow conversion from Kudf to UdfInvoker.
 * This may change if we ever get rid of Kudf. As it stands we need
 * to do a conversion from custom UDF -> Kudf so we can support strong
 * typing etc.
 */
public class PluggableUdf implements Kudf {

  private final FunctionInvoker udf;
  private final Object actualUdf;

  public PluggableUdf(
      final FunctionInvoker udfInvoker,
      final Object actualUdf
  ) {
    this.udf = Objects.requireNonNull(udfInvoker, "udfInvoker");
    this.actualUdf = Objects.requireNonNull(actualUdf, "actualUdf");
  }

  @Override
  public Object evaluate(final Object... args) {
    try {
      ExtensionSecurityManager.INSTANCE.pushInUdf();
      return udf.eval(actualUdf, args);
    } finally {
      ExtensionSecurityManager.INSTANCE.popOutUdf();
    }
  }

}
