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

import org.apache.kafka.connect.data.Struct;

@UdfDescription(name="test_udf", description = "test")
@SuppressWarnings("unused")
public class TestUdf {

  @Udf(description = "returns the method name")
  public String doStuffIntString(final int arg1, final String arg2) {
    return "doStuffIntString";
  }

  @Udf(description = "returns the method name")
  public String doStuffLongString(final long arg1, final String arg2) {
    return "doStuffLongString";
  }

  @Udf(description = "returns the method name")
  public String doStuffLongLongString(final long arg1, final long arg2, final String arg3) {
    return "doStuffLongLongString";
  }

  @Udf(description = "returns method name")
  public String doStuffLongVarargs(final long... longs) {
    return "doStuffLongVarargs";
  }

  @Udf(description = "returns the value of 'A'")
  public String doStuffStruct(
      @UdfParameter(schema = "STRUCT<A VARCHAR>") final Struct struct
  ) {
    return struct.getString("A");
  }
}
