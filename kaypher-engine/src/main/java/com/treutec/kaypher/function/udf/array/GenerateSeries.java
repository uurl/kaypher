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

package com.treutec.kaypher.function.udf.array;

import com.treutec.kaypher.function.KaypherFunctionException;
import com.treutec.kaypher.function.udf.Udf;
import com.treutec.kaypher.function.udf.UdfDescription;
import com.treutec.kaypher.function.udf.UdfParameter;
import java.util.ArrayList;
import java.util.List;

/**
 * This UDF constructs an array containing an array of INTs or BIGINTs in the specified range
 */
@UdfDescription(name = "GENERATE_SERIES", description = "Construct an array of a range of values")
public class GenerateSeries {

  @Udf
  public List<Integer> generateSeriesInt(
      @UdfParameter(description = "The beginning of the series") final int start,
      @UdfParameter(description = "Marks the end of the series (inclusive)") final int end
  ) {
    return generateSeriesInt(start, end, end - start > 0 ? 1 : -1);
  }

  @Udf
  public List<Integer> generateSeriesInt(
      @UdfParameter(description = "The beginning of the series") final int start,
      @UdfParameter(description = "Marks the end of the series (inclusive)") final int end,
      @UdfParameter(description = "Difference between each value in the series") final int step
  ) {
    checkStep(step);
    final int diff = end - start;
    if (diff > 0 && step < 0 || diff < 0 && step > 0) {
      throw new KaypherFunctionException("GENERATE_SERIES step has wrong sign");
    }
    final int size = 1 + diff / step;
    final List<Integer> result = new ArrayList<>(size);
    int pos = 0;
    int val = start;
    while (pos++ < size) {
      result.add(val);
      val += step;
    }
    return result;
  }

  @Udf
  public List<Long> generateSeriesLong(
      @UdfParameter(description = "The beginning of the series") final long start,
      @UdfParameter(description = "Marks the end of the series (inclusive)") final long end
  ) {
    return generateSeriesLong(start, end, end - start > 0 ? 1 : -1);
  }

  @Udf
  public List<Long> generateSeriesLong(
      @UdfParameter(description = "The beginning of the series") final long start,
      @UdfParameter
          (description = "Marks the end of the series (inclusive)") final long end,
      @UdfParameter(description = "Difference between each value in the series") final int step
  ) {
    checkStep(step);
    final long diff = end - start;
    if (diff > 0 && step < 0 || diff < 0 && step > 0) {
      throw new KaypherFunctionException("GENERATE_SERIES step has wrong sign");
    }
    final int size = 1 + (int) (diff / step);
    final List<Long> result = new ArrayList<>(size);
    int pos = 0;
    long val = start;
    while (pos++ < size) {
      result.add(val);
      val += step;
    }
    return result;
  }

  private void checkStep(final int step) {
    if (step == 0) {
      throw new KaypherFunctionException("GENERATE_SERIES step cannot be zero");
    }
  }

}