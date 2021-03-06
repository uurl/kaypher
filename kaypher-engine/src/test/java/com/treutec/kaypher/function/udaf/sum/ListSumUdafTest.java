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

package com.treutec.kaypher.function.udaf.sum;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import com.treutec.kaypher.function.udaf.TableUdaf;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class ListSumUdafTest {

  @Test
  public void shouldSumLongList() {
    final TableUdaf<List<Long>, Long, Long>  udaf = ListSumUdaf.sumLongList();
    final Long[] values = new Long[] {1L, 1L, 1L, 1L, 1L};
    final List<Long> list = Arrays.asList(values);
    Long sum = udaf.aggregate(list, 0L);

    assertThat(5L, equalTo(sum));
  }

  @Test
  public void shouldSumIntList() {
    final TableUdaf<List<Integer>, Integer, Integer>  udaf = ListSumUdaf.sumIntList();
    final Integer[] values = new Integer[] {1, 1, 1, 1, 1};
    final List<Integer> list = Arrays.asList(values);
    Integer sum = udaf.aggregate(list, 0);

    assertThat(5, equalTo(sum));
  }

  @Test
  public void shouldSumDoubleList() {
    final TableUdaf<List<Double>, Double, Double>  udaf = ListSumUdaf.sumDoubleList();
    final Double[] values = new Double[] {1.0, 1.0, 1.0, 1.0, 1.0};
    final List<Double> list = Arrays.asList(values);
    Double sum = udaf.aggregate(list, 0.0);

    assertThat(5.0, equalTo(sum));
  }

  @Test
  public void shouldASumZeroes() {
    final TableUdaf<List<Integer>, Integer, Integer>  udaf = ListSumUdaf.sumIntList();
    final Integer[] values = new Integer[] {0, 0, 0, 0, 0};
    final List<Integer> list = Arrays.asList(values);
    Integer sum = udaf.aggregate(list, 0);

    assertThat(0, equalTo(sum));
  }

  @Test
  public void shouldSumEmpty() {
    final TableUdaf<List<Integer>, Integer, Integer>  udaf = ListSumUdaf.sumIntList();

    final int sum = udaf.aggregate(Collections.emptyList(), 0);

    assertThat(0, equalTo(sum));
  }

  @Test
  public void shouldIgnoreNull() {
    final TableUdaf<List<Integer>, Integer, Integer>  udaf = ListSumUdaf.sumIntList();
    final Integer[] values = new Integer[] {1, 1, null, 1};
    final List<Integer> list = Arrays.asList(values);
    Integer sum = udaf.aggregate(list, 0);
    
    assertThat(3, equalTo(sum));
  }

  @Test
  public void shouldMergeSums() {
    final TableUdaf<List<Integer>, Integer, Integer>  udaf = ListSumUdaf.sumIntList();

    final Integer[] leftValues = new Integer[] {1, 1, 1, 1};
    final List<Integer> leftList = Arrays.asList(leftValues);
    Integer sumLeft = udaf.aggregate(leftList, 0);

    final Integer[] rightValues = new Integer[] {2, 2, 2};
    final List<Integer> rightList = Arrays.asList(rightValues);
    Integer sumRight = udaf.aggregate(rightList, 0);


    final Integer merged = udaf.merge(sumLeft, sumRight);
    assertThat(10, equalTo(merged));
  }

  @Test
  public void shouldUndoSum() {
    final TableUdaf<List<Integer>, Integer, Integer>  udaf = ListSumUdaf.sumIntList();
    final Integer[] values = new Integer[] {1, 1, 1, 1};
    final List<Integer> list = Arrays.asList(values);
    Integer sum = udaf.aggregate(list, 0);

    final Integer[] undoValues = new Integer[] {1, 1, 1};
    final List<Integer> undoList = Arrays.asList(undoValues);
    int undo = udaf.undo(undoList, sum);

    assertThat(1, equalTo(undo));
  }

}
