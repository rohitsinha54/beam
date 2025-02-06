/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.metrics;

import static org.junit.Assert.assertEquals;

import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Lineage}. */
@RunWith(JUnit4.class)
public class LineageTest {
  @Test
  public void testGetFqName() {
    Map<String, String> testCases =
        ImmutableMap.<String, String>builder()
            .put("apache-beam", "apache-beam")
            .put("`apache-beam`", "```apache-beam```")
            .put("apache.beam", "`apache.beam`")
            .put("apache:beam", "`apache:beam`")
            .put("apache beam", "`apache beam`")
            .put("apache`beam", "`apache``beam`")
            .put("apache` beam", "`apache`` beam`")
            .put("`apache-beam", "```apache-beam`")
            .put("apache-beam`", "`apache-beam```")
            .put("`apache beam`", "```apache beam```")
            .put("apache\tbeam", "`apache\tbeam`")
            .put("apache\nbeam", "`apache\nbeam`")
            .build();
    testCases.forEach(
        (key, value) -> {
          Iterator<String> fqnPartsIterator =
              Lineage.getFQNParts("apache", null, ImmutableList.of(key), null);
          String fqnPartsString = getFqnPartsString(fqnPartsIterator);
          assertEquals("apache:" + value, fqnPartsString);
        });
    testCases.forEach(
        (key, value) -> {
          Iterator<String> fqnPartsIterator =
              Lineage.getFQNParts("apache", "beam", ImmutableList.of(key), null);
          String fqnPartsString = getFqnPartsString(fqnPartsIterator);
          assertEquals("apache:beam:" + value, fqnPartsString);
        });
    testCases.forEach(
        (key, value) -> {
          Iterator<String> fqnPartsIterator =
              Lineage.getFQNParts("apache", "beam", ImmutableList.of(key, key), null);
          String fqnPartsString = getFqnPartsString(fqnPartsIterator);
          assertEquals("apache:beam:" + value + "." + value, fqnPartsString);
        });
  }

  private static String getFqnPartsString(Iterator<String> fqnPartsIterator) {
    return StreamSupport.stream(
            java.util.Spliterators.spliteratorUnknownSize(
                fqnPartsIterator, java.util.Spliterator.ORDERED),
            false)
        .collect(Collectors.joining(""));
  }
}
