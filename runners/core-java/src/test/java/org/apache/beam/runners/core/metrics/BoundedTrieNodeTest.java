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
package org.apache.beam.runners.core.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.*;
import java.util.Collections;
import java.util.stream.Collectors;
import org.apache.beam.runners.core.metrics.BoundedTrieData.BoundedTrieNode;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests for {@link BoundedTrieNode}. */
@RunWith(JUnit4.class)
public class BoundedTrieNodeTest {
  private static final Logger LOG = LoggerFactory.getLogger(BoundedTrieNodeTest.class);

  /**
   * Generates {@code n} random segments with a fixed depth.
   *
   * @param n The number of segments to generate.
   * @param depth The depth of each segment.
   * @param overlap The probability that a segment will share a prefix with a previous segment.
   * @param rand A random number generator.
   * @return A list of segments.
   */
  private static List<List<String>> generateSegmentsFixedDepth(
      int n, int depth, double overlap, Random rand) {
    if (depth == 0) {
      return Collections.nCopies(n, Collections.emptyList());
    } else {
      List<List<String>> result = new ArrayList<>();
      List<String> seen = new ArrayList<>();
      for (List<String> suffix : generateSegmentsFixedDepth(n, depth - 1, overlap, rand)) {
        String prefix;
        if (seen.isEmpty() || rand.nextDouble() > overlap) {
          prefix = String.valueOf((char) ('a' + seen.size()));
          seen.add(prefix);
        } else {
          prefix = seen.get(rand.nextInt(seen.size()));
        }
        List<String> newSegments = new ArrayList<>();
        newSegments.add(prefix);
        newSegments.addAll(suffix);
        result.add(newSegments);
      }
      return result;
    }
  }

  /**
   * Generates {@code n} random segments with a depth between {@code minDepth} and {@code maxDepth}.
   *
   * @param n The number of segments to generate.
   * @param minDepth The minimum depth of each segment.
   * @param maxDepth The maximum depth of each segment.
   * @param overlap The probability that a segment will share a prefix with a previous segment.
   * @param rand A random number generator.
   * @return A list of segments.
   */
  private static List<List<String>> randomSegments(
      int n, int minDepth, int maxDepth, double overlap, Random rand) {
    List<List<String>> result = new ArrayList<>();
    List<Integer> depths = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      depths.add(minDepth + (i % (maxDepth - minDepth + 1)));
    }
    Iterator<Integer> depthIterator = depths.iterator();
    for (List<String> segments : generateSegmentsFixedDepth(n, maxDepth, overlap, rand)) {
      int depth = depthIterator.next();
      result.add(segments.subList(0, depth));
    }
    return result;
  }

  /**
   * Asserts that the given {@link BoundedTrieNode} covers the expected segments.
   *
   * @param node The {@link BoundedTrieNode} to check.
   * @param expected The expected segments.
   * @param maxTruncated The maximum number of truncated segments allowed.
   */
  private void assertCovers(BoundedTrieNode node, Set<List<String>> expected, int maxTruncated) {
    assertCoversFlattened(node.flattened(), expected, maxTruncated);
  }

  /**
   * Verifies that the flattened list of segments covers the expected segments.
   *
   * @param flattened The flattened list of segments.
   * @param expected The expected segments.
   * @param maxTruncated The maximum number of truncated segments allowed.
   */
  private void assertCoversFlattened(
      List<List<String>> flattened, Set<List<String>> expected, int maxTruncated) {
    Set<List<String>> exactSegments = new HashSet<>();
    Set<List<String>> truncatedSegments = new HashSet<>();
    for (List<String> entry : flattened) {
      List<String> segments = new ArrayList<>(entry.subList(0, entry.size() - 1));
      String last = entry.get(entry.size() - 1);
      if (Boolean.parseBoolean(last)) {
        truncatedSegments.add(segments);
      } else {
        exactSegments.add(segments);
      }
    }

    assertTrue(
        "Exact segments set should not be larger than expected set",
        exactSegments.size() <= expected.size());
    assertTrue(
        "Expected set should contain all exact segments", expected.containsAll(exactSegments));

    assertTrue(
        "Number of truncated segments should not exceed maxTruncated",
        truncatedSegments.size() <= maxTruncated);

    Set<List<String>> seenTruncated = new HashSet<>();
    for (List<String> segments : expected) {
      if (!exactSegments.contains(segments)) {
        int found = 0;
        for (int i = 0; i < segments.size(); i++) {
          if (truncatedSegments.contains(segments.subList(0, i))) {
            seenTruncated.add(segments.subList(0, i));
            found++;
          }
        }
        assertEquals(
            String.format(
                "Expected exactly one prefix of %s to occur in %s, found %s",
                segments, truncatedSegments, found),
            1,
            found);
      }
    }

    assertEquals(
        "Seen truncated segments should match the truncated segments set",
        seenTruncated,
        truncatedSegments);
  }

  /**
   * Runs a test case for the {@link #assertCoversFlattened} method.
   *
   * @param flattened The flattened list of segments.
   * @param expected The expected segments.
   * @param maxTruncated The maximum number of truncated segments allowed.
   */
  private void runCoversTest(List<String> flattened, List<String> expected, int maxTruncated) {
    List<List<String>> parsedFlattened =
        flattened.stream()
            .map(
                s -> {
                  List<String> result =
                      new ArrayList<>(Arrays.asList(s.replace("*", "").split("")));
                  result.add(s.endsWith("*") ? Boolean.TRUE.toString() : Boolean.FALSE.toString());
                  return result;
                })
            .collect(Collectors.toList());
    Set<List<String>> parsedExpected =
        expected.stream().map(s -> Arrays.asList(s.split(""))).collect(Collectors.toSet());
    assertCoversFlattened(parsedFlattened, parsedExpected, maxTruncated);
  }

  /**
   * Runs a test case for the {@link BoundedTrieNode} class.
   *
   * @param toAdd The segments to add to the {@link BoundedTrieNode}.
   */
  private void runTest(List<List<String>> toAdd) {
    Set<List<String>> everything = new HashSet<>(toAdd);
    Set<List<String>> allPrefixes = new HashSet<>();
    for (List<String> segments : everything) {
      for (int i = 0; i < segments.size(); i++) {
        allPrefixes.add(segments.subList(0, i));
      }
    }
    Set<List<String>> everythingDeduped = new HashSet<>(everything);
    everythingDeduped.removeAll(allPrefixes);

    // Test basic addition.
    BoundedTrieNode node = new BoundedTrieNode();
    int initialSize = node.getSize();
    assertEquals(1, initialSize);

    int totalSize = initialSize;
    for (List<String> segments : everything) {
      totalSize += node.add(segments);
    }

    assertEquals(everythingDeduped.size(), node.getSize());
    assertEquals(totalSize, node.getSize());
    assertCovers(node, everythingDeduped, 0);

    // Test merging.
    BoundedTrieNode node0 = new BoundedTrieNode();
    BoundedTrieNode node1 = new BoundedTrieNode();
    int i = 0;
    for (List<String> segments : everything) {
      if (i % 2 == 0) {
        node0.add(segments);
      } else {
        node1.add(segments);
      }
      i++;
    }
    int preMergeSize = node0.getSize();
    int mergeDelta = node0.merge(node1);
    assertEquals(preMergeSize + mergeDelta, node0.getSize());
    assertEquals(node0, node);

    // Test trimming.
    int trimDelta = 0;
    if (node.getSize() > 1) {
      trimDelta = node.trim();
      assertTrue(trimDelta < 0);
      assertEquals(totalSize + trimDelta, node.getSize());
      assertCovers(node, everythingDeduped, 1);
    }

    if (node.getSize() > 1) {
      int trim2Delta = node.trim();
      assertTrue(trim2Delta < 0);
      assertEquals(totalSize + trimDelta + trim2Delta, node.getSize());
      assertCovers(node, everythingDeduped, 2);
    }

    // Verify adding after trimming is a no-op.
    BoundedTrieNode nodeCopy = new BoundedTrieNode(node);
    for (List<String> segments : everything) {
      assertEquals(0, node.add(segments));
    }
    assertEquals(node, nodeCopy);

    // Verify merging after trimming is a no-op.
    assertEquals(0, node.merge(node0));
    assertEquals(0, node.merge(node1));
    assertEquals(node, nodeCopy);

    // Test adding new values.
    int expectedDelta = node.isTruncated() ? 0 : 2;
    List<List<String>> newValues =
        Arrays.asList(Collections.singletonList("new1"), Arrays.asList("new2", "new2.1"));
    assertEquals(expectedDelta, node.addAll(newValues));

    Set<List<String>> expectedWithNewValues = new HashSet<>(everythingDeduped);
    expectedWithNewValues.addAll(newValues);
    assertCovers(node, expectedWithNewValues, 2);

    // Test merging new values.
    BoundedTrieNode newValuesNode = new BoundedTrieNode();
    newValuesNode.addAll(newValues);
    assertCovers(newValuesNode, new HashSet<>(newValues), 0);
    assertEquals(expectedDelta, nodeCopy.merge(newValuesNode));
    assertCovers(nodeCopy, expectedWithNewValues, 2);
    // adding after merge should not change previous node on which this was merged
    List<String> additionalValue = Arrays.asList("new3", "new3.1");
    expectedDelta = node.isTruncated() ? 0 : 1;
    assertEquals(expectedDelta, newValuesNode.add(additionalValue));
    // previous node on which the merge was done should have remained same
    assertCovers(nodeCopy, expectedWithNewValues, 2);
    // the newValuesNode should have changed
    Set<List<String>> updatedNewValues = new HashSet<>(newValues);
    updatedNewValues.add(additionalValue);
    assertCovers(newValuesNode, updatedNewValues, 0);
  }

  /**
   * Fuzzy segment generator for testing {@link BoundedTrieNode} class.
   *
   * @param iterations The number of iterations to run.
   * @param n The number of segments to generate for each iteration.
   * @param minDepth The minimum depth of each segment.
   * @param maxDepth The maximum depth of each segment.
   * @param overlap The probability that a segment will share a prefix with a previous segment.
   */
  private void runFuzz(int iterations, int n, int minDepth, int maxDepth, double overlap) {
    for (int i = 0; i < iterations; i++) {
      long seed = new Random().nextLong();
      Random rand = new Random(seed);
      List<List<String>> segments = randomSegments(n, minDepth, maxDepth, overlap, rand);
      try {
        runTest(segments);
      } catch (AssertionError e) {
        LOG.info("SEED: {}", seed);
        throw e;
      }
    }
  }

  @Test
  public void testTrivial() {
    runTest(Arrays.asList(Arrays.asList("a", "b"), Arrays.asList("a", "c")));
  }

  @Test
  public void testFlat() {
    runTest(
        Arrays.asList(Arrays.asList("a", "a"), Arrays.asList("b", "b"), Arrays.asList("c", "c")));
  }

  @Test
  public void testDeep() {
    runTest(Arrays.asList(Collections.nCopies(10, "a"), Collections.nCopies(12, "b")));
  }

  @Test
  public void testSmall() {
    runFuzz(10, 5, 2, 3, 0.5);
  }

  @Test
  public void testMedium() {
    runFuzz(10, 20, 2, 4, 0.5);
  }

  @Test
  public void testLargeSparse() {
    runFuzz(10, 120, 2, 4, 0.2);
  }

  @Test
  public void testLargeDense() {
    runFuzz(10, 120, 2, 4, 0.8);
  }

  @Test
  public void testCoversExact() {
    runCoversTest(Arrays.asList("ab", "ac", "cd"), Arrays.asList("ab", "ac", "cd"), 0);

    assertThrows(
        AssertionError.class,
        () -> runCoversTest(Arrays.asList("ab", "ac", "cd"), Arrays.asList("ac", "cd"), 0));
    assertThrows(
        AssertionError.class,
        () -> runCoversTest(Arrays.asList("ab", "ac"), Arrays.asList("ab", "ac", "cd"), 0));
    assertThrows(
        AssertionError.class,
        () -> runCoversTest(Arrays.asList("a*", "cd"), Arrays.asList("ab", "ac", "cd"), 0));
  }

  @Test
  public void testCoversTruncated() {
    runCoversTest(Arrays.asList("a*", "cd"), Arrays.asList("ab", "ac", "cd"), 1);
    runCoversTest(Arrays.asList("a*", "cd"), Arrays.asList("ab", "ac", "abcde", "cd"), 1);

    assertThrows(
        AssertionError.class,
        () -> runCoversTest(Arrays.asList("ab", "ac", "cd"), Arrays.asList("ac", "cd"), 1));
    assertThrows(
        AssertionError.class,
        () -> runCoversTest(Arrays.asList("ab", "ac"), Arrays.asList("ab", "ac", "cd"), 1));
    assertThrows(
        AssertionError.class,
        () -> runCoversTest(Arrays.asList("a*", "c*"), Arrays.asList("ab", "ac", "cd"), 1));
    assertThrows(
        AssertionError.class,
        () -> runCoversTest(Arrays.asList("a*", "c*"), Arrays.asList("ab", "ac"), 1));
  }

  @Test
  public void testBoundedTrieDataCombine() {
    BoundedTrieData empty = BoundedTrieData.empty();
    BoundedTrieData singletonA = BoundedTrieData.create(ImmutableList.of("a", "a"));
    BoundedTrieData singletonB = BoundedTrieData.create(ImmutableList.of("b", "b"));
    BoundedTrieNode lotsRoot = new BoundedTrieNode();
    lotsRoot.addAll(Arrays.asList(Arrays.asList("c", "c"), Arrays.asList("d", "d")));
    BoundedTrieData lots = BoundedTrieData.create(lotsRoot);

    assertEquals(Collections.emptySet(), empty.getBoundedTrieResult());
    assertEquals(
        ImmutableSet.of(Arrays.asList("a", "a", String.valueOf(false))),
        empty.combine(singletonA).getBoundedTrieResult());
    assertEquals(
        ImmutableSet.of(Arrays.asList("a", "a", String.valueOf(false))),
        singletonA.combine(empty).getBoundedTrieResult());
    assertEquals(
        ImmutableSet.of(
            Arrays.asList("a", "a", String.valueOf(false)),
            Arrays.asList("b", "b", String.valueOf(false))),
        singletonA.combine(singletonB).getBoundedTrieResult());
    assertEquals(
        ImmutableSet.of(
            Arrays.asList("a", "a", String.valueOf(false)),
            Arrays.asList("c", "c", String.valueOf(false)),
            Arrays.asList("d", "d", String.valueOf(false))),
        singletonA.combine(lots).getBoundedTrieResult());
    assertEquals(
        ImmutableSet.of(
            Arrays.asList("a", "a", String.valueOf(false)),
            Arrays.asList("c", "c", String.valueOf(false)),
            Arrays.asList("d", "d", String.valueOf(false))),
        lots.combine(singletonA).getBoundedTrieResult());
  }

  @Test
  public void testBoundedTrieDataCombineTrim() {
    BoundedTrieNode left = new BoundedTrieNode();
    left.addAll(Arrays.asList(Arrays.asList("a", "x"), Arrays.asList("b", "d")));
    BoundedTrieNode right = new BoundedTrieNode();
    right.addAll(Arrays.asList(Arrays.asList("a", "y"), Arrays.asList("c", "d")));

    BoundedTrieData combined =
        BoundedTrieData.create(left).combine(BoundedTrieData.create(null, right, 3));

    assertEquals(
        ImmutableSet.of(
            Arrays.asList("a", String.valueOf(true)),
            Arrays.asList("b", "d", String.valueOf(false)),
            Arrays.asList("c", "d", String.valueOf(false))),
        combined.getBoundedTrieResult());
  }
}
