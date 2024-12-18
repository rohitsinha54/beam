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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.MetricsApi.BoundedTrie;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;

/**
 * Experimental and subject to incompatible changes, or even removal, in a future releases.
 *
 * <p>Represents data stored in a bounded trie. This data structure is used to efficiently store and
 * aggregate a collection of string sequences, paths/FQN with a limited size.
 *
 * <p>The trie can be in one of two states:
 *
 * <p>This class is thread-safe.
 */
@Internal
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class BoundedTrieData implements Serializable {

  private static final int DEFAULT_BOUND = 100; // Default maximum size of the trie

  /**
   * Returns an {@link Optional} containing the singleton path if this {@link BoundedTrieData}
   * represents a single path.
   */
  private List<String> singleton;

  /**
   * Returns an {@link Optional} containing the root {@link BoundedTrieNode} if this {@link
   * BoundedTrieData} represents a trie.
   */
  private BoundedTrieNode root;

  /** Returns the maximum size of the trie. */
  private int bound;

  public BoundedTrieData() {
    this(null, null, DEFAULT_BOUND);
  }

  public BoundedTrieData(List<String> singleton) {
    this(singleton, null, DEFAULT_BOUND);
  }

  public BoundedTrieData(BoundedTrieNode root) {
    this(null, root, DEFAULT_BOUND);
  }

  public BoundedTrieData(List<String> singleton, BoundedTrieNode root, int bound) {
    assert singleton == null || root == null;
    this.singleton = singleton;
    this.root = root;
    this.bound = bound;
  }

  /** Converts this {@link BoundedTrieData} to its proto {@link BoundedTrie}. */
  public BoundedTrie toProto() {
    BoundedTrie.Builder builder = BoundedTrie.newBuilder();
    builder.setBound(this.bound);
    if (this.singleton != null) {
      builder.addAllSingleton(this.singleton);
    }
    if (this.root != null) {
      builder.setRoot(this.root.toProto());
    }
    return builder.build();
  }

  /** Creates a {@link BoundedTrieData} instance from its proto {@link BoundedTrie}. */
  public static BoundedTrieData fromProto(BoundedTrie proto) {
    List<String> singleton = proto.getSingletonList();
    BoundedTrieNode root = proto.hasRoot() ? BoundedTrieNode.fromProto(proto.getRoot()) : null;
    return new BoundedTrieData(singleton, root, proto.getBound());
  }

  /** Returns this {@link BoundedTrieData} as a {@link BoundedTrieNode}. */
  @Nonnull
  private BoundedTrieNode asTrie() {
    if (this.root != null) {
      return this.root;
    } else {
      BoundedTrieNode root = new BoundedTrieNode();
      if (this.singleton != null) {
        root.add(this.singleton);
      }
      return root;
    }
  }

  /** Returns a new {@link BoundedTrieData} instance that is a deep copy of this instance. */
  public synchronized BoundedTrieData getCumulative() {
    List<String> singleton = this.singleton == null ? null : new ArrayList<>(this.singleton);
    // deep copy
    BoundedTrieNode root = this.root == null ? null : new BoundedTrieNode(this.root);
    return new BoundedTrieData(singleton, root, this.bound);
  }

  /**
   * Returns an immutable set of lists, where each list represents a path in the bounded trie. The
   * last element in each path is a boolean in string representation denoting whether this path was
   * truncated. i.e. <["a", "b", "false"], ["c", "true"]>
   *
   * @return The set of paths.
   */
  public synchronized Set<List<String>> getBoundedTrieResult() {
    if (this.root == null) {
      if (this.singleton == null) {
        return ImmutableSet.of();
      } else {
        List<String> list = new ArrayList<>(this.singleton);
        list.add(String.valueOf(false));
        return ImmutableSet.of(list);
      }
    } else {
      return ImmutableSet.copyOf(this.root.flattened());
    }
  }

  /**
   * Adds a new path to this {@link BoundedTrieData} and hence the {@link BoundedTrieData} is
   * modified.
   *
   * @param segments The path to add.
   */
  public synchronized void add(@Nonnull Iterable<String> segments) {
    List<String> segmentsParts = ImmutableList.copyOf(segments);
    if (this.root == null && this.singleton == null) {
      this.singleton = segmentsParts;
    } else if (this.singleton != null && this.singleton.equals(segmentsParts)) {
    } else {
      if (this.root == null) {
        this.root = this.asTrie();
        // since now, we are setting the root clean up singleton
        this.singleton = null;
      }
      this.root.add(segmentsParts);
      if (this.root != null) {
        if (this.root.getSize() > this.bound) {
          this.root.trim();
        }
      }
    }
  }

  /**
   * Combines this {@link BoundedTrieData} with another {@link BoundedTrieData}.
   *
   * @param other The other {@link BoundedTrieData} to combine with.
   */
  public synchronized void combine(@Nonnull BoundedTrieData other) {
    if (other.root == null && other.singleton == null) {
      return;
    }
    if (this.root == null && this.singleton == null) {
      this.root = other.root == null ? null : new BoundedTrieNode(other.root); // deep copy
      this.singleton = other.singleton == null ? null : new ArrayList<>(other.singleton);
      this.bound = other.bound;
      return;
    }
    BoundedTrieNode combined = new BoundedTrieNode(this.asTrie());
    if (other.root != null) {
      combined.merge(other.root);
    } else {
      combined.add(other.singleton);
    }
    this.bound = Math.min(this.bound, other.bound);
    while (combined.getSize() > this.bound) {
      combined.trim();
    }
    this.root = combined;
    this.singleton = null;
  }

  /**
   * Returns the number of paths stored in this trie.
   *
   * @return The size of the trie.
   */
  public synchronized int size() {
    if (this.singleton != null) {
      return 1;
    } else if (this.root != null) {
      return root.getSize();
    } else {
      return 0;
    }
  }

  public void clear() {
    this.root = null;
    this.singleton = null;
    this.bound = DEFAULT_BOUND;
  }

  /**
   * Checks if the trie contains the given path.
   *
   * @param value The path to check.
   * @return True if the trie contains the path, false otherwise.
   */
  public boolean contains(@Nonnull List<String> value) {
    if (this.singleton != null) {
      return value.equals(this.singleton);
    } else if (this.root != null) {
      return this.root.contains(value);
    } else {
      return false;
    }
  }

  @Override
  public final boolean equals(@Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || this.getClass() != other.getClass()) {
      return false;
    }
    BoundedTrieData that = (BoundedTrieData) other;
    return this.asTrie().equals(that.asTrie());
  }

  @Override
  public final int hashCode() {
    return this.asTrie().hashCode();
  }

  @Override
  public final String toString() {
    return "BoundedTrieData(" + this.asTrie() + ")";
  }

  // ------------------------------ BoundedTrieNode Implementation ------------------------------

  /**
   * BoundedTrieNode implementation. This class is not thread-safe and relies on the {@link
   * BoundedTrieData} which uses this class to ensure thread-safety by acquiring a lock on the root
   * of the tree itself. This avoids acquiring and release N nodes in a path. This class is not
   * intended to be used directly outside of {@link BoundedTrieData} with multiple threads.
   */
  static class BoundedTrieNode implements Serializable {
    /**
     * A map from strings to child nodes. Each key represents a segment of a path/FQN, and the
     * corresponding value represents the subtree rooted at that segment.
     */
    private Map<String, BoundedTrieNode> children;

    /**
     * A flag indicating whether this node has been truncated. A truncated node represents an
     * aggregation/roll-up of multiple paths that share a common prefix.
     */
    private boolean truncated;

    /**
     * The size of the subtree rooted at this node. This represents the number of distinct paths
     * that pass through this node.
     */
    private int size;

    /** Constructs an empty `BoundedTrieNode` with size 1 and not truncated. */
    BoundedTrieNode() {
      this(new HashMap<>(), false, 1);
    }

    /**
     * Constructs a `BoundedTrieNode` with the given children, truncation status, and size.
     *
     * @param children The children of this node.
     * @param truncated Whether this node is truncated.
     * @param size The size of the subtree rooted at this node.
     */
    BoundedTrieNode(@Nonnull Map<String, BoundedTrieNode> children, boolean truncated, int size) {
      this.children = children;
      this.size = size;
      this.truncated = truncated;
    }

    /**
     * Constructs a deep copy of the given `BoundedTrieNode`.
     *
     * @param other The node to copy.
     */
    public BoundedTrieNode(BoundedTrieNode other) {
      assert other != null;
      this.truncated = other.truncated;
      this.size = other.size;
      this.children = new HashMap<>();
      // deep copy
      other.children.forEach((key, value) -> children.put(key, new BoundedTrieNode(value)));
    }

    /**
     * Adds a path represented by the given list of segments to this trie.
     *
     * @param segments The segments of the path to add.
     * @return The change in the size of the subtree rooted at this node.
     */
    int add(List<String> segments) {
      if (truncated || segments.isEmpty()) {
        return 0;
      }
      String head = segments.get(0);
      List<String> tail = segments.subList(1, segments.size());
      boolean wasEmpty = children.isEmpty();
      BoundedTrieNode currChild = children.get(head);
      int delta = 0;
      if (currChild == null) {
        currChild = new BoundedTrieNode();
        children.put(head, currChild);
        delta = wasEmpty ? 0 : 1;
      }
      if (!tail.isEmpty()) {
        delta += currChild.add(tail);
      }
      size += delta;
      return delta;
    }

    /**
     * Adds multiple paths to this trie.
     *
     * @param segmentsIter An iterator over the paths to add.
     * @return The total change in the size of the subtree rooted at this node.
     */
    int addAll(List<List<String>> segmentsIter) {
      return segmentsIter.stream().mapToInt(this::add).sum();
    }

    /**
     * Trims this trie by truncating the largest subtree.
     *
     * @return The change in the size of the subtree rooted at this node.
     */
    int trim() {
      if (children.isEmpty()) {
        return 0;
      }
      BoundedTrieNode maxChild =
          Collections.max(children.values(), Comparator.comparingInt(BoundedTrieNode::getSize));
      int delta;
      if (maxChild.size == 1) {
        delta = 1 - size;
        truncated = true;
        children = new HashMap<>();
      } else {
        delta = maxChild.trim();
      }
      size += delta;
      return delta;
    }

    /**
     * Merges the given `BoundedTrieNode` into this node and as a result this node is changed.
     *
     * @param other The node to merge.
     * @return The change in the size of the subtree rooted at this node.
     */
    int merge(BoundedTrieNode other) {
      if (truncated) {
        return 0;
      }
      if (other.truncated) {
        truncated = true;
        children = new HashMap<>();
        int delta = 1 - size;
        size += delta;
        return delta;
      }
      if (other.children.isEmpty()) {
        return 0;
      }
      if (children.isEmpty()) {
        children = new HashMap<>();
        other.children.forEach((key, value) -> children.put(key, new BoundedTrieNode(value)));
        int delta = this.size - other.size;
        size += delta;
        return delta;
      }
      int delta = 0;
      for (Map.Entry<String, BoundedTrieNode> entry : other.children.entrySet()) {
        String prefix = entry.getKey();
        BoundedTrieNode otherChild = entry.getValue();
        BoundedTrieNode thisChild = children.get(prefix);
        if (thisChild == null) {
          children.put(prefix, new BoundedTrieNode(otherChild));
          delta += otherChild.size;
        } else {
          delta += thisChild.merge(otherChild);
        }
      }
      size += delta;
      return delta;
    }

    /**
     * Returns a flattened representation of this trie.
     *
     * <p>The flattened representation is a list of lists of strings, where each inner list
     * represents a path in the trie and the last element in the list is a boolean in string
     * representation denoting whether this path was truncated. i.e. <["a", "b", "false"], ["c",
     * "true"]>
     *
     * @return The flattened representation of this trie.
     */
    List<List<String>> flattened() {
      List<List<String>> result = new ArrayList<>();
      if (truncated) {
        result.add(Collections.singletonList(String.valueOf(true)));
      } else if (children.isEmpty()) {
        result.add(Collections.singletonList(String.valueOf(false)));
      } else {
        List<String> prefixes = new ArrayList<>(children.keySet());
        Collections.sort(prefixes);
        for (String prefix : prefixes) {
          BoundedTrieNode child = children.get(prefix);
          if (child != null) {
            for (List<String> flattened : child.flattened()) {
              List<String> newList = new ArrayList<>();
              newList.add(prefix);
              newList.addAll(flattened);
              result.add(newList);
            }
          }
        }
      }
      return result;
    }

    /**
     * Converts this `BoundedTrieNode` to proto.
     *
     * @return The {@link org.apache.beam.model.pipeline.v1.MetricsApi.BoundedTrieNode}
     *     representation of this node.
     */
    org.apache.beam.model.pipeline.v1.MetricsApi.BoundedTrieNode toProto() {
      org.apache.beam.model.pipeline.v1.MetricsApi.BoundedTrieNode.Builder builder =
          org.apache.beam.model.pipeline.v1.MetricsApi.BoundedTrieNode.newBuilder();
      builder.setTruncated(truncated);
      children.forEach((key, value) -> builder.putChildren(key, value.toProto()));
      return builder.build();
    }

    /**
     * Constructs a `BoundedTrieNode` from proto.
     *
     * @param proto The {@link org.apache.beam.model.pipeline.v1.MetricsApi.BoundedTrieNode}
     *     representation of the node.
     * @return The corresponding `BoundedTrieNode`.
     */
    static BoundedTrieNode fromProto(
        org.apache.beam.model.pipeline.v1.MetricsApi.BoundedTrieNode proto) {
      BoundedTrieNode node = new BoundedTrieNode();
      if (proto.getTruncated()) {
        node.truncated = true;
        node.children = new HashMap<>();
      } else {
        node.children = new HashMap<>();
        proto
            .getChildrenMap()
            .forEach((key, value) -> node.children.put(key, BoundedTrieNode.fromProto(value)));
        node.size =
            Math.max(1, node.children.values().stream().mapToInt(BoundedTrieNode::getSize).sum());
      }
      return node;
    }

    /**
     * Checks if the trie contains the given path represented by the list of segments.
     *
     * @param segments The segments of the path to check.
     * @return True if the trie contains the path, false otherwise.
     */
    boolean contains(List<String> segments) {
      if (truncated || segments.isEmpty()) {
        return true;
      }
      String head = segments.get(0);
      List<String> tail = segments.subList(1, segments.size());
      return children.containsKey(head) && children.get(head).contains(tail);
    }

    /**
     * Returns the size of the subtree rooted at this node.
     *
     * @return The size of the subtree.
     */
    int getSize() {
      return size;
    }

    /**
     * Returns whether this node is truncated.
     *
     * @return Whether this node is truncated.
     */
    boolean isTruncated() {
      return truncated;
    }

    @Override
    public int hashCode() {
      int result = 17; // standard prime numbers
      result = 31 * result + size;
      // recursively traverse to calculate hashcode of each node
      result = 31 * result + children.hashCode();
      result = 31 * result + (truncated ? 1 : 0);
      return result;
    }

    @Override
    public boolean equals(@Nullable Object other) {
      if (this == other) {
        return true;
      }
      if (other == null || getClass() != other.getClass()) {
        return false;
      }
      BoundedTrieNode that = (BoundedTrieNode) other;
      return truncated == that.truncated && children.equals(that.children);
    }

    @Override
    public String toString() {
      return "{"
          + flattened().stream()
              .map(list -> "'" + String.join("", list) + "'")
              .collect(Collectors.joining(", "))
          + "}";
    }
  }
}
