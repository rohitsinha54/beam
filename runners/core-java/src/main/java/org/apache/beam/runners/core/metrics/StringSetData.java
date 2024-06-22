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

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.metrics.StringSetResult;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Data describing the StringSet. This should retain enough detail that it can be combined with other
 * {@link StringSetData}.
 */
@AutoValue
public abstract class StringSetData implements Serializable {

  public abstract Set<String> stringSet();

  /** Returns a {@link StringSetData} using the current time. */
  public static StringSetData create(Set<String> stringSet) {
    return new AutoValue_StringSetData(stringSet);
  }

  public static StringSetData empty() {
    return EmptyStringSetData.INSTANCE;
  }

  public StringSetData combine(StringSetData other) {
    Set<String> merged = new HashSet<>();
    merged.addAll(this.stringSet());
    merged.addAll(other.stringSet());
    return StringSetData.create(merged);
  }

  public StringSetResult extractResult() {
    return StringSetResult.create(stringSet());
  }

  /** Empty {@link StringSetData}, representing no values reported. */
  public static class EmptyStringSetData extends StringSetData {

    private static final EmptyStringSetData INSTANCE = new EmptyStringSetData();

    private EmptyStringSetData() {}

    @Override
    public Set<String> stringSet() {
      return Collections.emptySet();
    }

    @Override
    public StringSetResult extractResult() {
      return StringSetResult.empty();
    }
  }
}
