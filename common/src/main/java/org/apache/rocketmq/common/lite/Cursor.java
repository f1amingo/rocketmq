/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.common.lite;

import java.util.HashMap;
import java.util.Map;

/**
 * Distributed read checkpoint encoding a half-open interval [begin, end) per broker.
 * <p>
 * Serialized as {@code {"ranges":{"broker-a":[0,3]}}}.
 * {@code begin} is the inclusive lower offset, {@code end} is the exclusive upper offset.
 * The interval length equals {@code end - begin}; {@code begin == end} denotes an empty range.
 */
public class Cursor {

    /** brokerName → [begin, end) half-open offset range. */
    private Map<String, long[]> ranges;

    /** Default constructor, creates an empty cursor. */
    public Cursor() {
        this.ranges = new HashMap<>();
    }

    public Cursor(Map<String, long[]> ranges) {
        this.ranges = ranges != null ? new HashMap<>(ranges) : new HashMap<>();
    }

    public Map<String, long[]> getRanges() {
        return ranges;
    }

    public void setRanges(Map<String, long[]> ranges) {
        this.ranges = ranges != null ? ranges : new HashMap<>();
    }

    /**
     * @return the raw [begin, end) array for the given broker, or null if absent.
     */
    public long[] getRange(String brokerName) {
        return ranges.get(brokerName);
    }

    public boolean isEmpty() {
        return ranges.isEmpty();
    }

    /**
     * Decode cursor from an OffsetOption.
     * Returns an empty Cursor if the option is null or has no cursor value.
     */
    public static Cursor fromOffsetOption(OffsetOption offsetOption) {
        if (offsetOption == null) {
            return new Cursor();
        }
        return offsetOption.getCursor() != null ? offsetOption.getCursor() : new Cursor();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Cursor{ranges={");
        boolean first = true;
        for (Map.Entry<String, long[]> entry : ranges.entrySet()) {
            if (!first) {
                sb.append(", ");
            }
            sb.append(entry.getKey()).append("=[").append(entry.getValue()[0])
                .append(", ").append(entry.getValue()[1]).append(")");
            first = false;
        }
        sb.append("}}");
        return sb.toString();
    }
}
