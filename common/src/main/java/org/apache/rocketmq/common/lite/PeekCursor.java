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

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Peek cursor for pagination across multiple brokers.
 * Encodes/decodes a map of brokerName -> nextOffset as JSON.
 */
public class PeekCursor {

    private final Map<String, Long> brokerOffsets;

    public PeekCursor(Map<String, Long> brokerOffsets) {
        this.brokerOffsets = brokerOffsets != null ? new HashMap<>(brokerOffsets) : new HashMap<>();
    }

    public Long getBrokerOffset(String brokerName) {
        return brokerOffsets.get(brokerName);
    }

    /**
     * Encode cursor to JSON string.
     */
    public String encode() {
        return JSON.toJSONString(brokerOffsets);
    }

    /**
     * Decode cursor from JSON string.
     */
    public static PeekCursor decode(String encoded) {
        if (encoded == null || encoded.isEmpty()) {
            return new PeekCursor(Collections.emptyMap());
        }
        Map<String, Long> offsets = JSON.parseObject(encoded, new TypeReference<Map<String, Long>>() {
        });
        return new PeekCursor(offsets);
    }

    /**
     * Decode cursor from an OffsetOption.
     * Returns an empty PeekCursor if the option is null or has no cursor value.
     */
    public static PeekCursor fromOffsetOption(OffsetOption offsetOption) {
        if (offsetOption == null) {
            return new PeekCursor(Collections.emptyMap());
        }
        return decode(offsetOption.getCursor());
    }

    @Override
    public String toString() {
        return "PeekCursor{" + brokerOffsets + '}';
    }
}
