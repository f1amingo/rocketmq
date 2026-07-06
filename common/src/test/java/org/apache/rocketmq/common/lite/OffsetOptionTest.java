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
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class OffsetOptionTest {

    @Test
    public void testDefaultConstructor() {
        OffsetOption option = new OffsetOption();
        assertNull(option.getType());
        assertEquals(0L, option.getValue());
        assertNull(option.getCursor());
    }

    @Test
    public void testTypeValueConstructor() {
        OffsetOption option = new OffsetOption(OffsetOption.Type.OFFSET, 100L);
        assertEquals(OffsetOption.Type.OFFSET, option.getType());
        assertEquals(100L, option.getValue());
        assertNull(option.getCursor());
    }

    @Test
    public void testOfCursor() {
        Map<String, long[]> ranges = new HashMap<>();
        ranges.put("broker-a", new long[]{0, 5});
        Cursor cursor = new Cursor(ranges);
        OffsetOption option = OffsetOption.ofCursor(cursor);
        assertEquals(OffsetOption.Type.CURSOR, option.getType());
        assertNotNull(option.getCursor());
        assertEquals(cursor, option.getCursor());
    }

    @Test
    public void testSetters() {
        OffsetOption option = new OffsetOption();
        option.setType(OffsetOption.Type.TIMESTAMP);
        option.setValue(123456789L);
        Cursor cursor = new Cursor();
        option.setCursor(cursor);
        assertEquals(OffsetOption.Type.TIMESTAMP, option.getType());
        assertEquals(123456789L, option.getValue());
        assertEquals(cursor, option.getCursor());
    }

    @Test
    public void testEquals_sameTypeAndValue() {
        OffsetOption a = new OffsetOption(OffsetOption.Type.POLICY, 0L);
        OffsetOption b = new OffsetOption(OffsetOption.Type.POLICY, 0L);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    public void testEquals_differentType() {
        OffsetOption a = new OffsetOption(OffsetOption.Type.POLICY, 0L);
        OffsetOption b = new OffsetOption(OffsetOption.Type.OFFSET, 0L);
        assertNotEquals(a, b);
    }

    @Test
    public void testEquals_differentValue() {
        OffsetOption a = new OffsetOption(OffsetOption.Type.OFFSET, 100L);
        OffsetOption b = new OffsetOption(OffsetOption.Type.OFFSET, 200L);
        assertNotEquals(a, b);
    }

    @Test
    public void testEquals_cursorOptions() {
        Map<String, long[]> ranges = new HashMap<>();
        ranges.put("broker-a", new long[]{0, 5});
        Cursor cursor1 = new Cursor(ranges);
        Cursor cursor2 = new Cursor(new HashMap<>(ranges));
        OffsetOption a = OffsetOption.ofCursor(cursor1);
        OffsetOption b = OffsetOption.ofCursor(cursor2);
        // Cursor doesn't override equals, so different instances are not equal
        assertNotEquals(a, b);
    }

    @Test
    public void testEquals_nullAndDifferentClass() {
        OffsetOption option = new OffsetOption(OffsetOption.Type.POLICY, 0L);
        assertNotEquals(option, null);
        assertNotEquals(option, "string");
    }

    @Test
    public void testPolicyConstants() {
        assertEquals(0L, OffsetOption.POLICY_LAST_VALUE);
        assertEquals(1L, OffsetOption.POLICY_MIN_VALUE);
        assertEquals(2L, OffsetOption.POLICY_MAX_VALUE);
    }

    @Test
    public void testTypeValues() {
        OffsetOption.Type[] types = OffsetOption.Type.values();
        assertEquals(5, types.length);
        assertTrue(java.util.Arrays.asList(types).contains(OffsetOption.Type.POLICY));
        assertTrue(java.util.Arrays.asList(types).contains(OffsetOption.Type.OFFSET));
        assertTrue(java.util.Arrays.asList(types).contains(OffsetOption.Type.TAIL_N));
        assertTrue(java.util.Arrays.asList(types).contains(OffsetOption.Type.TIMESTAMP));
        assertTrue(java.util.Arrays.asList(types).contains(OffsetOption.Type.CURSOR));
    }

    @Test
    public void testToString() {
        OffsetOption option = new OffsetOption(OffsetOption.Type.OFFSET, 42L);
        String str = option.toString();
        assertTrue(str.contains("OFFSET"));
        assertTrue(str.contains("42"));
    }
}
