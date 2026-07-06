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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CursorTest {

    @Test
    public void testDefaultConstructor() {
        Cursor cursor = new Cursor();
        assertNotNull(cursor.getRanges());
        assertTrue(cursor.isEmpty());
    }

    @Test
    public void testConstructorWithRanges() {
        Map<String, long[]> ranges = new HashMap<>();
        ranges.put("broker-a", new long[]{0, 10});
        ranges.put("broker-b", new long[]{5, 20});
        Cursor cursor = new Cursor(ranges);
        assertFalse(cursor.isEmpty());
        assertEquals(2, cursor.getRanges().size());
    }

    @Test
    public void testConstructorWithNullRanges() {
        Cursor cursor = new Cursor(null);
        assertNotNull(cursor.getRanges());
        assertTrue(cursor.isEmpty());
    }

    @Test
    public void testConstructorDefensiveCopy() {
        Map<String, long[]> ranges = new HashMap<>();
        ranges.put("broker-a", new long[]{0, 10});
        Cursor cursor = new Cursor(ranges);
        // mutate the original map
        ranges.put("broker-b", new long[]{0, 5});
        // cursor should not be affected
        assertEquals(1, cursor.getRanges().size());
    }

    @Test
    public void testGetRange() {
        Map<String, long[]> ranges = new HashMap<>();
        ranges.put("broker-a", new long[]{3, 7});
        Cursor cursor = new Cursor(ranges);
        assertArrayEquals(new long[]{3, 7}, cursor.getRange("broker-a"));
        assertNull(cursor.getRange("broker-b"));
    }

    @Test
    public void testIsEmpty() {
        Cursor empty = new Cursor();
        assertTrue(empty.isEmpty());
        Map<String, long[]> ranges = new HashMap<>();
        ranges.put("broker-a", new long[]{0, 1});
        Cursor nonEmpty = new Cursor(ranges);
        assertFalse(nonEmpty.isEmpty());
    }

    @Test
    public void testFromOffsetOption_null() {
        Cursor cursor = Cursor.fromOffsetOption(null);
        assertNotNull(cursor);
        assertTrue(cursor.isEmpty());
    }

    @Test
    public void testFromOffsetOption_noCursor() {
        OffsetOption option = new OffsetOption(OffsetOption.Type.POLICY, 0);
        Cursor cursor = Cursor.fromOffsetOption(option);
        assertNotNull(cursor);
        assertTrue(cursor.isEmpty());
    }

    @Test
    public void testFromOffsetOption_withCursor() {
        Map<String, long[]> ranges = new HashMap<>();
        ranges.put("broker-a", new long[]{0, 5});
        Cursor expected = new Cursor(ranges);
        OffsetOption option = OffsetOption.ofCursor(expected);
        Cursor actual = Cursor.fromOffsetOption(option);
        assertNotNull(actual);
        assertFalse(actual.isEmpty());
        assertArrayEquals(new long[]{0, 5}, actual.getRange("broker-a"));
    }

    @Test
    public void testToString() {
        Map<String, long[]> ranges = new HashMap<>();
        ranges.put("broker-a", new long[]{0, 3});
        Cursor cursor = new Cursor(ranges);
        String str = cursor.toString();
        assertTrue(str.contains("broker-a"));
        assertTrue(str.contains("[0, 3)"));
    }
}
