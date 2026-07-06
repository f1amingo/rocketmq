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
package org.apache.rocketmq.remoting.protocol.header;

import org.apache.rocketmq.common.lite.OffsetOption;
import org.apache.rocketmq.common.lite.PeekDirection;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PeekLiteMessageRequestHeaderTest {

    @Test
    public void testGettersAndSetters() {
        PeekLiteMessageRequestHeader header = new PeekLiteMessageRequestHeader();
        header.setParentTopic("parentTopic");
        header.setLiteTopic("liteTopic");
        header.setConsumerGroup("group");
        header.setMaxMsgNum(32);
        header.setOffsetOptionType("OFFSET");
        header.setOffsetOptionValue(100L);
        header.setPeekDirection("FORWARD");

        assertEquals("parentTopic", header.getParentTopic());
        assertEquals("liteTopic", header.getLiteTopic());
        assertEquals("group", header.getConsumerGroup());
        assertEquals(32, header.getMaxMsgNum());
        assertEquals("OFFSET", header.getOffsetOptionType());
        assertEquals(100L, header.getOffsetOptionValue());
        assertEquals("FORWARD", header.getPeekDirection());
    }

    @Test
    public void testToPeekDirection_forward() {
        PeekLiteMessageRequestHeader header = new PeekLiteMessageRequestHeader();
        header.setPeekDirection("FORWARD");
        assertEquals(PeekDirection.FORWARD, header.toPeekDirection());
    }

    @Test
    public void testToPeekDirection_backward() {
        PeekLiteMessageRequestHeader header = new PeekLiteMessageRequestHeader();
        header.setPeekDirection("BACKWARD");
        assertEquals(PeekDirection.BACKWARD, header.toPeekDirection());
    }

    @Test
    public void testToPeekDirection_nullDefaultsToForward() {
        PeekLiteMessageRequestHeader header = new PeekLiteMessageRequestHeader();
        header.setPeekDirection(null);
        assertEquals(PeekDirection.FORWARD, header.toPeekDirection());
    }

    @Test
    public void testToOffsetOption_offset() {
        PeekLiteMessageRequestHeader header = new PeekLiteMessageRequestHeader();
        header.setOffsetOptionType("OFFSET");
        header.setOffsetOptionValue(42L);
        OffsetOption option = header.toOffsetOption();
        assertNotNull(option);
        assertEquals(OffsetOption.Type.OFFSET, option.getType());
        assertEquals(42L, option.getValue());
    }

    @Test
    public void testToOffsetOption_policy() {
        PeekLiteMessageRequestHeader header = new PeekLiteMessageRequestHeader();
        header.setOffsetOptionType("POLICY");
        header.setOffsetOptionValue(1L);
        OffsetOption option = header.toOffsetOption();
        assertNotNull(option);
        assertEquals(OffsetOption.Type.POLICY, option.getType());
        assertEquals(1L, option.getValue());
    }

    @Test
    public void testToOffsetOption_timestamp() {
        PeekLiteMessageRequestHeader header = new PeekLiteMessageRequestHeader();
        header.setOffsetOptionType("TIMESTAMP");
        header.setOffsetOptionValue(System.currentTimeMillis());
        OffsetOption option = header.toOffsetOption();
        assertNotNull(option);
        assertEquals(OffsetOption.Type.TIMESTAMP, option.getType());
    }

    @Test
    public void testToOffsetOption_nullType() {
        PeekLiteMessageRequestHeader header = new PeekLiteMessageRequestHeader();
        header.setOffsetOptionType(null);
        assertNull(header.toOffsetOption());
    }

    @Test
    public void testToOffsetOption_invalidType() {
        PeekLiteMessageRequestHeader header = new PeekLiteMessageRequestHeader();
        header.setOffsetOptionType("INVALID_TYPE");
        header.setOffsetOptionValue(0L);
        assertNull(header.toOffsetOption());
    }

    @Test
    public void testToString() {
        PeekLiteMessageRequestHeader header = new PeekLiteMessageRequestHeader();
        header.setParentTopic("parentTopic");
        header.setLiteTopic("liteTopic");
        header.setConsumerGroup("group");
        header.setMaxMsgNum(10);
        header.setOffsetOptionType("POLICY");
        header.setOffsetOptionValue(0L);
        header.setPeekDirection("FORWARD");
        String str = header.toString();
        assertTrue(str.contains("parentTopic"));
        assertTrue(str.contains("liteTopic"));
        assertTrue(str.contains("group"));
        assertTrue(str.contains("FORWARD"));
    }

    @Test
    public void testCheckFieldsDoesNotThrow() throws Exception {
        PeekLiteMessageRequestHeader header = new PeekLiteMessageRequestHeader();
        // checkFields is a no-op, should not throw
        header.checkFields();
    }
}
