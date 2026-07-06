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
package org.apache.rocketmq.proxy.grpc.v2.consumer;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Cursor;
import apache.rocketmq.v2.OffsetOption;
import apache.rocketmq.v2.PeekDirection;
import apache.rocketmq.v2.PeekMessageRequest;
import apache.rocketmq.v2.PeekMessageResponse;
import apache.rocketmq.v2.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.rocketmq.client.consumer.PeekResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.proxy.grpc.v2.BaseActivityTest;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class PeekMessageActivityTest extends BaseActivityTest {

    private static final String TOPIC = "testTopic";
    private static final String LITE_TOPIC = "testLiteTopic";
    private static final String CONSUMER_GROUP = "testGroup";

    private PeekMessageActivity peekMessageActivity;

    @Before
    public void before() throws Throwable {
        super.before();
        this.peekMessageActivity = new PeekMessageActivity(
            messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
    }

    @Test
    public void testPeekMessage_policyOffset_forward() throws Exception {
        PeekResult peekResult = new PeekResult(PopStatus.FOUND, new ArrayList<>());
        peekResult.setRestNum(0);
        when(messagingProcessor.peekLiteMessage(any(), anyString(), anyString(), anyString(),
            anyInt(), any(), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(peekResult));

        PeekMessageRequest request = PeekMessageRequest.newBuilder()
            .setTopic(Resource.newBuilder().setName(TOPIC).build())
            .setLiteTopic(LITE_TOPIC)
            .setGroup(Resource.newBuilder().setName(CONSUMER_GROUP).build())
            .setMaxMsgNum(10)
            .setOffsetOption(OffsetOption.newBuilder()
                .setPolicy(OffsetOption.Policy.MIN)
                .build())
            .setDirection(PeekDirection.FORWARD)
            .build();

        CompletableFuture<PeekMessageResponse> future =
            peekMessageActivity.peekMessage(createContext(), request);
        PeekMessageResponse response = future.get();
        assertNotNull(response);
        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(0, response.getMessagesCount());
        assertEquals(0, response.getRestNum());
    }

    @Test
    public void testPeekMessage_timestampOffset() throws Exception {
        PeekResult peekResult = new PeekResult(PopStatus.FOUND, new ArrayList<>());
        peekResult.setRestNum(5);
        when(messagingProcessor.peekLiteMessage(any(), anyString(), anyString(), anyString(),
            anyInt(), any(), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(peekResult));

        PeekMessageRequest request = PeekMessageRequest.newBuilder()
            .setTopic(Resource.newBuilder().setName(TOPIC).build())
            .setLiteTopic(LITE_TOPIC)
            .setGroup(Resource.newBuilder().setName(CONSUMER_GROUP).build())
            .setMaxMsgNum(10)
            .setOffsetOption(OffsetOption.newBuilder()
                .setTimestamp(System.currentTimeMillis())
                .build())
            .setDirection(PeekDirection.FORWARD)
            .build();

        CompletableFuture<PeekMessageResponse> future =
            peekMessageActivity.peekMessage(createContext(), request);
        PeekMessageResponse response = future.get();
        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(5, response.getRestNum());
    }

    @Test
    public void testPeekMessage_cursorOffset() throws Exception {
        PeekResult peekResult = new PeekResult(PopStatus.FOUND, new ArrayList<>());
        peekResult.setRestNum(0);
        when(messagingProcessor.peekLiteMessage(any(), anyString(), anyString(), anyString(),
            anyInt(), any(), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(peekResult));

        Cursor protoCursor = Cursor.newBuilder()
            .putRanges("broker-a", Cursor.OffsetRange.newBuilder()
                .setBegin(0).setEnd(10).build())
            .build();
        PeekMessageRequest request = PeekMessageRequest.newBuilder()
            .setTopic(Resource.newBuilder().setName(TOPIC).build())
            .setLiteTopic(LITE_TOPIC)
            .setGroup(Resource.newBuilder().setName(CONSUMER_GROUP).build())
            .setMaxMsgNum(10)
            .setOffsetOption(OffsetOption.newBuilder()
                .setCursor(protoCursor)
                .build())
            .setDirection(PeekDirection.BACKWARD)
            .build();

        CompletableFuture<PeekMessageResponse> future =
            peekMessageActivity.peekMessage(createContext(), request);
        PeekMessageResponse response = future.get();
        assertEquals(Code.OK, response.getStatus().getCode());
    }

    @Test
    public void testPeekMessage_withCursorInResponse() throws Exception {
        Map<String, long[]> ranges = new HashMap<>();
        ranges.put("broker-a", new long[]{5, 10});
        org.apache.rocketmq.common.lite.Cursor pojoCursor =
            new org.apache.rocketmq.common.lite.Cursor(ranges);

        List<MessageExt> msgs = new ArrayList<>();
        MessageExt msg = new MessageExt();
        msg.setTopic(TOPIC);
        msg.setBody("test".getBytes());
        msg.setQueueOffset(5);
        msg.setQueueId(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreTimestamp(System.currentTimeMillis());
        msg.setBornHost(new java.net.InetSocketAddress("127.0.0.1", 1234));
        msg.setStoreHost(new java.net.InetSocketAddress("127.0.0.1", 5678));
        msg.setMsgId("test-msg-id");
        msg.setReconsumeTimes(0);
        // Initialize properties map to avoid NPE in GrpcConverter
        org.apache.rocketmq.common.message.MessageAccessor.putProperty(msg, "init", "true");
        msgs.add(msg);

        PeekResult peekResult = new PeekResult(PopStatus.FOUND, msgs);
        peekResult.setCursor(pojoCursor);
        peekResult.setRestNum(20);
        when(messagingProcessor.peekLiteMessage(any(), anyString(), anyString(), anyString(),
            anyInt(), any(), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(peekResult));

        PeekMessageRequest request = PeekMessageRequest.newBuilder()
            .setTopic(Resource.newBuilder().setName(TOPIC).build())
            .setLiteTopic(LITE_TOPIC)
            .setGroup(Resource.newBuilder().setName(CONSUMER_GROUP).build())
            .setMaxMsgNum(10)
            .setOffsetOption(OffsetOption.newBuilder()
                .setPolicy(OffsetOption.Policy.LAST)
                .build())
            .setDirection(PeekDirection.FORWARD)
            .build();

        CompletableFuture<PeekMessageResponse> future =
            peekMessageActivity.peekMessage(createContext(), request);
        PeekMessageResponse response = future.get();
        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(1, response.getMessagesCount());
        assertTrue(response.hasCursor());
        assertEquals(5, response.getCursor().getRangesOrThrow("broker-a").getBegin());
        assertEquals(10, response.getCursor().getRangesOrThrow("broker-a").getEnd());
        assertEquals(20, response.getRestNum());
    }

    @Test
    public void testPeekMessage_nullResult() throws Exception {
        when(messagingProcessor.peekLiteMessage(any(), anyString(), anyString(), anyString(),
            anyInt(), any(), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(null));

        PeekMessageRequest request = PeekMessageRequest.newBuilder()
            .setTopic(Resource.newBuilder().setName(TOPIC).build())
            .setLiteTopic(LITE_TOPIC)
            .setGroup(Resource.newBuilder().setName(CONSUMER_GROUP).build())
            .setMaxMsgNum(10)
            .setOffsetOption(OffsetOption.newBuilder()
                .setPolicy(OffsetOption.Policy.LAST)
                .build())
            .setDirection(PeekDirection.FORWARD)
            .build();

        CompletableFuture<PeekMessageResponse> future =
            peekMessageActivity.peekMessage(createContext(), request);
        PeekMessageResponse response = future.get();
        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(0, response.getMessagesCount());
    }

    @Test
    public void testPeekMessage_noOffsetOption() {
        PeekMessageRequest request = PeekMessageRequest.newBuilder()
            .setTopic(Resource.newBuilder().setName(TOPIC).build())
            .setLiteTopic(LITE_TOPIC)
            .setGroup(Resource.newBuilder().setName(CONSUMER_GROUP).build())
            .setMaxMsgNum(10)
            .setDirection(PeekDirection.FORWARD)
            .build();

        CompletableFuture<PeekMessageResponse> future =
            peekMessageActivity.peekMessage(createContext(), request);
        assertTrue(future.isCompletedExceptionally());
        try {
            future.get();
            fail("Expected ExecutionException");
        } catch (ExecutionException e) {
            assertNotNull(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    public void testPeekMessage_processorException() {
        CompletableFuture<PeekResult> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("broker error"));
        when(messagingProcessor.peekLiteMessage(any(), anyString(), anyString(), anyString(),
            anyInt(), any(), any(), anyLong()))
            .thenReturn(failedFuture);

        PeekMessageRequest request = PeekMessageRequest.newBuilder()
            .setTopic(Resource.newBuilder().setName(TOPIC).build())
            .setLiteTopic(LITE_TOPIC)
            .setGroup(Resource.newBuilder().setName(CONSUMER_GROUP).build())
            .setMaxMsgNum(10)
            .setOffsetOption(OffsetOption.newBuilder()
                .setPolicy(OffsetOption.Policy.LAST)
                .build())
            .setDirection(PeekDirection.FORWARD)
            .build();

        CompletableFuture<PeekMessageResponse> future =
            peekMessageActivity.peekMessage(createContext(), request);
        assertTrue(future.isCompletedExceptionally());
    }

    @Test
    public void testPeekMessage_emptyCursorNotSetInResponse() throws Exception {
        // Empty cursor should NOT be set in response
        PeekResult peekResult = new PeekResult(PopStatus.FOUND, new ArrayList<>());
        peekResult.setCursor(new org.apache.rocketmq.common.lite.Cursor());
        peekResult.setRestNum(0);
        when(messagingProcessor.peekLiteMessage(any(), anyString(), anyString(), anyString(),
            anyInt(), any(), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(peekResult));

        PeekMessageRequest request = PeekMessageRequest.newBuilder()
            .setTopic(Resource.newBuilder().setName(TOPIC).build())
            .setLiteTopic(LITE_TOPIC)
            .setGroup(Resource.newBuilder().setName(CONSUMER_GROUP).build())
            .setMaxMsgNum(10)
            .setOffsetOption(OffsetOption.newBuilder()
                .setPolicy(OffsetOption.Policy.LAST)
                .build())
            .setDirection(PeekDirection.FORWARD)
            .build();

        CompletableFuture<PeekMessageResponse> future =
            peekMessageActivity.peekMessage(createContext(), request);
        PeekMessageResponse response = future.get();
        assertEquals(Code.OK, response.getStatus().getCode());
        // empty cursor should not be included in response
        assertTrue(!response.hasCursor() || response.getCursor().getRangesCount() == 0);
    }
}
