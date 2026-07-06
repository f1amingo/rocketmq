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
package org.apache.rocketmq.broker.processor;

import io.netty.channel.ChannelHandlerContext;
import java.nio.ByteBuffer;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.lite.AbstractLiteLifecycleManager;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.lite.LiteUtil;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.PeekLiteMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.exception.ConsumeQueueException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PeekLiteMessageProcessorTest {

    private static final String PARENT_TOPIC = "parentTopic";
    private static final String LITE_TOPIC = "liteTopic";
    private static final String GROUP = "group";

    @Mock
    private BrokerController brokerController;
    @Mock
    private MessageStore messageStore;
    @Mock
    private TopicConfigManager topicConfigManager;
    @Mock
    private SubscriptionGroupManager subscriptionGroupManager;
    @Mock
    private ConsumerOffsetManager consumerOffsetManager;
    @Mock
    private AbstractLiteLifecycleManager liteLifecycleManager;
    @Mock
    private BrokerStatsManager brokerStatsManager;
    @Mock
    private ChannelHandlerContext handlerContext;

    private BrokerConfig brokerConfig;
    private PeekLiteMessageProcessor peekLiteMessageProcessor;

    @Before
    public void setUp() {
        brokerConfig = new BrokerConfig();
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        when(brokerController.getMessageStore()).thenReturn(messageStore);
        when(brokerController.getTopicConfigManager()).thenReturn(topicConfigManager);
        when(brokerController.getSubscriptionGroupManager()).thenReturn(subscriptionGroupManager);
        when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        when(brokerController.getLiteLifecycleManager()).thenReturn(liteLifecycleManager);
        when(brokerController.getBrokerStatsManager()).thenReturn(brokerStatsManager);
        when(messageStore.now()).thenReturn(System.currentTimeMillis());

        peekLiteMessageProcessor = new PeekLiteMessageProcessor(brokerController);
    }

    // ==================== preCheck tests ====================

    @Test
    public void testPreCheck_noPermission() {
        brokerConfig.setBrokerPermission(PermName.PERM_WRITE);
        PeekLiteMessageRequestHeader header = buildHeader();
        RemotingCommand response = RemotingCommand.createResponseCommand(
            org.apache.rocketmq.remoting.protocol.header.PopMessageResponseHeader.class);
        RemotingCommand result = peekLiteMessageProcessor.preCheck(handlerContext, header, response);
        assertNotNull(result);
        assertEquals(ResponseCode.NO_PERMISSION, result.getCode());
    }

    @Test
    public void testPreCheck_maxMsgNumExceeded() {
        PeekLiteMessageRequestHeader header = buildHeader();
        header.setMaxMsgNum(64); // exceeds MAX_PEEK_MSG_NUM = 32
        RemotingCommand response = RemotingCommand.createResponseCommand(
            org.apache.rocketmq.remoting.protocol.header.PopMessageResponseHeader.class);
        RemotingCommand result = peekLiteMessageProcessor.preCheck(handlerContext, header, response);
        assertNotNull(result);
        assertEquals(ResponseCode.INVALID_PARAMETER, result.getCode());
    }

    @Test
    public void testPreCheck_topicNotExist() {
        PeekLiteMessageRequestHeader header = buildHeader();
        when(topicConfigManager.selectTopicConfig(PARENT_TOPIC)).thenReturn(null);
        RemotingCommand response = RemotingCommand.createResponseCommand(
            org.apache.rocketmq.remoting.protocol.header.PopMessageResponseHeader.class);
        RemotingCommand result = peekLiteMessageProcessor.preCheck(handlerContext, header, response);
        assertNotNull(result);
        assertEquals(ResponseCode.TOPIC_NOT_EXIST, result.getCode());
    }

    @Test
    public void testPreCheck_topicNotReadable() {
        PeekLiteMessageRequestHeader header = buildHeader();
        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setPerm(PermName.PERM_WRITE);
        when(topicConfigManager.selectTopicConfig(PARENT_TOPIC)).thenReturn(topicConfig);
        RemotingCommand response = RemotingCommand.createResponseCommand(
            org.apache.rocketmq.remoting.protocol.header.PopMessageResponseHeader.class);
        RemotingCommand result = peekLiteMessageProcessor.preCheck(handlerContext, header, response);
        assertNotNull(result);
        assertEquals(ResponseCode.NO_PERMISSION, result.getCode());
    }

    @Test
    public void testPreCheck_topicNotLiteType() {
        PeekLiteMessageRequestHeader header = buildHeader();
        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setPerm(PermName.PERM_READ | PermName.PERM_WRITE);
        topicConfig.setTopicMessageType(TopicMessageType.NORMAL);
        when(topicConfigManager.selectTopicConfig(PARENT_TOPIC)).thenReturn(topicConfig);
        RemotingCommand response = RemotingCommand.createResponseCommand(
            org.apache.rocketmq.remoting.protocol.header.PopMessageResponseHeader.class);
        RemotingCommand result = peekLiteMessageProcessor.preCheck(handlerContext, header, response);
        assertNotNull(result);
        assertEquals(ResponseCode.INVALID_PARAMETER, result.getCode());
    }

    @Test
    public void testPreCheck_consumeNotEnabled() {
        PeekLiteMessageRequestHeader header = buildHeader();
        TopicConfig topicConfig = buildLiteTopicConfig();
        when(topicConfigManager.selectTopicConfig(PARENT_TOPIC)).thenReturn(topicConfig);
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setConsumeEnable(false);
        when(subscriptionGroupManager.findSubscriptionGroupConfig(GROUP)).thenReturn(groupConfig);
        RemotingCommand response = RemotingCommand.createResponseCommand(
            org.apache.rocketmq.remoting.protocol.header.PopMessageResponseHeader.class);
        RemotingCommand result = peekLiteMessageProcessor.preCheck(handlerContext, header, response);
        assertNotNull(result);
        assertEquals(ResponseCode.NO_PERMISSION, result.getCode());
    }

    @Test
    public void testPreCheck_bindTopicMismatch() {
        PeekLiteMessageRequestHeader header = buildHeader();
        TopicConfig topicConfig = buildLiteTopicConfig();
        when(topicConfigManager.selectTopicConfig(PARENT_TOPIC)).thenReturn(topicConfig);
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setConsumeEnable(true);
        groupConfig.setLiteBindTopic("otherTopic");
        when(subscriptionGroupManager.findSubscriptionGroupConfig(GROUP)).thenReturn(groupConfig);
        RemotingCommand response = RemotingCommand.createResponseCommand(
            org.apache.rocketmq.remoting.protocol.header.PopMessageResponseHeader.class);
        RemotingCommand result = peekLiteMessageProcessor.preCheck(handlerContext, header, response);
        assertNotNull(result);
        assertEquals(ResponseCode.INVALID_PARAMETER, result.getCode());
    }

    @Test
    public void testPreCheck_lmqNotExist_returnsSuccess() {
        PeekLiteMessageRequestHeader header = buildHeader();
        TopicConfig topicConfig = buildLiteTopicConfig();
        when(topicConfigManager.selectTopicConfig(PARENT_TOPIC)).thenReturn(topicConfig);
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setConsumeEnable(true);
        groupConfig.setLiteBindTopic(PARENT_TOPIC);
        when(subscriptionGroupManager.findSubscriptionGroupConfig(GROUP)).thenReturn(groupConfig);
        String lmqName = LiteUtil.toLmqName(PARENT_TOPIC, LITE_TOPIC);
        when(liteLifecycleManager.isLmqExist(lmqName)).thenReturn(false);
        RemotingCommand response = RemotingCommand.createResponseCommand(
            org.apache.rocketmq.remoting.protocol.header.PopMessageResponseHeader.class);
        RemotingCommand result = peekLiteMessageProcessor.preCheck(handlerContext, header, response);
        // lmq not exist → return SUCCESS (empty result is valid for peek)
        assertNotNull(result);
        assertEquals(ResponseCode.SUCCESS, result.getCode());
    }

    @Test
    public void testPreCheck_normal_passesPreCheck() {
        PeekLiteMessageRequestHeader header = buildHeader();
        TopicConfig topicConfig = buildLiteTopicConfig();
        when(topicConfigManager.selectTopicConfig(PARENT_TOPIC)).thenReturn(topicConfig);
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setConsumeEnable(true);
        groupConfig.setLiteBindTopic(PARENT_TOPIC);
        when(subscriptionGroupManager.findSubscriptionGroupConfig(GROUP)).thenReturn(groupConfig);
        String lmqName = LiteUtil.toLmqName(PARENT_TOPIC, LITE_TOPIC);
        when(liteLifecycleManager.isLmqExist(lmqName)).thenReturn(true);
        RemotingCommand response = RemotingCommand.createResponseCommand(
            org.apache.rocketmq.remoting.protocol.header.PopMessageResponseHeader.class);
        RemotingCommand result = peekLiteMessageProcessor.preCheck(handlerContext, header, response);
        // null means precheck passed
        assertNull(result);
    }

    // ==================== processRequest tests ====================

    @Test
    public void testProcessRequest_minOffsetNegative() throws RemotingCommandException {
        setupPreCheckPass();
        String lmqName = LiteUtil.toLmqName(PARENT_TOPIC, LITE_TOPIC);
        when(messageStore.getMinOffsetInQueue(lmqName, 0)).thenReturn(-1L);
        RemotingCommand request = buildRequest();
        RemotingCommand response = peekLiteMessageProcessor.processRequest(handlerContext, request);
        assertEquals(ResponseCode.SYSTEM_ERROR, response.getCode());
    }

    @Test
    public void testProcessRequest_maxOffsetException() throws Exception {
        setupPreCheckPass();
        String lmqName = LiteUtil.toLmqName(PARENT_TOPIC, LITE_TOPIC);
        when(messageStore.getMinOffsetInQueue(lmqName, 0)).thenReturn(0L);
        when(messageStore.getMaxOffsetInQueue(lmqName, 0)).thenThrow(new ConsumeQueueException("test"));
        RemotingCommand request = buildRequest();
        RemotingCommand response = peekLiteMessageProcessor.processRequest(handlerContext, request);
        assertEquals(ResponseCode.SYSTEM_ERROR, response.getCode());
    }

    @Test
    public void testProcessRequest_forwardFound() throws Exception {
        setupPreCheckPass();
        String lmqName = LiteUtil.toLmqName(PARENT_TOPIC, LITE_TOPIC);
        when(messageStore.getMinOffsetInQueue(lmqName, 0)).thenReturn(0L);
        when(messageStore.getMaxOffsetInQueue(lmqName, 0)).thenReturn(100L);
        // consumer offset fallback
        when(consumerOffsetManager.queryOffset(anyString(), anyString(), anyInt())).thenReturn(-1L);

        GetMessageResult getResult = buildGetMessageResult(GetMessageStatus.FOUND, 3, 100L, 0L, 100L);
        when(messageStore.getMessage(eq(GROUP), eq(lmqName), eq(0), anyLong(), anyInt(), any()))
            .thenReturn(getResult);

        RemotingCommand request = buildRequest();
        RemotingCommand response = peekLiteMessageProcessor.processRequest(handlerContext, request);
        assertEquals(ResponseCode.SUCCESS, response.getCode());
        assertNotNull(response.getBody());
    }

    @Test
    public void testProcessRequest_forwardEmpty() throws Exception {
        setupPreCheckPass();
        String lmqName = LiteUtil.toLmqName(PARENT_TOPIC, LITE_TOPIC);
        when(messageStore.getMinOffsetInQueue(lmqName, 0)).thenReturn(0L);
        when(messageStore.getMaxOffsetInQueue(lmqName, 0)).thenReturn(100L);
        when(consumerOffsetManager.queryOffset(anyString(), anyString(), anyInt())).thenReturn(-1L);

        GetMessageResult getResult = new GetMessageResult();
        getResult.setStatus(GetMessageStatus.NO_MESSAGE_IN_QUEUE);
        getResult.setNextBeginOffset(0L);
        getResult.setMaxOffset(100L);
        when(messageStore.getMessage(eq(GROUP), eq(lmqName), eq(0), anyLong(), anyInt(), any()))
            .thenReturn(getResult);

        RemotingCommand request = buildRequest();
        RemotingCommand response = peekLiteMessageProcessor.processRequest(handlerContext, request);
        // Empty result is valid for peek → SUCCESS
        assertEquals(ResponseCode.SUCCESS, response.getCode());
    }

    @Test
    public void testProcessRequest_backwardDirection() throws Exception {
        setupPreCheckPass();
        String lmqName = LiteUtil.toLmqName(PARENT_TOPIC, LITE_TOPIC);
        when(messageStore.getMinOffsetInQueue(lmqName, 0)).thenReturn(0L);
        when(messageStore.getMaxOffsetInQueue(lmqName, 0)).thenReturn(100L);

        PeekLiteMessageRequestHeader header = buildHeader();
        header.setPeekDirection("BACKWARD");
        header.setOffsetOptionType("OFFSET");
        header.setOffsetOptionValue(50L);
        RemotingCommand request = buildRequestWithHeader(header);

        GetMessageResult getResult = buildGetMessageResult(GetMessageStatus.FOUND, 2, 50L, 48L, 100L);
        when(messageStore.getMessage(eq(GROUP), eq(lmqName), eq(0), anyLong(), anyInt(), any()))
            .thenReturn(getResult);

        RemotingCommand response = peekLiteMessageProcessor.processRequest(handlerContext, request);
        assertEquals(ResponseCode.SUCCESS, response.getCode());
    }

    @Test
    public void testProcessRequest_offsetTooSmall_retry() throws Exception {
        setupPreCheckPass();
        String lmqName = LiteUtil.toLmqName(PARENT_TOPIC, LITE_TOPIC);
        when(messageStore.getMinOffsetInQueue(lmqName, 0)).thenReturn(0L);
        when(messageStore.getMaxOffsetInQueue(lmqName, 0)).thenReturn(100L);

        // First call returns OFFSET_TOO_SMALL
        GetMessageResult firstResult = new GetMessageResult();
        firstResult.setStatus(GetMessageStatus.OFFSET_TOO_SMALL);
        firstResult.setNextBeginOffset(10L);
        // Retry call returns FOUND
        GetMessageResult retryResult = buildGetMessageResult(GetMessageStatus.FOUND, 1, 20L, 10L, 100L);

        when(messageStore.getMessage(eq(GROUP), eq(lmqName), eq(0), anyLong(), anyInt(), any()))
            .thenReturn(firstResult)
            .thenReturn(retryResult);

        RemotingCommand request = buildRequest();
        RemotingCommand response = peekLiteMessageProcessor.processRequest(handlerContext, request);
        assertEquals(ResponseCode.SUCCESS, response.getCode());
    }

    @Test
    public void testProcessRequest_policyMinOffset() throws Exception {
        setupPreCheckPass();
        String lmqName = LiteUtil.toLmqName(PARENT_TOPIC, LITE_TOPIC);
        when(messageStore.getMinOffsetInQueue(lmqName, 0)).thenReturn(5L);
        when(messageStore.getMaxOffsetInQueue(lmqName, 0)).thenReturn(100L);

        PeekLiteMessageRequestHeader header = buildHeader();
        header.setOffsetOptionType("POLICY");
        header.setOffsetOptionValue(1L); // POLICY_MIN_VALUE
        RemotingCommand request = buildRequestWithHeader(header);

        GetMessageResult getResult = buildGetMessageResult(GetMessageStatus.FOUND, 1, 10L, 5L, 100L);
        when(messageStore.getMessage(eq(GROUP), eq(lmqName), eq(0), eq(5L), anyInt(), any()))
            .thenReturn(getResult);

        RemotingCommand response = peekLiteMessageProcessor.processRequest(handlerContext, request);
        assertEquals(ResponseCode.SUCCESS, response.getCode());
    }

    @Test
    public void testProcessRequest_policyMaxOffset() throws Exception {
        setupPreCheckPass();
        String lmqName = LiteUtil.toLmqName(PARENT_TOPIC, LITE_TOPIC);
        when(messageStore.getMinOffsetInQueue(lmqName, 0)).thenReturn(0L);
        when(messageStore.getMaxOffsetInQueue(lmqName, 0)).thenReturn(100L);

        PeekLiteMessageRequestHeader header = buildHeader();
        header.setOffsetOptionType("POLICY");
        header.setOffsetOptionValue(2L); // POLICY_MAX_VALUE
        RemotingCommand request = buildRequestWithHeader(header);

        // at maxOffset, no messages to read forward
        GetMessageResult getResult = new GetMessageResult();
        getResult.setStatus(GetMessageStatus.NO_MESSAGE_IN_QUEUE);
        getResult.setNextBeginOffset(100L);
        getResult.setMaxOffset(100L);
        when(messageStore.getMessage(eq(GROUP), eq(lmqName), eq(0), eq(100L), anyInt(), any()))
            .thenReturn(getResult);

        RemotingCommand response = peekLiteMessageProcessor.processRequest(handlerContext, request);
        assertEquals(ResponseCode.SUCCESS, response.getCode());
    }

    @Test
    public void testProcessRequest_timestampOffset() throws Exception {
        setupPreCheckPass();
        String lmqName = LiteUtil.toLmqName(PARENT_TOPIC, LITE_TOPIC);
        when(messageStore.getMinOffsetInQueue(lmqName, 0)).thenReturn(0L);
        when(messageStore.getMaxOffsetInQueue(lmqName, 0)).thenReturn(100L);

        long timestamp = System.currentTimeMillis() - 1000;
        when(messageStore.getOffsetInQueueByTime(lmqName, 0, timestamp)).thenReturn(42L);

        PeekLiteMessageRequestHeader header = buildHeader();
        header.setOffsetOptionType("TIMESTAMP");
        header.setOffsetOptionValue(timestamp);
        RemotingCommand request = buildRequestWithHeader(header);

        GetMessageResult getResult = buildGetMessageResult(GetMessageStatus.FOUND, 1, 10L, 42L, 100L);
        when(messageStore.getMessage(eq(GROUP), eq(lmqName), eq(0), eq(42L), anyInt(), any()))
            .thenReturn(getResult);

        RemotingCommand response = peekLiteMessageProcessor.processRequest(handlerContext, request);
        assertEquals(ResponseCode.SUCCESS, response.getCode());
    }

    @Test
    public void testProcessRequest_backwardRestNum() throws Exception {
        setupPreCheckPass();
        String lmqName = LiteUtil.toLmqName(PARENT_TOPIC, LITE_TOPIC);
        when(messageStore.getMinOffsetInQueue(lmqName, 0)).thenReturn(0L);
        when(messageStore.getMaxOffsetInQueue(lmqName, 0)).thenReturn(100L);

        PeekLiteMessageRequestHeader header = buildHeader();
        header.setPeekDirection("BACKWARD");
        header.setOffsetOptionType("OFFSET");
        header.setOffsetOptionValue(50L);
        header.setMaxMsgNum(10);
        RemotingCommand request = buildRequestWithHeader(header);

        // BACKWARD: startOffset = max(0, 51-10) = 41, readNum = 51-41 = 10
        GetMessageResult getResult = buildGetMessageResult(GetMessageStatus.FOUND, 10, 50L, 41L, 100L);
        when(messageStore.getMessage(eq(GROUP), eq(lmqName), eq(0), eq(41L), eq(10), any()))
            .thenReturn(getResult);

        RemotingCommand response = peekLiteMessageProcessor.processRequest(handlerContext, request);
        assertEquals(ResponseCode.SUCCESS, response.getCode());
    }

    @Test
    public void testRejectRequest() {
        assertThat(peekLiteMessageProcessor.rejectRequest()).isFalse();
    }

    // ==================== helpers ====================

    private TopicConfig buildLiteTopicConfig() {
        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setPerm(PermName.PERM_READ | PermName.PERM_WRITE);
        topicConfig.setTopicMessageType(TopicMessageType.LITE);
        return topicConfig;
    }

    private PeekLiteMessageRequestHeader buildHeader() {
        PeekLiteMessageRequestHeader header = new PeekLiteMessageRequestHeader();
        header.setParentTopic(PARENT_TOPIC);
        header.setLiteTopic(LITE_TOPIC);
        header.setConsumerGroup(GROUP);
        header.setMaxMsgNum(10);
        header.setOffsetOptionType("POLICY");
        header.setOffsetOptionValue(0L);
        header.setPeekDirection("FORWARD");
        return header;
    }

    private RemotingCommand buildRequest() {
        return buildRequestWithHeader(buildHeader());
    }

    private RemotingCommand buildRequestWithHeader(PeekLiteMessageRequestHeader header) {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PEEK_LITE_MESSAGE, header);
        request.makeCustomHeaderToNet();
        return request;
    }

    private void setupPreCheckPass() {
        TopicConfig topicConfig = buildLiteTopicConfig();
        when(topicConfigManager.selectTopicConfig(PARENT_TOPIC)).thenReturn(topicConfig);
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setConsumeEnable(true);
        groupConfig.setLiteBindTopic(PARENT_TOPIC);
        when(subscriptionGroupManager.findSubscriptionGroupConfig(GROUP)).thenReturn(groupConfig);
        String lmqName = LiteUtil.toLmqName(PARENT_TOPIC, LITE_TOPIC);
        when(liteLifecycleManager.isLmqExist(lmqName)).thenReturn(true);
    }

    private GetMessageResult buildGetMessageResult(GetMessageStatus status, int msgCount,
        long nextBeginOffset, long minOffset, long maxOffset) {
        GetMessageResult result = new GetMessageResult();
        result.setStatus(status);
        result.setNextBeginOffset(nextBeginOffset);
        result.setMinOffset(minOffset);
        result.setMaxOffset(maxOffset);
        if (GetMessageStatus.FOUND.equals(status)) {
            for (int i = 0; i < msgCount; i++) {
                ByteBuffer bb = ByteBuffer.allocate(64);
                bb.putLong(56, System.currentTimeMillis()); // store timestamp position
                SelectMappedBufferResult bufferResult = new SelectMappedBufferResult(0, bb, 64, null);
                result.addMessage(bufferResult);
            }
        }
        return result;
    }
}
