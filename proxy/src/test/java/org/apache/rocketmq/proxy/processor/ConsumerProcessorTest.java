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

package org.apache.rocketmq.proxy.processor;

import com.google.common.collect.Sets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.client.consumer.PeekResult;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.lite.Cursor;
import org.apache.rocketmq.common.lite.OffsetOption;
import org.apache.rocketmq.common.lite.PeekDirection;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;
import org.apache.rocketmq.common.utils.FutureUtils;
import org.apache.rocketmq.proxy.common.utils.ProxyUtils;
import org.apache.rocketmq.proxy.service.message.ReceiptHandleMessage;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.proxy.service.route.MessageQueueSelector;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.filter.FilterAPI;
import org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PeekLiteMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsumerProcessorTest extends BaseProcessorTest {

    private static final String CONSUMER_GROUP = "consumerGroup";
    private static final String TOPIC = "topic";
    private static final String CLIENT_ID = "clientId";

    private ConsumerProcessor consumerProcessor;

    @Before
    public void before() throws Throwable {
        super.before();
        this.consumerProcessor = new ConsumerProcessor(messagingProcessor, serviceManager, Executors.newCachedThreadPool());
    }

    @Test
    public void testPopMessage() throws Throwable {
        final String tag = "tag";
        final long invisibleTime = Duration.ofSeconds(15).toMillis();
        ArgumentCaptor<AddressableMessageQueue> messageQueueArgumentCaptor = ArgumentCaptor.forClass(AddressableMessageQueue.class);
        ArgumentCaptor<PopMessageRequestHeader> requestHeaderArgumentCaptor = ArgumentCaptor.forClass(PopMessageRequestHeader.class);

        List<MessageExt> messageExtList = new ArrayList<>();
        messageExtList.add(createMessageExt(TOPIC, "noMatch", 0, invisibleTime));
        messageExtList.add(createMessageExt(TOPIC, tag, 0, invisibleTime));
        messageExtList.add(createMessageExt(TOPIC, tag, 1, invisibleTime));
        PopResult innerPopResult = new PopResult(PopStatus.FOUND, messageExtList);
        when(this.messageService.popMessage(any(), messageQueueArgumentCaptor.capture(), requestHeaderArgumentCaptor.capture(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(innerPopResult));

        when(this.topicRouteService.getCurrentMessageQueueView(any(), anyString()))
            .thenReturn(mock(MessageQueueView.class));

        ArgumentCaptor<String> ackMessageIdArgumentCaptor = ArgumentCaptor.forClass(String.class);
        when(this.messagingProcessor.ackMessage(any(), any(), ackMessageIdArgumentCaptor.capture(), anyString(), anyString(), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(mock(AckResult.class)));

        ArgumentCaptor<String> toDLQMessageIdArgumentCaptor = ArgumentCaptor.forClass(String.class);
        when(this.messagingProcessor.forwardMessageToDeadLetterQueue(any(), any(), toDLQMessageIdArgumentCaptor.capture(), anyString(), anyString(), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(mock(RemotingCommand.class)));

        AddressableMessageQueue messageQueue = mock(AddressableMessageQueue.class);
        PopResult popResult = this.consumerProcessor.popMessage(
            createContext(),
            (ctx, messageQueueView) -> messageQueue,
            CONSUMER_GROUP,
            TOPIC,
            60,
            invisibleTime,
            Duration.ofSeconds(3).toMillis(),
            ConsumeInitMode.MAX,
            FilterAPI.build(TOPIC, tag, ExpressionType.TAG),
            false,
            (ctx, consumerGroup, subscriptionData, messageExt) -> {
                if (!messageExt.getTags().equals(tag)) {
                    return PopMessageResultFilter.FilterResult.NO_MATCH;
                }
                if (messageExt.getReconsumeTimes() > 0) {
                    return PopMessageResultFilter.FilterResult.TO_DLQ;
                }
                return PopMessageResultFilter.FilterResult.MATCH;
            },
            null,
            Duration.ofSeconds(3).toMillis()
        ).get();

        assertSame(messageQueue, messageQueueArgumentCaptor.getValue());
        assertEquals(CONSUMER_GROUP, requestHeaderArgumentCaptor.getValue().getConsumerGroup());
        assertEquals(TOPIC, requestHeaderArgumentCaptor.getValue().getTopic());
        assertEquals(ProxyUtils.MAX_MSG_NUMS_FOR_POP_REQUEST, requestHeaderArgumentCaptor.getValue().getMaxMsgNums());
        assertEquals(tag, requestHeaderArgumentCaptor.getValue().getExp());
        assertEquals(ExpressionType.TAG, requestHeaderArgumentCaptor.getValue().getExpType());

        assertEquals(PopStatus.FOUND, popResult.getPopStatus());
        assertEquals(1, popResult.getMsgFoundList().size());
        assertEquals(messageExtList.get(1), popResult.getMsgFoundList().get(0));

        assertEquals(messageExtList.get(0).getMsgId(), ackMessageIdArgumentCaptor.getValue());
        assertEquals(messageExtList.get(2).getMsgId(), toDLQMessageIdArgumentCaptor.getValue());
    }

    @Test
    public void testAckMessage() throws Throwable {
        ReceiptHandle handle = create(createMessageExt(MixAll.RETRY_GROUP_TOPIC_PREFIX + TOPIC, "", 0, 3000));
        assertNotNull(handle);

        ArgumentCaptor<AckMessageRequestHeader> requestHeaderArgumentCaptor = ArgumentCaptor.forClass(AckMessageRequestHeader.class);
        AckResult innerAckResult = new AckResult();
        innerAckResult.setStatus(AckStatus.OK);
        when(this.messageService.ackMessage(any(), any(), anyString(), requestHeaderArgumentCaptor.capture(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(innerAckResult));

        AckResult ackResult = this.consumerProcessor.ackMessage(createContext(), handle, MessageClientIDSetter.createUniqID(),
            CONSUMER_GROUP, TOPIC, null, 3000).get();

        assertEquals(AckStatus.OK, ackResult.getStatus());
        assertEquals(KeyBuilder.buildPopRetryTopic(TOPIC, CONSUMER_GROUP, new BrokerConfig().isEnableRetryTopicV2()), requestHeaderArgumentCaptor.getValue().getTopic());
        assertEquals(CONSUMER_GROUP, requestHeaderArgumentCaptor.getValue().getConsumerGroup());
        assertEquals(handle.getReceiptHandle(), requestHeaderArgumentCaptor.getValue().getExtraInfo());
    }

    @Test
    public void testBatchAckExpireMessage() throws Throwable {
        String brokerName1 = "brokerName1";

        List<ReceiptHandleMessage> receiptHandleMessageList = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            MessageExt expireMessage = createMessageExt(TOPIC, "", 0, 3000, System.currentTimeMillis() - 10000,
                0, 0, 0, i, brokerName1);
            ReceiptHandle expireHandle = create(expireMessage);
            receiptHandleMessageList.add(new ReceiptHandleMessage(expireHandle, expireMessage.getMsgId()));
        }

        List<BatchAckResult> batchAckResultList = this.consumerProcessor.batchAckMessage(createContext(), receiptHandleMessageList, CONSUMER_GROUP, TOPIC, 3000).get();

        verify(this.messageService, never()).batchAckMessage(any(), anyList(), anyString(), anyString(), anyLong());
        assertEquals(receiptHandleMessageList.size(), batchAckResultList.size());
        for (BatchAckResult batchAckResult : batchAckResultList) {
            assertNull(batchAckResult.getAckResult());
            assertNotNull(batchAckResult.getProxyException());
            assertNotNull(batchAckResult.getReceiptHandleMessage());
        }

    }

    @Test
    public void testBatchAckMessage() throws Throwable {
        String brokerName1 = "brokerName1";
        String brokerName2 = "brokerName2";
        String errThrowBrokerName = "errThrowBrokerName";
        MessageExt expireMessage = createMessageExt(TOPIC, "", 0, 3000, System.currentTimeMillis() - 10000,
            0, 0, 0, 0, brokerName1);
        ReceiptHandle expireHandle = create(expireMessage);

        List<ReceiptHandleMessage> receiptHandleMessageList = new ArrayList<>();
        receiptHandleMessageList.add(new ReceiptHandleMessage(expireHandle, expireMessage.getMsgId()));
        List<String> broker1Msg = new ArrayList<>();
        List<String> broker2Msg = new ArrayList<>();

        long now = System.currentTimeMillis();
        int msgNum = 3;
        for (int i = 0; i < msgNum; i++) {
            MessageExt brokerMessage = createMessageExt(TOPIC, "", 0, 3000, now,
                0, 0, 0, i + 1, brokerName1);
            ReceiptHandle brokerHandle = create(brokerMessage);
            receiptHandleMessageList.add(new ReceiptHandleMessage(brokerHandle, brokerMessage.getMsgId()));
            broker1Msg.add(brokerMessage.getMsgId());
        }
        for (int i = 0; i < msgNum; i++) {
            MessageExt brokerMessage = createMessageExt(TOPIC, "", 0, 3000, now,
                0, 0, 0, i + 1, brokerName2);
            ReceiptHandle brokerHandle = create(brokerMessage);
            receiptHandleMessageList.add(new ReceiptHandleMessage(brokerHandle, brokerMessage.getMsgId()));
            broker2Msg.add(brokerMessage.getMsgId());
        }

        // for this message, will throw exception in batchAckMessage
        MessageExt errThrowMessage = createMessageExt(TOPIC, "", 0, 3000, now,
            0, 0, 0, 0, errThrowBrokerName);
        ReceiptHandle errThrowHandle = create(errThrowMessage);
        receiptHandleMessageList.add(new ReceiptHandleMessage(errThrowHandle, errThrowMessage.getMsgId()));

        Collections.shuffle(receiptHandleMessageList);

        doAnswer((Answer<CompletableFuture<AckResult>>) invocation -> {
            List<ReceiptHandleMessage> handleMessageList = invocation.getArgument(1, List.class);
            AckResult ackResult = new AckResult();
            String brokerName = handleMessageList.get(0).getReceiptHandle().getBrokerName();
            if (brokerName.equals(brokerName1)) {
                ackResult.setStatus(AckStatus.OK);
            } else if (brokerName.equals(brokerName2)) {
                ackResult.setStatus(AckStatus.NO_EXIST);
            } else {
                return FutureUtils.completeExceptionally(new RuntimeException());
            }

            return CompletableFuture.completedFuture(ackResult);
        }).when(this.messageService).batchAckMessage(any(), anyList(), anyString(), anyString(), anyLong());

        List<BatchAckResult> batchAckResultList = this.consumerProcessor.batchAckMessage(createContext(), receiptHandleMessageList, CONSUMER_GROUP, TOPIC, 3000).get();
        assertEquals(receiptHandleMessageList.size(), batchAckResultList.size());

        // check ackResult for each msg
        Map<String, BatchAckResult> msgBatchAckResult = new HashMap<>();
        for (BatchAckResult batchAckResult : batchAckResultList) {
            msgBatchAckResult.put(batchAckResult.getReceiptHandleMessage().getMessageId(), batchAckResult);
        }
        for (String msgId : broker1Msg) {
            assertEquals(AckStatus.OK, msgBatchAckResult.get(msgId).getAckResult().getStatus());
            assertNull(msgBatchAckResult.get(msgId).getProxyException());
        }
        for (String msgId : broker2Msg) {
            assertEquals(AckStatus.NO_EXIST, msgBatchAckResult.get(msgId).getAckResult().getStatus());
            assertNull(msgBatchAckResult.get(msgId).getProxyException());
        }
        assertNotNull(msgBatchAckResult.get(expireMessage.getMsgId()).getProxyException());
        assertEquals(ProxyExceptionCode.INVALID_RECEIPT_HANDLE, msgBatchAckResult.get(expireMessage.getMsgId()).getProxyException().getCode());
        assertNull(msgBatchAckResult.get(expireMessage.getMsgId()).getAckResult());

        assertNotNull(msgBatchAckResult.get(errThrowMessage.getMsgId()).getProxyException());
        assertEquals(ProxyExceptionCode.INTERNAL_SERVER_ERROR, msgBatchAckResult.get(errThrowMessage.getMsgId()).getProxyException().getCode());
        assertNull(msgBatchAckResult.get(errThrowMessage.getMsgId()).getAckResult());
    }

    @Test
    public void testChangeInvisibleTime() throws Throwable {
        ReceiptHandle handle = create(createMessageExt(MixAll.RETRY_GROUP_TOPIC_PREFIX + TOPIC, "", 0, 3000));
        assertNotNull(handle);

        ArgumentCaptor<ChangeInvisibleTimeRequestHeader> requestHeaderArgumentCaptor = ArgumentCaptor.forClass(ChangeInvisibleTimeRequestHeader.class);
        AckResult innerAckResult = new AckResult();
        innerAckResult.setStatus(AckStatus.OK);
        when(this.messageService.changeInvisibleTime(any(), any(), anyString(), requestHeaderArgumentCaptor.capture(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(innerAckResult));

        AckResult ackResult = this.consumerProcessor.changeInvisibleTime(createContext(), handle, MessageClientIDSetter.createUniqID(),
            CONSUMER_GROUP, TOPIC, 1000, null, 3000, true).get();

        assertEquals(AckStatus.OK, ackResult.getStatus());
        assertEquals(KeyBuilder.buildPopRetryTopic(TOPIC, CONSUMER_GROUP, new BrokerConfig().isEnableRetryTopicV2()), requestHeaderArgumentCaptor.getValue().getTopic());
        assertEquals(CONSUMER_GROUP, requestHeaderArgumentCaptor.getValue().getConsumerGroup());
        assertEquals(1000, requestHeaderArgumentCaptor.getValue().getInvisibleTime().longValue());
        assertEquals(handle.getReceiptHandle(), requestHeaderArgumentCaptor.getValue().getExtraInfo());
    }

    @Test
    public void testChangeInvisibleTimeShouldPreservePopTimeWhenExtraInfoUpdated() throws Throwable {
        ReceiptHandle handle = create(createMessageExt(MixAll.RETRY_GROUP_TOPIC_PREFIX + TOPIC, "", 0, 3000));
        assertNotNull(handle);

        long popTime = 1777203436411L;
        String newExtraInfo = "newExtraInfo";
        AckResult innerAckResult = new AckResult();
        innerAckResult.setStatus(AckStatus.OK);
        innerAckResult.setPopTime(popTime);
        innerAckResult.setExtraInfo(newExtraInfo);
        when(this.messageService.changeInvisibleTime(any(), any(), anyString(), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(innerAckResult));

        AckResult ackResult = this.consumerProcessor.changeInvisibleTime(createContext(), handle,
            MessageClientIDSetter.createUniqID(), CONSUMER_GROUP, TOPIC, 1000, null, 3000, true).get();

        assertEquals(AckStatus.OK, ackResult.getStatus());
        assertEquals(newExtraInfo + MessageConst.KEY_SEPARATOR + handle.getCommitLogOffset(), ackResult.getExtraInfo());
        assertEquals(popTime, ackResult.getPopTime());
    }

    @Test
    public void testLockBatch() throws Throwable {
        Set<MessageQueue> mqSet = new HashSet<>();
        MessageQueue mq1 = new MessageQueue(TOPIC, "broker1", 0);
        AddressableMessageQueue addressableMessageQueue1 = new AddressableMessageQueue(mq1, "127.0.0.1");
        MessageQueue mq2 = new MessageQueue(TOPIC, "broker2", 0);
        AddressableMessageQueue addressableMessageQueue2 = new AddressableMessageQueue(mq2, "127.0.0.1");
        mqSet.add(mq1);
        mqSet.add(mq2);
        when(this.topicRouteService.buildAddressableMessageQueue(any(), any())).thenAnswer(i -> new AddressableMessageQueue((MessageQueue) i.getArguments()[1], "127.0.0.1"));
        when(this.messageService.lockBatchMQ(any(), eq(addressableMessageQueue1), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(Sets.newHashSet(mq1)));
        when(this.messageService.lockBatchMQ(any(), eq(addressableMessageQueue2), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(Sets.newHashSet(mq2)));
        Set<MessageQueue> result = this.consumerProcessor.lockBatchMQ(ProxyContext.create(), mqSet, CONSUMER_GROUP, CLIENT_ID, 1000)
            .get();
        assertThat(result).isEqualTo(mqSet);
    }

    @Test
    public void testLockBatchPartialSuccess() throws Throwable {
        Set<MessageQueue> mqSet = new HashSet<>();
        MessageQueue mq1 = new MessageQueue(TOPIC, "broker1", 0);
        AddressableMessageQueue addressableMessageQueue1 = new AddressableMessageQueue(mq1, "127.0.0.1");
        MessageQueue mq2 = new MessageQueue(TOPIC, "broker2", 0);
        AddressableMessageQueue addressableMessageQueue2 = new AddressableMessageQueue(mq2, "127.0.0.1");
        mqSet.add(mq1);
        mqSet.add(mq2);
        when(this.topicRouteService.buildAddressableMessageQueue(any(), any())).thenAnswer(i -> new AddressableMessageQueue((MessageQueue) i.getArguments()[1], "127.0.0.1"));
        when(this.messageService.lockBatchMQ(any(), eq(addressableMessageQueue1), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(Sets.newHashSet(mq1)));
        when(this.messageService.lockBatchMQ(any(), eq(addressableMessageQueue2), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(Sets.newHashSet()));
        Set<MessageQueue> result = this.consumerProcessor.lockBatchMQ(ProxyContext.create(), mqSet, CONSUMER_GROUP, CLIENT_ID, 1000)
            .get();
        assertThat(result).isEqualTo(Sets.newHashSet(mq1));
    }

    @Test
    public void testLockBatchPartialSuccessWithException() throws Throwable {
        Set<MessageQueue> mqSet = new HashSet<>();
        MessageQueue mq1 = new MessageQueue(TOPIC, "broker1", 0);
        AddressableMessageQueue addressableMessageQueue1 = new AddressableMessageQueue(mq1, "127.0.0.1");
        MessageQueue mq2 = new MessageQueue(TOPIC, "broker2", 0);
        AddressableMessageQueue addressableMessageQueue2 = new AddressableMessageQueue(mq2, "127.0.0.1");
        mqSet.add(mq1);
        mqSet.add(mq2);
        when(this.topicRouteService.buildAddressableMessageQueue(any(), any())).thenAnswer(i -> new AddressableMessageQueue((MessageQueue) i.getArguments()[1], "127.0.0.1"));
        when(this.messageService.lockBatchMQ(any(), eq(addressableMessageQueue1), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(Sets.newHashSet(mq1)));
        CompletableFuture<Set<MessageQueue>> future = new CompletableFuture<>();
        future.completeExceptionally(new MQBrokerException(1, "err"));
        when(this.messageService.lockBatchMQ(any(), eq(addressableMessageQueue2), any(), anyLong()))
            .thenReturn(future);
        Set<MessageQueue> result = this.consumerProcessor.lockBatchMQ(ProxyContext.create(), mqSet, CONSUMER_GROUP, CLIENT_ID, 1000)
            .get();
        assertThat(result).isEqualTo(Sets.newHashSet(mq1));
    }

    @Test
    public void testPopMessageWithToReturnFilter() throws Throwable {
        final String tag = "tag";
        final long invisibleTime = Duration.ofSeconds(15).toMillis();
        ArgumentCaptor<AddressableMessageQueue> messageQueueArgumentCaptor = ArgumentCaptor.forClass(AddressableMessageQueue.class);
        ArgumentCaptor<PopMessageRequestHeader> requestHeaderArgumentCaptor = ArgumentCaptor.forClass(PopMessageRequestHeader.class);

        List<MessageExt> messageExtList = new ArrayList<>();
        messageExtList.add(createMessageExt(TOPIC, tag, 0, invisibleTime));
        PopResult innerPopResult = new PopResult(PopStatus.FOUND, messageExtList);
        when(this.messageService.popMessage(any(), messageQueueArgumentCaptor.capture(), requestHeaderArgumentCaptor.capture(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(innerPopResult));

        when(this.topicRouteService.getCurrentMessageQueueView(any(), anyString()))
            .thenReturn(mock(MessageQueueView.class));

        ArgumentCaptor<String> ackMessageIdArgumentCaptor = ArgumentCaptor.forClass(String.class);
        when(this.messagingProcessor.ackMessage(any(), any(), ackMessageIdArgumentCaptor.capture(), anyString(), anyString(), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(mock(AckResult.class)));

        ArgumentCaptor<String> changeInvisibleTimeMessageIdArgumentCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Long> changeInvisibleTimeInvisibleTimeArgumentCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Boolean> changeInvisibleTimeSuspendArgumentCaptor = ArgumentCaptor.forClass(Boolean.class);
        when(this.messagingProcessor.changeInvisibleTime(any(), any(), changeInvisibleTimeMessageIdArgumentCaptor.capture(),
            anyString(), anyString(), changeInvisibleTimeInvisibleTimeArgumentCaptor.capture(), any(), anyLong(),
            changeInvisibleTimeSuspendArgumentCaptor.capture()))
            .thenReturn(CompletableFuture.completedFuture(mock(AckResult.class)));

        AddressableMessageQueue messageQueue = mock(AddressableMessageQueue.class);
        PopResult popResult = this.consumerProcessor.popMessage(
            createContext(),
            (ctx, messageQueueView) -> messageQueue,
            CONSUMER_GROUP,
            TOPIC,
            60,
            invisibleTime,
            Duration.ofSeconds(3).toMillis(),
            ConsumeInitMode.MAX,
            FilterAPI.build(TOPIC, tag, ExpressionType.TAG),
            false,
            (ctx, consumerGroup, subscriptionData, messageExt) -> {
                // Return TO_RETURN for the message
                return PopMessageResultFilter.FilterResult.TO_RETURN;
            },
            null,
            Duration.ofSeconds(3).toMillis()
        ).get();

        // Verify that changeInvisibleTime was called with suspend=true
        verify(this.messagingProcessor).changeInvisibleTime(any(), any(), eq(messageExtList.get(0).getMsgId()),
            eq(CONSUMER_GROUP), eq(TOPIC), eq(Duration.ofSeconds(1).toMillis()), eq(null),
            eq(MessagingProcessor.DEFAULT_TIMEOUT_MILLS), eq(true));

        // Verify that the message was NOT added to the result list
        assertEquals(PopStatus.FOUND, popResult.getPopStatus());
        assertEquals(0, popResult.getMsgFoundList().size());
    }

    @Test
    public void testChangeInvisibleTimeWithSuspendFalse() throws Throwable {
        ReceiptHandle handle = create(createMessageExt(MixAll.RETRY_GROUP_TOPIC_PREFIX + TOPIC, "", 0, 3000));
        assertNotNull(handle);

        ArgumentCaptor<ChangeInvisibleTimeRequestHeader> requestHeaderArgumentCaptor = ArgumentCaptor.forClass(ChangeInvisibleTimeRequestHeader.class);
        AckResult innerAckResult = new AckResult();
        innerAckResult.setStatus(AckStatus.OK);
        when(this.messageService.changeInvisibleTime(any(), any(), anyString(), requestHeaderArgumentCaptor.capture(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(innerAckResult));

        AckResult ackResult = this.consumerProcessor.changeInvisibleTime(createContext(), handle, MessageClientIDSetter.createUniqID(),
            CONSUMER_GROUP, TOPIC, 1000, null, 3000, false).get();

        assertEquals(AckStatus.OK, ackResult.getStatus());
        assertEquals(KeyBuilder.buildPopRetryTopic(TOPIC, CONSUMER_GROUP, new BrokerConfig().isEnableRetryTopicV2()), requestHeaderArgumentCaptor.getValue().getTopic());
        assertEquals(CONSUMER_GROUP, requestHeaderArgumentCaptor.getValue().getConsumerGroup());
        assertEquals(1000, requestHeaderArgumentCaptor.getValue().getInvisibleTime().longValue());
        assertEquals(handle.getReceiptHandle(), requestHeaderArgumentCaptor.getValue().getExtraInfo());
        assertFalse("Suspend should be false", requestHeaderArgumentCaptor.getValue().isSuspend());
    }

    @Test
    public void testChangeInvisibleTimeWithSuspendTrue() throws Throwable {
        ReceiptHandle handle = create(createMessageExt(MixAll.RETRY_GROUP_TOPIC_PREFIX + TOPIC, "", 0, 3000));
        assertNotNull(handle);

        ArgumentCaptor<ChangeInvisibleTimeRequestHeader> requestHeaderArgumentCaptor = ArgumentCaptor.forClass(ChangeInvisibleTimeRequestHeader.class);
        AckResult innerAckResult = new AckResult();
        innerAckResult.setStatus(AckStatus.OK);
        when(this.messageService.changeInvisibleTime(any(), any(), anyString(), requestHeaderArgumentCaptor.capture(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(innerAckResult));

        AckResult ackResult = this.consumerProcessor.changeInvisibleTime(createContext(), handle, MessageClientIDSetter.createUniqID(),
            CONSUMER_GROUP, TOPIC, 1000, null, 3000, true).get();

        assertEquals(AckStatus.OK, ackResult.getStatus());
        assertEquals(KeyBuilder.buildPopRetryTopic(TOPIC, CONSUMER_GROUP, new BrokerConfig().isEnableRetryTopicV2()), requestHeaderArgumentCaptor.getValue().getTopic());
        assertEquals(CONSUMER_GROUP, requestHeaderArgumentCaptor.getValue().getConsumerGroup());
        assertEquals(1000, requestHeaderArgumentCaptor.getValue().getInvisibleTime().longValue());
        assertEquals(handle.getReceiptHandle(), requestHeaderArgumentCaptor.getValue().getExtraInfo());
        assertTrue("Suspend should be true", requestHeaderArgumentCaptor.getValue().isSuspend());
    }

    // ==================== peekLiteMessage tests ====================

    private static final String PARENT_TOPIC = "parentTopic";
    private static final String LITE_TOPIC = "liteTopic";
    private static final String BROKER_A = "broker-a";
    private static final String BROKER_B = "broker-b";

    private AddressableMessageQueue buildBrokerActingQueue(String topic, String brokerName) {
        return new AddressableMessageQueue(
            new MessageQueue(topic, brokerName, -1), "127.0.0.1:10911");
    }

    private void mockReadQueues(List<AddressableMessageQueue> queues) {
        MessageQueueView view = mock(MessageQueueView.class);
        MessageQueueSelector selector = mock(MessageQueueSelector.class);
        when(view.getReadSelector()).thenReturn(selector);
        when(selector.getBrokerActingQueues()).thenReturn(queues);
        try {
            when(this.topicRouteService.getAllMessageQueueView(any(), eq(PARENT_TOPIC))).thenReturn(view);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private MessageExt createPeekMessageExt(String brokerName, long queueOffset, long storeTimestamp) {
        MessageExt msg = new MessageExt();
        msg.setTopic(PARENT_TOPIC);
        msg.setBrokerName(brokerName);
        msg.setQueueOffset(queueOffset);
        msg.setStoreTimestamp(storeTimestamp);
        msg.setMsgId(MessageClientIDSetter.createUniqID());
        return msg;
    }

    // --- Task 1: parameter validation ---

    @Test(expected = IllegalArgumentException.class)
    public void testPeekLiteMessage_maxMsgNumsZero() {
        this.consumerProcessor.peekLiteMessage(
            createContext(), CONSUMER_GROUP, PARENT_TOPIC, LITE_TOPIC,
            0, new OffsetOption(OffsetOption.Type.POLICY, 0), PeekDirection.FORWARD, 3000);
    }

    @Test
    public void testPeekLiteMessage_maxMsgNumsCapped() throws Throwable {
        AddressableMessageQueue queue = buildBrokerActingQueue(PARENT_TOPIC, BROKER_A);
        mockReadQueues(Collections.singletonList(queue));

        PopResult emptyPopResult = new PopResult(PopStatus.FOUND, new ArrayList<>());
        ArgumentCaptor<PeekLiteMessageRequestHeader> headerCaptor =
            ArgumentCaptor.forClass(PeekLiteMessageRequestHeader.class);
        when(this.messageService.peekLiteMessage(any(), any(), headerCaptor.capture(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(emptyPopResult));

        this.consumerProcessor.peekLiteMessage(
            createContext(), CONSUMER_GROUP, PARENT_TOPIC, LITE_TOPIC,
            999, new OffsetOption(OffsetOption.Type.POLICY, 0), PeekDirection.FORWARD, 3000).get();

        assertEquals(ProxyUtils.MAX_MSG_NUMS_FOR_POP_REQUEST, headerCaptor.getValue().getMaxMsgNum());
    }

    // --- Task 2: route and queue ---

    @Test
    public void testPeekLiteMessage_noReadableQueue() {
        mockReadQueues(Collections.emptyList());

        CompletableFuture<PeekResult> future = this.consumerProcessor.peekLiteMessage(
            createContext(), CONSUMER_GROUP, PARENT_TOPIC, LITE_TOPIC,
            10, new OffsetOption(OffsetOption.Type.POLICY, 0), PeekDirection.FORWARD, 3000);

        assertTrue(future.isCompletedExceptionally());
        try {
            future.get();
            fail("Expected ExecutionException");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof ProxyException);
            assertEquals(ProxyExceptionCode.FORBIDDEN, ((ProxyException) e.getCause()).getCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    public void testPeekLiteMessage_nullReadableQueue() {
        mockReadQueues(null);

        CompletableFuture<PeekResult> future = this.consumerProcessor.peekLiteMessage(
            createContext(), CONSUMER_GROUP, PARENT_TOPIC, LITE_TOPIC,
            10, new OffsetOption(OffsetOption.Type.POLICY, 0), PeekDirection.FORWARD, 3000);

        assertTrue(future.isCompletedExceptionally());
        try {
            future.get();
            fail("Expected ExecutionException");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof ProxyException);
            assertEquals(ProxyExceptionCode.FORBIDDEN, ((ProxyException) e.getCause()).getCode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // --- Task 3: non-CURSOR OffsetOption header building ---

    @Test
    public void testPeekLiteMessage_forwardWithOffsetOption() throws Throwable {
        AddressableMessageQueue queue = buildBrokerActingQueue(PARENT_TOPIC, BROKER_A);
        mockReadQueues(Collections.singletonList(queue));

        PopResult emptyPopResult = new PopResult(PopStatus.FOUND, new ArrayList<>());
        ArgumentCaptor<PeekLiteMessageRequestHeader> headerCaptor =
            ArgumentCaptor.forClass(PeekLiteMessageRequestHeader.class);
        when(this.messageService.peekLiteMessage(any(), any(), headerCaptor.capture(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(emptyPopResult));

        OffsetOption offsetOption = new OffsetOption(OffsetOption.Type.OFFSET, 100L);
        this.consumerProcessor.peekLiteMessage(
            createContext(), CONSUMER_GROUP, PARENT_TOPIC, LITE_TOPIC,
            10, offsetOption, PeekDirection.FORWARD, 3000).get();

        PeekLiteMessageRequestHeader header = headerCaptor.getValue();
        assertEquals("OFFSET", header.getOffsetOptionType());
        assertEquals(100L, header.getOffsetOptionValue());
        assertEquals("FORWARD", header.getPeekDirection());
        assertEquals(PARENT_TOPIC, header.getParentTopic());
        assertEquals(LITE_TOPIC, header.getLiteTopic());
        assertEquals(CONSUMER_GROUP, header.getConsumerGroup());
    }

    // --- Task 4: CURSOR type header building ---

    @Test
    public void testPeekLiteMessage_cursorForward() throws Throwable {
        AddressableMessageQueue queue = buildBrokerActingQueue(PARENT_TOPIC, BROKER_A);
        mockReadQueues(Collections.singletonList(queue));

        PopResult emptyPopResult = new PopResult(PopStatus.FOUND, new ArrayList<>());
        ArgumentCaptor<PeekLiteMessageRequestHeader> headerCaptor =
            ArgumentCaptor.forClass(PeekLiteMessageRequestHeader.class);
        when(this.messageService.peekLiteMessage(any(), any(), headerCaptor.capture(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(emptyPopResult));

        Map<String, long[]> ranges = new HashMap<>();
        ranges.put(BROKER_A, new long[]{10, 20});
        Cursor cursor = new Cursor(ranges);
        OffsetOption offsetOption = OffsetOption.ofCursor(cursor);

        this.consumerProcessor.peekLiteMessage(
            createContext(), CONSUMER_GROUP, PARENT_TOPIC, LITE_TOPIC,
            10, offsetOption, PeekDirection.FORWARD, 3000).get();

        PeekLiteMessageRequestHeader header = headerCaptor.getValue();
        assertEquals("OFFSET", header.getOffsetOptionType());
        assertEquals(20L, header.getOffsetOptionValue()); // range[1]
    }

    @Test
    public void testPeekLiteMessage_cursorBackward() throws Throwable {
        AddressableMessageQueue queue = buildBrokerActingQueue(PARENT_TOPIC, BROKER_A);
        mockReadQueues(Collections.singletonList(queue));

        PopResult emptyPopResult = new PopResult(PopStatus.FOUND, new ArrayList<>());
        ArgumentCaptor<PeekLiteMessageRequestHeader> headerCaptor =
            ArgumentCaptor.forClass(PeekLiteMessageRequestHeader.class);
        when(this.messageService.peekLiteMessage(any(), any(), headerCaptor.capture(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(emptyPopResult));

        Map<String, long[]> ranges = new HashMap<>();
        ranges.put(BROKER_A, new long[]{10, 20});
        Cursor cursor = new Cursor(ranges);
        OffsetOption offsetOption = OffsetOption.ofCursor(cursor);

        this.consumerProcessor.peekLiteMessage(
            createContext(), CONSUMER_GROUP, PARENT_TOPIC, LITE_TOPIC,
            10, offsetOption, PeekDirection.BACKWARD, 3000).get();

        PeekLiteMessageRequestHeader header = headerCaptor.getValue();
        assertEquals("OFFSET", header.getOffsetOptionType());
        assertEquals(9L, header.getOffsetOptionValue()); // range[0] - 1
    }

    @Test
    public void testPeekLiteMessage_cursorBrokerNotInCursor() throws Throwable {
        AddressableMessageQueue queue = buildBrokerActingQueue(PARENT_TOPIC, BROKER_A);
        mockReadQueues(Collections.singletonList(queue));

        // Cursor only contains broker-b, so broker-a should be skipped
        Map<String, long[]> ranges = new HashMap<>();
        ranges.put(BROKER_B, new long[]{10, 20});
        Cursor cursor = new Cursor(ranges);
        OffsetOption offsetOption = OffsetOption.ofCursor(cursor);

        PeekResult result = this.consumerProcessor.peekLiteMessage(
            createContext(), CONSUMER_GROUP, PARENT_TOPIC, LITE_TOPIC,
            10, offsetOption, PeekDirection.FORWARD, 3000).get();

        // broker-a was skipped → no requests sent → empty result
        verify(this.messageService, never()).peekLiteMessage(any(), any(), any(), anyLong());
        assertEquals(PopStatus.FOUND, result.getPopStatus());
        assertTrue(result.getMsgFoundList().isEmpty());
        assertNull(result.getCursor());
    }

    // --- Task 5: result aggregation ---

    @Test
    public void testPeekLiteMessage_emptyResult() throws Throwable {
        AddressableMessageQueue queue = buildBrokerActingQueue(PARENT_TOPIC, BROKER_A);
        mockReadQueues(Collections.singletonList(queue));

        PopResult emptyPopResult = new PopResult(PopStatus.FOUND, new ArrayList<>());
        emptyPopResult.setRestNum(0);
        when(this.messageService.peekLiteMessage(any(), any(), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(emptyPopResult));

        PeekResult result = this.consumerProcessor.peekLiteMessage(
            createContext(), CONSUMER_GROUP, PARENT_TOPIC, LITE_TOPIC,
            10, new OffsetOption(OffsetOption.Type.POLICY, 0), PeekDirection.FORWARD, 3000).get();

        assertEquals(PopStatus.FOUND, result.getPopStatus());
        assertTrue(result.getMsgFoundList().isEmpty());
        assertNull(result.getCursor());
        assertEquals(0, result.getRestNum());
    }

    @Test
    public void testPeekLiteMessage_forwardSortAndTruncate() throws Throwable {
        AddressableMessageQueue queueA = buildBrokerActingQueue(PARENT_TOPIC, BROKER_A);
        AddressableMessageQueue queueB = buildBrokerActingQueue(PARENT_TOPIC, BROKER_B);
        mockReadQueues(java.util.Arrays.asList(queueA, queueB));

        // broker-a: 3 messages with timestamps 100, 300, 500
        List<MessageExt> msgsA = new ArrayList<>();
        msgsA.add(createPeekMessageExt(BROKER_A, 0, 100));
        msgsA.add(createPeekMessageExt(BROKER_A, 1, 300));
        msgsA.add(createPeekMessageExt(BROKER_A, 2, 500));
        PopResult resultA = new PopResult(PopStatus.FOUND, msgsA);
        resultA.setRestNum(5);

        // broker-b: 3 messages with timestamps 200, 400, 600
        List<MessageExt> msgsB = new ArrayList<>();
        msgsB.add(createPeekMessageExt(BROKER_B, 0, 200));
        msgsB.add(createPeekMessageExt(BROKER_B, 1, 400));
        msgsB.add(createPeekMessageExt(BROKER_B, 2, 600));
        PopResult resultB = new PopResult(PopStatus.FOUND, msgsB);
        resultB.setRestNum(3);

        doAnswer((Answer<CompletableFuture<PopResult>>) invocation -> {
            AddressableMessageQueue mq = invocation.getArgument(1);
            if (BROKER_A.equals(mq.getBrokerName())) {
                return CompletableFuture.completedFuture(resultA);
            }
            return CompletableFuture.completedFuture(resultB);
        }).when(this.messageService).peekLiteMessage(any(), any(), any(), anyLong());

        PeekResult result = this.consumerProcessor.peekLiteMessage(
            createContext(), CONSUMER_GROUP, PARENT_TOPIC, LITE_TOPIC,
            4, new OffsetOption(OffsetOption.Type.POLICY, 0), PeekDirection.FORWARD, 3000).get();

        assertEquals(PopStatus.FOUND, result.getPopStatus());
        assertEquals(4, result.getMsgFoundList().size());
        // FORWARD: ascending by storeTimestamp → 100, 200, 300, 400
        assertEquals(100, result.getMsgFoundList().get(0).getStoreTimestamp());
        assertEquals(200, result.getMsgFoundList().get(1).getStoreTimestamp());
        assertEquals(300, result.getMsgFoundList().get(2).getStoreTimestamp());
        assertEquals(400, result.getMsgFoundList().get(3).getStoreTimestamp());
        // restNum = broker-side(5+3) + truncated(6-4) = 10
        assertEquals(10, result.getRestNum());
        assertNotNull(result.getCursor());
    }

    @Test
    public void testPeekLiteMessage_backwardSort() throws Throwable {
        AddressableMessageQueue queue = buildBrokerActingQueue(PARENT_TOPIC, BROKER_A);
        mockReadQueues(Collections.singletonList(queue));

        List<MessageExt> msgs = new ArrayList<>();
        msgs.add(createPeekMessageExt(BROKER_A, 0, 100));
        msgs.add(createPeekMessageExt(BROKER_A, 1, 300));
        msgs.add(createPeekMessageExt(BROKER_A, 2, 500));
        PopResult popResult = new PopResult(PopStatus.FOUND, msgs);
        popResult.setRestNum(0);
        when(this.messageService.peekLiteMessage(any(), any(), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(popResult));

        PeekResult result = this.consumerProcessor.peekLiteMessage(
            createContext(), CONSUMER_GROUP, PARENT_TOPIC, LITE_TOPIC,
            10, new OffsetOption(OffsetOption.Type.POLICY, 0), PeekDirection.BACKWARD, 3000).get();

        assertEquals(3, result.getMsgFoundList().size());
        // BACKWARD: descending by storeTimestamp → 500, 300, 100
        assertEquals(500, result.getMsgFoundList().get(0).getStoreTimestamp());
        assertEquals(300, result.getMsgFoundList().get(1).getStoreTimestamp());
        assertEquals(100, result.getMsgFoundList().get(2).getStoreTimestamp());
    }

    @Test
    public void testPeekLiteMessage_cursorBuiltFromMessages() throws Throwable {
        AddressableMessageQueue queue = buildBrokerActingQueue(PARENT_TOPIC, BROKER_A);
        mockReadQueues(Collections.singletonList(queue));

        List<MessageExt> msgs = new ArrayList<>();
        msgs.add(createPeekMessageExt(BROKER_A, 5, 100));
        msgs.add(createPeekMessageExt(BROKER_A, 6, 200));
        msgs.add(createPeekMessageExt(BROKER_A, 7, 300));
        PopResult popResult = new PopResult(PopStatus.FOUND, msgs);
        popResult.setRestNum(0);
        when(this.messageService.peekLiteMessage(any(), any(), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(popResult));

        PeekResult result = this.consumerProcessor.peekLiteMessage(
            createContext(), CONSUMER_GROUP, PARENT_TOPIC, LITE_TOPIC,
            10, new OffsetOption(OffsetOption.Type.POLICY, 0), PeekDirection.FORWARD, 3000).get();

        assertNotNull(result.getCursor());
        long[] range = result.getCursor().getRange(BROKER_A);
        assertNotNull(range);
        // [begin, end) = [5, 8)
        assertEquals(5, range[0]);
        assertEquals(8, range[1]);
    }

    // --- Task 6: exception handling ---

    @Test
    public void testPeekLiteMessage_brokerFutureException() {
        AddressableMessageQueue queue = buildBrokerActingQueue(PARENT_TOPIC, BROKER_A);
        mockReadQueues(Collections.singletonList(queue));

        CompletableFuture<PopResult> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("broker timeout"));
        when(this.messageService.peekLiteMessage(any(), any(), any(), anyLong()))
            .thenReturn(failedFuture);

        CompletableFuture<PeekResult> future = this.consumerProcessor.peekLiteMessage(
            createContext(), CONSUMER_GROUP, PARENT_TOPIC, LITE_TOPIC,
            10, new OffsetOption(OffsetOption.Type.POLICY, 0), PeekDirection.FORWARD, 3000);

        assertTrue(future.isCompletedExceptionally());
        try {
            future.get();
            fail("Expected ExecutionException");
        } catch (ExecutionException e) {
            // allOf propagates the broker failure as-is through the future chain
            assertNotNull(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
