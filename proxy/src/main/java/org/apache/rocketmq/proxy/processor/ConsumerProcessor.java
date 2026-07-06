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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.PeekResult;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.lite.OffsetOption;
import org.apache.rocketmq.common.lite.PeekDirection;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageClientExt;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.utils.FutureUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;
import org.apache.rocketmq.proxy.common.utils.ProxyUtils;
import org.apache.rocketmq.common.lite.Cursor;
import org.apache.rocketmq.proxy.service.ServiceManager;
import org.apache.rocketmq.proxy.service.message.ReceiptHandleMessage;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMaxOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMinOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopLiteMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PeekLiteMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

public class ConsumerProcessor extends AbstractProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private final ExecutorService executor;

    public ConsumerProcessor(MessagingProcessor messagingProcessor, ServiceManager serviceManager,
        ExecutorService executor) {
        super(messagingProcessor, serviceManager);
        this.executor = executor;
    }

    public CompletableFuture<PopResult> popMessage(
        ProxyContext ctx,
        QueueSelector queueSelector,
        String consumerGroup,
        String topic,
        int maxMsgNums,
        long invisibleTime,
        long pollTime,
        int initMode,
        SubscriptionData subscriptionData,
        boolean fifo,
        PopMessageResultFilter popMessageResultFilter,
        String attemptId,
        long timeoutMillis
    ) {
        CompletableFuture<PopResult> future = new CompletableFuture<>();
        try {
            AddressableMessageQueue messageQueue = queueSelector.select(ctx, this.serviceManager.getTopicRouteService().getCurrentMessageQueueView(ctx, topic));
            if (messageQueue == null) {
                throw new ProxyException(ProxyExceptionCode.FORBIDDEN, "no readable queue");
            }
            return popMessage(ctx, messageQueue, consumerGroup, topic, maxMsgNums, invisibleTime, pollTime, initMode,
                subscriptionData, fifo, popMessageResultFilter, attemptId, timeoutMillis);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    public CompletableFuture<PopResult> popMessage(
        ProxyContext ctx,
        AddressableMessageQueue messageQueue,
        String consumerGroup,
        String topic,
        int maxMsgNums,
        long invisibleTime,
        long pollTime,
        int initMode,
        SubscriptionData subscriptionData,
        boolean fifo,
        PopMessageResultFilter popMessageResultFilter,
        String attemptId,
        long timeoutMillis
    ) {
        CompletableFuture<PopResult> future = new CompletableFuture<>();
        try {
            if (maxMsgNums > ProxyUtils.MAX_MSG_NUMS_FOR_POP_REQUEST) {
                log.warn("change maxNums from {} to {} for pop request, with info: topic:{}, group:{}",
                    maxMsgNums, ProxyUtils.MAX_MSG_NUMS_FOR_POP_REQUEST, topic, consumerGroup);
                maxMsgNums = ProxyUtils.MAX_MSG_NUMS_FOR_POP_REQUEST;
            }

            PopMessageRequestHeader requestHeader = new PopMessageRequestHeader();
            requestHeader.setConsumerGroup(consumerGroup);
            requestHeader.setTopic(topic);
            requestHeader.setQueueId(messageQueue.getQueueId());
            requestHeader.setMaxMsgNums(maxMsgNums);
            requestHeader.setInvisibleTime(invisibleTime);
            requestHeader.setPollTime(pollTime);
            requestHeader.setInitMode(initMode);
            requestHeader.setExpType(subscriptionData.getExpressionType());
            requestHeader.setExp(subscriptionData.getSubString());
            requestHeader.setOrder(fifo);
            requestHeader.setAttemptId(attemptId);

            future = this.serviceManager.getMessageService().popMessage(
                    ctx,
                    messageQueue,
                    requestHeader,
                    timeoutMillis)
                .thenApplyAsync(popResult -> filterPopResult(ctx, popResult,
                    requestHeader, consumerGroup, topic, subscriptionData, popMessageResultFilter), this.executor);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return FutureUtils.addExecutor(future, this.executor);
    }

    private PopResult filterPopResult(ProxyContext ctx, PopResult popResult, CommandCustomHeader requestHeader,
        String consumerGroup, String topic, SubscriptionData subscriptionData,
        PopMessageResultFilter popMessageResultFilter) {
        if (PopStatus.FOUND.equals(popResult.getPopStatus()) &&
            !CollectionUtils.isEmpty(popResult.getMsgFoundList()) &&
            popMessageResultFilter != null) {

            List<MessageExt> messageExtList = new ArrayList<>();
            for (MessageExt messageExt : popResult.getMsgFoundList()) {
                try {
                    fillUniqIDIfNeed(messageExt);
                    String handleString = createHandle(messageExt.getProperty(MessageConst.PROPERTY_POP_CK), messageExt.getCommitLogOffset());
                    if (handleString == null) {
                        log.error("[BUG] pop message from broker but handle is empty. requestHeader:{}, msg:{}", requestHeader, messageExt);
                        messageExtList.add(messageExt);
                        continue;
                    }
                    MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_POP_CK, handleString);

                    String liteTopic = messageExt.getProperty(MessageConst.PROPERTY_LITE_TOPIC);

                    PopMessageResultFilter.FilterResult filterResult =
                        popMessageResultFilter.filterMessage(ctx, consumerGroup, subscriptionData, messageExt);
                    switch (filterResult) {
                        case NO_MATCH:
                            this.messagingProcessor.ackMessage(
                                ctx,
                                ReceiptHandle.decode(handleString),
                                messageExt.getMsgId(),
                                consumerGroup,
                                topic,
                                liteTopic,
                                MessagingProcessor.DEFAULT_TIMEOUT_MILLS);
                            break;
                        case TO_DLQ:
                            this.messagingProcessor.forwardMessageToDeadLetterQueue(
                                ctx,
                                ReceiptHandle.decode(handleString),
                                messageExt.getMsgId(),
                                consumerGroup,
                                topic,
                                liteTopic,
                                MessagingProcessor.DEFAULT_TIMEOUT_MILLS);
                            break;
                        case TO_RETURN:
                            this.messagingProcessor.changeInvisibleTime(
                                    ctx,
                                    ReceiptHandle.decode(handleString),
                                    messageExt.getMsgId(),
                                    consumerGroup,
                                    topic,
                                    MessagingProcessor.INVISIBLE_TIME_MS,
                                    null,
                                    MessagingProcessor.DEFAULT_TIMEOUT_MILLS,
                                    true);
                            break;
                        case MATCH:
                        default:
                            messageExtList.add(messageExt);
                            break;
                    }
                } catch (Throwable t) {
                    log.error("process filterMessage failed. requestHeader:{}, msg:{}", requestHeader, messageExt, t);
                    messageExtList.add(messageExt);
                }
            }
            popResult.setMsgFoundList(messageExtList);
        }
        return popResult;
    }

    public CompletableFuture<PopResult> popLiteMessage(
        ProxyContext ctx,
        QueueSelector queueSelector,
        String consumerGroup,
        String topic,
        int maxMsgNums,
        long invisibleTime,
        long pollTime,
        SubscriptionData subscriptionData,
        PopMessageResultFilter popMessageResultFilter,
        String attemptId,
        long timeoutMillis
    ) {
        CompletableFuture<PopResult> future = new CompletableFuture<>();
        try {
            AddressableMessageQueue messageQueue = queueSelector.select(ctx,
                this.serviceManager.getTopicRouteService().getCurrentMessageQueueView(ctx, topic));
            if (messageQueue == null) {
                throw new ProxyException(ProxyExceptionCode.FORBIDDEN, "no readable queue");
            }
            return doPopLiteMessage(ctx, messageQueue, consumerGroup, topic, maxMsgNums, invisibleTime, pollTime,
                subscriptionData, popMessageResultFilter, attemptId, timeoutMillis);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    private CompletableFuture<PopResult> doPopLiteMessage(
        ProxyContext ctx,
        AddressableMessageQueue messageQueue,
        String consumerGroup,
        String topic,
        int maxMsgNums,
        long invisibleTime,
        long pollTime,
        SubscriptionData subscriptionData,
        PopMessageResultFilter popMessageResultFilter,
        String attemptId,
        long timeoutMillis
    ) {
        CompletableFuture<PopResult> future = new CompletableFuture<>();
        try {
            if (maxMsgNums > ProxyUtils.MAX_MSG_NUMS_FOR_POP_REQUEST) {
                log.warn("change maxNums from {} to {} for pop request, with info: topic:{}, group:{}",
                    maxMsgNums, ProxyUtils.MAX_MSG_NUMS_FOR_POP_REQUEST, topic, consumerGroup);
                maxMsgNums = ProxyUtils.MAX_MSG_NUMS_FOR_POP_REQUEST;
            }

            PopLiteMessageRequestHeader requestHeader = new PopLiteMessageRequestHeader();
            requestHeader.setClientId(ctx.getClientID());
            requestHeader.setConsumerGroup(consumerGroup);
            requestHeader.setTopic(topic);
            requestHeader.setMaxMsgNum(maxMsgNums);
            requestHeader.setInvisibleTime(invisibleTime);
            requestHeader.setPollTime(pollTime);
            requestHeader.setAttemptId(attemptId);
            requestHeader.setBornTime(System.currentTimeMillis());

            future = this.serviceManager.getMessageService().popLiteMessage(
                    ctx,
                    messageQueue,
                    requestHeader,
                    timeoutMillis)
                .thenApplyAsync(popResult -> filterPopResult(ctx, popResult,
                    requestHeader, consumerGroup, topic, subscriptionData, popMessageResultFilter), this.executor);
        } catch (Throwable t) {
            future.completeExceptionally(t);
            FutureUtils.addExecutor(future, this.executor);
        }
        return future;
    }

    public CompletableFuture<PeekResult> peekLiteMessage(
        ProxyContext ctx,
        String consumerGroup,
        String parentTopic,
        String liteTopic,
        int maxMsgNums,
        OffsetOption offsetOption,
        PeekDirection direction,
        long timeoutMillis
    ) {
        if (maxMsgNums <= 0) {
            throw new IllegalArgumentException("maxMsgNums must be positive, but got " + maxMsgNums);
        }
        if (maxMsgNums > ProxyUtils.MAX_MSG_NUMS_FOR_POP_REQUEST) {
            log.warn("change maxNums from {} to {} for peek lite request, topic:{}, group:{}",
                maxMsgNums, ProxyUtils.MAX_MSG_NUMS_FOR_POP_REQUEST, parentTopic, consumerGroup);
            maxMsgNums = ProxyUtils.MAX_MSG_NUMS_FOR_POP_REQUEST;
        }
        final int cappedMaxMsgNums = maxMsgNums;

        CompletableFuture<PeekResult> future = new CompletableFuture<>();
        try {
            MessageQueueView messageQueueView =
                this.serviceManager.getTopicRouteService().getAllMessageQueueView(ctx, parentTopic);
            List<AddressableMessageQueue> readQueues = messageQueueView.getReadSelector().getBrokerActingQueues();
            if (readQueues == null || readQueues.isEmpty()) {
                throw new ProxyException(ProxyExceptionCode.FORBIDDEN, "no readable queue");
            }

            Cursor cursor = Cursor.fromOffsetOption(offsetOption);

            Map<String, PeekLiteMessageRequestHeader> brokerHeaders =
                buildPeekBrokerHeaders(readQueues, cursor, parentTopic, liteTopic,
                    consumerGroup, cappedMaxMsgNums, offsetOption, direction);

            Map<String, CompletableFuture<PopResult>> brokerFutureMap =
                fanOutPeekRequests(ctx, readQueues, brokerHeaders, timeoutMillis);

            future = CompletableFuture.allOf(
                    brokerFutureMap.values().toArray(new CompletableFuture[0]))
                .thenApplyAsync(v -> buildPeekResult(
                    brokerFutureMap, cappedMaxMsgNums, direction), this.executor);
        } catch (Throwable t) {
            future.completeExceptionally(t);
            FutureUtils.addExecutor(future, this.executor);
        }
        return future;
    }

    private Map<String, PeekLiteMessageRequestHeader> buildPeekBrokerHeaders(
        List<AddressableMessageQueue> readQueues,
        Cursor cursor,
        String parentTopic,
        String liteTopic,
        String consumerGroup,
        int maxMsgNums,
        OffsetOption offsetOption,
        PeekDirection direction
    ) {
        Map<String, PeekLiteMessageRequestHeader> headers = new HashMap<>();
        for (AddressableMessageQueue queue : readQueues) {
            String brokerName = queue.getBrokerName();

            // Resolve offset option: from cursor range or original policy/timestamp
            String optionType;
            long optionValue;
            if (offsetOption.getType() == OffsetOption.Type.CURSOR) {
                long[] range = cursor.getRange(brokerName);
                if (range == null) {
                    // broker not in cursor — skip
                    continue;
                }
                // Half-open [begin, end): FORWARD uses end as inclusive start;
                // BACKWARD uses begin-1 (broker adds +1 to recover exclusive upper bound)
                optionValue = (direction == PeekDirection.FORWARD) ? range[1] : range[0] - 1;
                if (optionValue < 0) {
                    // backward past queue head — skip
                    continue;
                }
                optionType = OffsetOption.Type.OFFSET.name();
            } else {
                optionType = offsetOption.getType().name();
                optionValue = offsetOption.getValue();
            }

            PeekLiteMessageRequestHeader header = new PeekLiteMessageRequestHeader();
            header.setParentTopic(parentTopic);
            header.setLiteTopic(liteTopic);
            header.setConsumerGroup(consumerGroup);
            header.setMaxMsgNum(maxMsgNums);
            header.setPeekDirection(direction.name());
            header.setOffsetOptionType(optionType);
            header.setOffsetOptionValue(optionValue);
            headers.put(brokerName, header);
        }
        return headers;
    }

    private Map<String, CompletableFuture<PopResult>> fanOutPeekRequests(
        ProxyContext ctx,
        List<AddressableMessageQueue> readQueues,
        Map<String, PeekLiteMessageRequestHeader> brokerHeaders,
        long timeoutMillis
    ) {
        Map<String, CompletableFuture<PopResult>> brokerFutureMap = new HashMap<>();
        for (AddressableMessageQueue queue : readQueues) {
            String brokerName = queue.getBrokerName();
            if (!brokerHeaders.containsKey(brokerName)) {
                continue;
            }
            CompletableFuture<PopResult> bf = this.serviceManager.getMessageService()
                .peekLiteMessage(ctx, queue, brokerHeaders.get(brokerName), timeoutMillis);
            brokerFutureMap.put(brokerName, bf);
        }
        return brokerFutureMap;
    }

    private PeekResult buildPeekResult(
        Map<String, CompletableFuture<PopResult>> brokerFutureMap,
        int maxMsgNums,
        PeekDirection direction
    ) {
        List<MessageExt> mergedMessages = new ArrayList<>();
        long totalRestNum = 0;

        for (Map.Entry<String, CompletableFuture<PopResult>> entry : brokerFutureMap.entrySet()) {
            PopResult result;
            try {
                result = entry.getValue().join();
            } catch (Exception e) {
                log.error("Failed to get peek result from broker: {}", entry.getKey(), e);
                throw new ProxyException(ProxyExceptionCode.INTERNAL_SERVER_ERROR, "Failed to get peek result from broker: " + entry.getKey(), e);
            }
            if (result != null) {
                totalRestNum += result.getRestNum();
                if (result.getMsgFoundList() != null && !result.getMsgFoundList().isEmpty()) {
                    mergedMessages.addAll(result.getMsgFoundList());
                }
            }
        }

        // FORWARD: ascending (oldest first); BACKWARD: descending (newest first)
        Comparator<MessageExt> cmp = Comparator.comparingLong(MessageExt::getStoreTimestamp);
        mergedMessages.sort(direction == PeekDirection.FORWARD ? cmp : cmp.reversed());
        // restNum = broker-side remaining (beyond fetched) + fetched but truncated by maxMsgNums
        long restNum = totalRestNum + Math.max(0, mergedMessages.size() - maxMsgNums);
        if (mergedMessages.size() > maxMsgNums) {
            mergedMessages = new ArrayList<>(mergedMessages.subList(0, maxMsgNums));
        }

        if (mergedMessages.isEmpty()) {
            PeekResult peekResult = new PeekResult(PopStatus.FOUND, mergedMessages);
            peekResult.setRestNum(restNum);
            // No messages → no interval → null cursor
            peekResult.setCursor(null);
            return peekResult;
        }

        // Build half-open interval [begin, end) per broker from actual message offsets.
        // Direction-independent: begin = min offset, end = max offset + 1.
        Map<String, long[]> offsetRanges = new HashMap<>();
        for (MessageExt msg : mergedMessages) {
            String brokerName = msg.getBrokerName();
            long offset = msg.getQueueOffset();
            offsetRanges.merge(brokerName, new long[] {offset, offset + 1},
                (a, b) -> new long[] {Math.min(a[0], b[0]), Math.max(a[1], b[1])});
        }

        PeekResult peekResult = new PeekResult(PopStatus.FOUND, mergedMessages);
        peekResult.setRestNum(restNum);
        peekResult.setCursor(new Cursor(offsetRanges));
        return peekResult;
    }

    private void fillUniqIDIfNeed(MessageExt messageExt) {
        if (StringUtils.isBlank(MessageClientIDSetter.getUniqID(messageExt))) {
            if (messageExt instanceof MessageClientExt) {
                MessageClientExt clientExt = (MessageClientExt) messageExt;
                MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, clientExt.getOffsetMsgId());
            }
        }
    }

    public CompletableFuture<AckResult> ackMessage(
        ProxyContext ctx,
        ReceiptHandle handle,
        String messageId,
        String consumerGroup,
        String topic,
        String liteTopic,
        long timeoutMillis
    ) {
        CompletableFuture<AckResult> future = new CompletableFuture<>();
        try {
            this.validateReceiptHandle(handle);

            AckMessageRequestHeader ackMessageRequestHeader = new AckMessageRequestHeader();
            ackMessageRequestHeader.setConsumerGroup(consumerGroup);
            ackMessageRequestHeader.setTopic(handle.getRealTopic(topic, consumerGroup));
            ackMessageRequestHeader.setQueueId(handle.getQueueId());
            ackMessageRequestHeader.setExtraInfo(handle.getReceiptHandle());
            ackMessageRequestHeader.setOffset(handle.getOffset());
            ackMessageRequestHeader.setLiteTopic(liteTopic);

            future = this.serviceManager.getMessageService().ackMessage(
                ctx,
                handle,
                messageId,
                ackMessageRequestHeader,
                timeoutMillis);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return FutureUtils.addExecutor(future, this.executor);
    }

    public CompletableFuture<List<BatchAckResult>> batchAckMessage(
        ProxyContext ctx,
        List<ReceiptHandleMessage> handleMessageList,
        String consumerGroup,
        String topic,
        long timeoutMillis
    ) {
        CompletableFuture<List<BatchAckResult>> future = new CompletableFuture<>();
        try {
            List<BatchAckResult> batchAckResultList = new ArrayList<>(handleMessageList.size());
            Map<String, List<ReceiptHandleMessage>> brokerHandleListMap = new HashMap<>();

            for (ReceiptHandleMessage handleMessage : handleMessageList) {
                if (handleMessage.getReceiptHandle().isExpired()) {
                    batchAckResultList.add(new BatchAckResult(handleMessage, EXPIRED_HANDLE_PROXY_EXCEPTION));
                    continue;
                }
                List<ReceiptHandleMessage> brokerHandleList = brokerHandleListMap.computeIfAbsent(handleMessage.getReceiptHandle().getBrokerName(), key -> new ArrayList<>());
                brokerHandleList.add(handleMessage);
            }

            if (brokerHandleListMap.isEmpty()) {
                return FutureUtils.addExecutor(CompletableFuture.completedFuture(batchAckResultList), this.executor);
            }
            Set<Map.Entry<String, List<ReceiptHandleMessage>>> brokerHandleListMapEntrySet = brokerHandleListMap.entrySet();
            CompletableFuture<List<BatchAckResult>>[] futures = new CompletableFuture[brokerHandleListMapEntrySet.size()];
            int futureIndex = 0;
            for (Map.Entry<String, List<ReceiptHandleMessage>> entry : brokerHandleListMapEntrySet) {
                futures[futureIndex++] = processBrokerHandle(ctx, consumerGroup, topic, entry.getValue(), timeoutMillis);
            }
            CompletableFuture.allOf(futures).whenComplete((val, throwable) -> {
                if (throwable != null) {
                    future.completeExceptionally(throwable);
                }
                for (CompletableFuture<List<BatchAckResult>> resultFuture : futures) {
                    batchAckResultList.addAll(resultFuture.join());
                }
                future.complete(batchAckResultList);
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return FutureUtils.addExecutor(future, this.executor);
    }

    protected CompletableFuture<List<BatchAckResult>> processBrokerHandle(ProxyContext ctx, String consumerGroup,
        String topic, List<ReceiptHandleMessage> handleMessageList, long timeoutMillis) {
        return this.serviceManager.getMessageService().batchAckMessage(ctx, handleMessageList, consumerGroup, topic, timeoutMillis)
            .thenApply(result -> {
                List<BatchAckResult> results = new ArrayList<>();
                for (ReceiptHandleMessage handleMessage : handleMessageList) {
                    results.add(new BatchAckResult(handleMessage, result));
                }
                return results;
            })
            .exceptionally(throwable -> {
                List<BatchAckResult> results = new ArrayList<>();
                for (ReceiptHandleMessage handleMessage : handleMessageList) {
                    results.add(new BatchAckResult(handleMessage, new ProxyException(ProxyExceptionCode.INTERNAL_SERVER_ERROR, throwable.getMessage(), throwable)));
                }
                return results;
            });
    }

    public CompletableFuture<AckResult> changeInvisibleTime(ProxyContext ctx, ReceiptHandle handle, String messageId,
        String groupName, String topicName, long invisibleTime, String liteTopic, long timeoutMillis, boolean suspend) {
        CompletableFuture<AckResult> future = new CompletableFuture<>();
        try {
            this.validateReceiptHandle(handle);

            ChangeInvisibleTimeRequestHeader changeInvisibleTimeRequestHeader = new ChangeInvisibleTimeRequestHeader();
            changeInvisibleTimeRequestHeader.setConsumerGroup(groupName);
            changeInvisibleTimeRequestHeader.setTopic(handle.getRealTopic(topicName, groupName));
            changeInvisibleTimeRequestHeader.setQueueId(handle.getQueueId());
            changeInvisibleTimeRequestHeader.setExtraInfo(handle.getReceiptHandle());
            changeInvisibleTimeRequestHeader.setOffset(handle.getOffset());
            changeInvisibleTimeRequestHeader.setInvisibleTime(invisibleTime);
            changeInvisibleTimeRequestHeader.setLiteTopic(liteTopic);
            changeInvisibleTimeRequestHeader.setSuspend(suspend);
            long commitLogOffset = handle.getCommitLogOffset();

            future = this.serviceManager.getMessageService().changeInvisibleTime(
                    ctx,
                    handle,
                    messageId,
                    changeInvisibleTimeRequestHeader,
                    timeoutMillis)
                .thenApplyAsync(ackResult -> {
                    if (StringUtils.isNotBlank(ackResult.getExtraInfo())) {
                        AckResult result = new AckResult();
                        result.setStatus(ackResult.getStatus());
                        result.setPopTime(ackResult.getPopTime());
                        result.setExtraInfo(createHandle(ackResult.getExtraInfo(), commitLogOffset));
                        return result;
                    } else {
                        return ackResult;
                    }
                }, this.executor);
        } catch (Throwable t) {
            future.completeExceptionally(t);
            FutureUtils.addExecutor(future, this.executor);
        }
        return future;
    }

    protected String createHandle(String handleString, long commitLogOffset) {
        if (handleString == null) {
            return null;
        }
        return handleString + MessageConst.KEY_SEPARATOR + commitLogOffset;
    }

    public CompletableFuture<PullResult> pullMessage(ProxyContext ctx, MessageQueue messageQueue, String consumerGroup,
        long queueOffset, int maxMsgNums, int sysFlag, long commitOffset,
        long suspendTimeoutMillis, SubscriptionData subscriptionData, long timeoutMillis) {
        CompletableFuture<PullResult> future = new CompletableFuture<>();
        try {
            AddressableMessageQueue addressableMessageQueue = serviceManager.getTopicRouteService()
                .buildAddressableMessageQueue(ctx, messageQueue);
            PullMessageRequestHeader requestHeader = new PullMessageRequestHeader();
            requestHeader.setConsumerGroup(consumerGroup);
            requestHeader.setTopic(addressableMessageQueue.getTopic());
            requestHeader.setQueueId(addressableMessageQueue.getQueueId());
            requestHeader.setQueueOffset(queueOffset);
            requestHeader.setMaxMsgNums(maxMsgNums);
            requestHeader.setSysFlag(sysFlag);
            requestHeader.setCommitOffset(commitOffset);
            requestHeader.setSuspendTimeoutMillis(suspendTimeoutMillis);
            requestHeader.setSubscription(subscriptionData.getSubString());
            requestHeader.setExpressionType(subscriptionData.getExpressionType());
            future = serviceManager.getMessageService().pullMessage(ctx, addressableMessageQueue, requestHeader, timeoutMillis);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return FutureUtils.addExecutor(future, this.executor);
    }

    public CompletableFuture<Void> updateConsumerOffset(ProxyContext ctx, MessageQueue messageQueue,
        String consumerGroup, long commitOffset, long timeoutMillis) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            AddressableMessageQueue addressableMessageQueue = serviceManager.getTopicRouteService()
                .buildAddressableMessageQueue(ctx, messageQueue);
            UpdateConsumerOffsetRequestHeader requestHeader = new UpdateConsumerOffsetRequestHeader();
            requestHeader.setConsumerGroup(consumerGroup);
            requestHeader.setTopic(addressableMessageQueue.getTopic());
            requestHeader.setQueueId(addressableMessageQueue.getQueueId());
            requestHeader.setCommitOffset(commitOffset);
            future = serviceManager.getMessageService().updateConsumerOffset(ctx, addressableMessageQueue, requestHeader, timeoutMillis);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return FutureUtils.addExecutor(future, this.executor);
    }

    public CompletableFuture<Void> updateConsumerOffsetAsync(ProxyContext ctx, MessageQueue messageQueue,
        String consumerGroup, long commitOffset, long timeoutMillis) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            AddressableMessageQueue addressableMessageQueue = serviceManager.getTopicRouteService()
                .buildAddressableMessageQueue(ctx, messageQueue);
            UpdateConsumerOffsetRequestHeader requestHeader = new UpdateConsumerOffsetRequestHeader();
            requestHeader.setConsumerGroup(consumerGroup);
            requestHeader.setTopic(addressableMessageQueue.getTopic());
            requestHeader.setQueueId(addressableMessageQueue.getQueueId());
            requestHeader.setCommitOffset(commitOffset);
            future = serviceManager.getMessageService().updateConsumerOffsetAsync(ctx, addressableMessageQueue, requestHeader, timeoutMillis);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return FutureUtils.addExecutor(future, this.executor);
    }

    public CompletableFuture<Long> queryConsumerOffset(ProxyContext ctx, MessageQueue messageQueue,
        String consumerGroup, long timeoutMillis) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        try {
            AddressableMessageQueue addressableMessageQueue = serviceManager.getTopicRouteService()
                .buildAddressableMessageQueue(ctx, messageQueue);
            QueryConsumerOffsetRequestHeader requestHeader = new QueryConsumerOffsetRequestHeader();
            requestHeader.setConsumerGroup(consumerGroup);
            requestHeader.setTopic(addressableMessageQueue.getTopic());
            requestHeader.setQueueId(addressableMessageQueue.getQueueId());
            future = serviceManager.getMessageService().queryConsumerOffset(ctx, addressableMessageQueue, requestHeader, timeoutMillis);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return FutureUtils.addExecutor(future, this.executor);
    }

    public CompletableFuture<Set<MessageQueue>> lockBatchMQ(ProxyContext ctx, Set<MessageQueue> mqSet,
        String consumerGroup, String clientId, long timeoutMillis) {
        CompletableFuture<Set<MessageQueue>> future = new CompletableFuture<>();
        try {
            Set<MessageQueue> successSet = new CopyOnWriteArraySet<>();
            Set<AddressableMessageQueue> addressableMessageQueueSet = buildAddressableSet(ctx, mqSet);
            Map<String, List<AddressableMessageQueue>> messageQueueSetMap = buildAddressableMapByBrokerName(addressableMessageQueueSet);
            List<CompletableFuture<Void>> futureList = new ArrayList<>();
            messageQueueSetMap.forEach((k, v) -> {
                LockBatchRequestBody requestBody = new LockBatchRequestBody();
                requestBody.setConsumerGroup(consumerGroup);
                requestBody.setClientId(clientId);
                requestBody.setMqSet(v.stream().map(AddressableMessageQueue::getMessageQueue).collect(Collectors.toSet()));
                CompletableFuture<Void> future0 = serviceManager.getMessageService()
                    .lockBatchMQ(ctx, v.get(0), requestBody, timeoutMillis)
                    .thenAccept(successSet::addAll);
                futureList.add(FutureUtils.addExecutor(future0, this.executor));
            });
            CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0])).whenComplete((v, t) -> {
                if (t != null) {
                    log.error("LockBatchMQ failed, group={}", consumerGroup, t);
                }
                future.complete(successSet);
            });
        } catch (Throwable t) {
            log.error("LockBatchMQ exception, group={}", consumerGroup, t);
            future.completeExceptionally(t);
        }
        return FutureUtils.addExecutor(future, this.executor);
    }

    public CompletableFuture<Void> unlockBatchMQ(ProxyContext ctx, Set<MessageQueue> mqSet,
        String consumerGroup, String clientId, long timeoutMillis) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            Set<AddressableMessageQueue> addressableMessageQueueSet = buildAddressableSet(ctx, mqSet);
            Map<String, List<AddressableMessageQueue>> messageQueueSetMap = buildAddressableMapByBrokerName(addressableMessageQueueSet);
            List<CompletableFuture<Void>> futureList = new ArrayList<>();
            messageQueueSetMap.forEach((k, v) -> {
                UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
                requestBody.setConsumerGroup(consumerGroup);
                requestBody.setClientId(clientId);
                requestBody.setMqSet(v.stream().map(AddressableMessageQueue::getMessageQueue).collect(Collectors.toSet()));
                CompletableFuture<Void> future0 = serviceManager.getMessageService().unlockBatchMQ(ctx, v.get(0), requestBody, timeoutMillis);
                futureList.add(FutureUtils.addExecutor(future0, this.executor));
            });
            CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0])).whenComplete((v, t) -> {
                if (t != null) {
                    log.error("UnlockBatchMQ failed, group={}", consumerGroup, t);
                }
                future.complete(null);
            });
        } catch (Throwable t) {
            log.error("UnlockBatchMQ exception, group={}", consumerGroup, t);
            future.completeExceptionally(t);
        }
        return FutureUtils.addExecutor(future, this.executor);
    }

    public CompletableFuture<Long> getMaxOffset(ProxyContext ctx, MessageQueue messageQueue, long timeoutMillis) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        try {
            AddressableMessageQueue addressableMessageQueue = serviceManager.getTopicRouteService()
                .buildAddressableMessageQueue(ctx, messageQueue);
            GetMaxOffsetRequestHeader requestHeader = new GetMaxOffsetRequestHeader();
            requestHeader.setTopic(addressableMessageQueue.getTopic());
            requestHeader.setQueueId(addressableMessageQueue.getQueueId());
            future = serviceManager.getMessageService().getMaxOffset(ctx, addressableMessageQueue, requestHeader, timeoutMillis);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return FutureUtils.addExecutor(future, this.executor);
    }

    public CompletableFuture<Long> getMinOffset(ProxyContext ctx, MessageQueue messageQueue, long timeoutMillis) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        try {
            AddressableMessageQueue addressableMessageQueue = serviceManager.getTopicRouteService()
                .buildAddressableMessageQueue(ctx, messageQueue);
            GetMinOffsetRequestHeader requestHeader = new GetMinOffsetRequestHeader();
            requestHeader.setTopic(addressableMessageQueue.getTopic());
            requestHeader.setQueueId(addressableMessageQueue.getQueueId());
            future = serviceManager.getMessageService().getMinOffset(ctx, addressableMessageQueue, requestHeader, timeoutMillis);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return FutureUtils.addExecutor(future, this.executor);
    }

    protected Set<AddressableMessageQueue> buildAddressableSet(ProxyContext ctx, Set<MessageQueue> mqSet) {
        Set<AddressableMessageQueue> addressableMessageQueueSet = new HashSet<>(mqSet.size());
        for (MessageQueue mq : mqSet) {
            try {
                addressableMessageQueueSet.add(serviceManager.getTopicRouteService().buildAddressableMessageQueue(ctx, mq));
            } catch (Exception e) {
                log.error("build addressable message queue fail, messageQueue = {}", mq, e);
            }
        }
        return addressableMessageQueueSet;
    }

    protected HashMap<String, List<AddressableMessageQueue>> buildAddressableMapByBrokerName(
        final Set<AddressableMessageQueue> mqSet) {
        HashMap<String, List<AddressableMessageQueue>> result = new HashMap<>();
        if (mqSet == null) {
            return result;
        }
        for (AddressableMessageQueue mq : mqSet) {
            if (mq == null) {
                continue;
            }
            List<AddressableMessageQueue> mqs = result.computeIfAbsent(mq.getBrokerName(), k -> new ArrayList<>());
            mqs.add(mq);
        }
        return result;
    }
}
