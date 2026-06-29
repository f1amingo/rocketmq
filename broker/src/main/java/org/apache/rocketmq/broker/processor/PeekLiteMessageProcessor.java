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
import java.util.List;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.lite.LiteMetadataUtil;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.lite.LiteUtil;
import org.apache.rocketmq.common.lite.OffsetOption;
import org.apache.rocketmq.common.lite.PeekDirection;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.PeekLiteMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageResponseHeader;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.exception.ConsumeQueueException;

public class PeekLiteMessageProcessor implements NettyRequestProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LITE_LOGGER_NAME);
    private static final int MAX_PEEK_MSG_NUM = 32;
    private static final int LMQ_QUEUE_ID = 0;

    private final BrokerController brokerController;

    public PeekLiteMessageProcessor(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {

        final long beginTimeMills = brokerController.getMessageStore().now();
        RemotingCommand response = RemotingCommand.createResponseCommand(PopMessageResponseHeader.class);
        response.setOpaque(request.getOpaque());
        final PopMessageResponseHeader responseHeader = (PopMessageResponseHeader) response.readCustomHeader();

        final PeekLiteMessageRequestHeader requestHeader =
            request.decodeCommandCustomHeader(PeekLiteMessageRequestHeader.class, true);

        RemotingCommand preCheckResponse = preCheck(ctx, requestHeader, response);
        if (preCheckResponse != null) {
            return preCheckResponse;
        }

        String group = requestHeader.getConsumerGroup();
        String parentTopic = requestHeader.getParentTopic();
        String liteTopic = requestHeader.getLiteTopic();
        int maxMsgNum = requestHeader.getMaxMsgNum();
        String lmqName = LiteUtil.toLmqName(parentTopic, liteTopic);
        PeekDirection direction = requestHeader.toPeekDirection();

        long minOffset = brokerController.getMessageStore().getMinOffsetInQueue(lmqName, LMQ_QUEUE_ID);
        if (minOffset < 0) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("failed to get minOffset for lmq: " + lmqName);
            return response;
        }

        long maxOffset;
        try {
            maxOffset = brokerController.getMessageStore().getMaxOffsetInQueue(lmqName, LMQ_QUEUE_ID);
        } catch (ConsumeQueueException e) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("failed to get maxOffset for lmq: " + lmqName);
            return response;
        }

        long anchorOffset = resolveAnchorOffset(requestHeader.toOffsetOption(), direction, lmqName, group, minOffset, maxOffset);
        if (anchorOffset < 0) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("failed to resolve start offset for lmq: " + lmqName);
            return response;
        }

        long startOffset;
        int readNum;
        if (direction == PeekDirection.BACKWARD) {
            // BACKWARD: read [startOffset, anchorOffset), cap readNum to avoid reading past anchor
            startOffset = Math.max(minOffset, anchorOffset - maxMsgNum);
            readNum = (int) (anchorOffset - startOffset);
        } else {
            startOffset = anchorOffset;
            readNum = maxMsgNum;
        }

        GetMessageResult getMessageResult = readNum > 0
            ? brokerController.getMessageStore().getMessage(group, lmqName, LMQ_QUEUE_ID, startOffset, readNum, null)
            : null;

        // Correct offset if store reports inconsistency
        if (getMessageResult != null && (
            GetMessageStatus.OFFSET_TOO_SMALL.equals(getMessageResult.getStatus())
                || GetMessageStatus.OFFSET_OVERFLOW_BADLY.equals(getMessageResult.getStatus()))) {
            startOffset = getMessageResult.getNextBeginOffset();
            getMessageResult = brokerController.getMessageStore()
                .getMessage(group, lmqName, LMQ_QUEUE_ID, startOffset, readNum, null);
        }

        if (getMessageResult != null) {
            long restNum;
            if (direction == PeekDirection.BACKWARD) {
                // Backward: remaining messages before current page
                restNum = startOffset - minOffset;
            } else {
                // Forward: remaining messages after current page
                restNum = getMessageResult.getMaxOffset() - getMessageResult.getNextBeginOffset();
            }
            responseHeader.setRestNum(restNum);
        }

        if (getMessageResult != null && getMessageResult.getMessageCount() > 0) {
            response.setCode(ResponseCode.SUCCESS);
            getMessageResult.setStatus(GetMessageStatus.FOUND);

            brokerController.getBrokerStatsManager().incGroupGetNums(group, parentTopic,
                getMessageResult.getMessageCount());
            brokerController.getBrokerStatsManager().incGroupGetSize(group, parentTopic,
                getMessageResult.getBufferTotalSize());
            brokerController.getBrokerStatsManager().incBrokerGetNums(parentTopic,
                getMessageResult.getMessageCount());
            brokerController.getBrokerStatsManager().incGroupGetLatency(group, parentTopic, LMQ_QUEUE_ID,
                (int) (brokerController.getMessageStore().now() - beginTimeMills));

            byte[] body = readGetMessageResult(getMessageResult);
            response.setBody(body);
        } else {
            // Peek is a read-only query; empty result is a valid response, not an error.
            response.setCode(ResponseCode.SUCCESS);
            if (getMessageResult != null) {
                getMessageResult.release();
            }
        }
        return response;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    RemotingCommand preCheck(ChannelHandlerContext ctx, PeekLiteMessageRequestHeader requestHeader,
        RemotingCommand response) {

        if (!PermName.isReadable(brokerController.getBrokerConfig().getBrokerPermission())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("the broker[" + brokerController.getBrokerConfig().getBrokerIP1()
                + "] peek lite message is forbidden");
            return response;
        }

        if (requestHeader.getMaxMsgNum() > MAX_PEEK_MSG_NUM) {
            response.setCode(ResponseCode.INVALID_PARAMETER);
            response.setRemark("maxMsgNum must be <= " + MAX_PEEK_MSG_NUM);
            return response;
        }

        TopicConfig topicConfig = brokerController.getTopicConfigManager()
            .selectTopicConfig(requestHeader.getParentTopic());
        if (null == topicConfig) {
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark("parentTopic[" + requestHeader.getParentTopic() + "] not exist, "
                + FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL));
            return response;
        }

        if (!PermName.isReadable(topicConfig.getPerm())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("the topic[" + requestHeader.getParentTopic() + "] peeking message is forbidden");
            return response;
        }

        if (!TopicMessageType.LITE.equals(topicConfig.getTopicMessageType())) {
            response.setCode(ResponseCode.INVALID_PARAMETER);
            response.setRemark("the topic[" + requestHeader.getParentTopic() + "] is not LITE type");
            return response;
        }

        if (!LiteMetadataUtil.isConsumeEnable(requestHeader.getConsumerGroup(), brokerController)) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("subscription group no permission, " + requestHeader.getConsumerGroup());
            return response;
        }

        String bindTopic = LiteMetadataUtil.getLiteBindTopic(requestHeader.getConsumerGroup(), brokerController);
        if (!requestHeader.getParentTopic().equals(bindTopic)) {
            response.setCode(ResponseCode.INVALID_PARAMETER);
            response.setRemark("subscription bind topic not match, group=" + requestHeader.getConsumerGroup());
            return response;
        }

        String lmqName = LiteUtil.toLmqName(requestHeader.getParentTopic(), requestHeader.getLiteTopic());
        if (!brokerController.getLiteLifecycleManager().isLmqExist(lmqName)) {
            response.setCode(ResponseCode.SUCCESS);
            return response;
        }

        return null;
    }

    /**
     * Resolve the anchor offset from the given offset option.
     * For BACKWARD reads, the anchor is the exclusive upper bound — messages in [startOffset, anchor) are read.
     * For FORWARD reads, the anchor is the inclusive start offset.
     */
    private long resolveAnchorOffset(OffsetOption offsetOption, PeekDirection direction, String lmqName, String group,
        long minOffset, long maxOffset) {
        if (offsetOption == null) {
            return getConsumerOffset(group, lmqName, minOffset);
        }
        OffsetOption.Type type = offsetOption.getType();
        long value = offsetOption.getValue();
        switch (type) {
            case POLICY:
                if (value == OffsetOption.POLICY_MIN_VALUE) {
                    return minOffset;
                } else if (value == OffsetOption.POLICY_MAX_VALUE) {
                    return maxOffset;
                } else {
                    return getConsumerOffset(group, lmqName, minOffset);
                }
            case TIMESTAMP:
                return brokerController.getMessageStore()
                    .getOffsetInQueueByTime(lmqName, LMQ_QUEUE_ID, value);
            case OFFSET:
                // FORWARD cursor: value is inclusive start offset, use as-is.
                // BACKWARD cursor: value is the last offset to include; +1 to convert to exclusive upper bound.
                return direction == PeekDirection.BACKWARD ? value + 1 : value;
            default:
                return getConsumerOffset(group, lmqName, minOffset);
        }
    }

    private long getConsumerOffset(String group, String lmqName, long fallback) {
        long offset = brokerController.getConsumerOffsetManager().queryOffset(group, lmqName, LMQ_QUEUE_ID);
        return offset >= 0 ? offset : fallback;
    }

    private byte[] readGetMessageResult(GetMessageResult getMessageResult) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(getMessageResult.getBufferTotalSize());
        try {
            List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
            for (ByteBuffer bb : messageBufferList) {
                byteBuffer.put(bb);
            }
        } finally {
            getMessageResult.release();
        }
        return byteBuffer.array();
    }
}
