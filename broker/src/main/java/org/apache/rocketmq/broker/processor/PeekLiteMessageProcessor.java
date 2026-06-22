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
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setOpaque(request.getOpaque());

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

        long startOffset = resolveStartOffset(requestHeader.toOffsetOption(), direction, lmqName, group, maxMsgNum);
        if (startOffset < 0) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("failed to resolve start offset for lmq: " + lmqName);
            return response;
        }

        GetMessageResult getMessageResult = brokerController.getMessageStore()
            .getMessage(group, lmqName, LMQ_QUEUE_ID, startOffset, maxMsgNum, null);

        // Correct offset if store reports inconsistency
        if (getMessageResult != null && (
            GetMessageStatus.OFFSET_TOO_SMALL.equals(getMessageResult.getStatus())
                || GetMessageStatus.OFFSET_OVERFLOW_BADLY.equals(getMessageResult.getStatus()))) {
            startOffset = getMessageResult.getNextBeginOffset();
            getMessageResult = brokerController.getMessageStore()
                .getMessage(group, lmqName, LMQ_QUEUE_ID, startOffset, maxMsgNum, null);
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
            response.setCode(ResponseCode.PULL_NOT_FOUND);
            if (getMessageResult != null) {
                getMessageResult.release();
            }
        }
        response.setRemark(getMessageResult != null ? getMessageResult.getStatus().name()
            : GetMessageStatus.NO_MESSAGE_IN_QUEUE.name());
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

        return null;
    }

    private long resolveStartOffset(OffsetOption offsetOption, PeekDirection direction,
        String lmqName, String group, int maxMsgNum) {
        try {
            long minOffset = brokerController.getMessageStore().getMinOffsetInQueue(lmqName, LMQ_QUEUE_ID);
            long maxOffset = brokerController.getMessageStore().getMaxOffsetInQueue(lmqName, LMQ_QUEUE_ID);

            long anchorOffset;
            if (offsetOption == null) {
                // Default: LAST - consumer offset, fallback to min
                anchorOffset = getConsumerOffset(group, lmqName, minOffset);
            } else {
                OffsetOption.Type type = offsetOption.getType();
                long value = offsetOption.getValue();
                switch (type) {
                    case POLICY:
                        if (value == OffsetOption.POLICY_MIN_VALUE) {
                            anchorOffset = minOffset;
                        } else if (value == OffsetOption.POLICY_MAX_VALUE) {
                            anchorOffset = maxOffset;
                        } else {
                            // POLICY_LAST (default)
                            anchorOffset = getConsumerOffset(group, lmqName, minOffset);
                        }
                        break;
                    case TIMESTAMP:
                        anchorOffset = brokerController.getMessageStore()
                            .getOffsetInQueueByTime(lmqName, LMQ_QUEUE_ID, value);
                        break;
                    default:
                        anchorOffset = getConsumerOffset(group, lmqName, minOffset);
                        break;
                }
            }

            if (direction == PeekDirection.BACKWARD) {
                // Read maxMsgNum messages ending at anchor
                return Math.max(minOffset, anchorOffset - maxMsgNum);
            }
            return anchorOffset;
        } catch (ConsumeQueueException e) {
            LOGGER.error("Failed to resolve start offset for lmq={}", lmqName, e);
            return -1;
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
