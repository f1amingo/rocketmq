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
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.PeekMessageRequest;
import apache.rocketmq.v2.PeekMessageResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.common.lite.OffsetOption;
import org.apache.rocketmq.common.lite.PeekDirection;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.AbstractMessagingActivity;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;

public class PeekMessageActivity extends AbstractMessagingActivity {

    public PeekMessageActivity(MessagingProcessor messagingProcessor,
        GrpcClientSettingsManager grpcClientSettingsManager, GrpcChannelManager grpcChannelManager) {
        super(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
    }

    public CompletableFuture<PeekMessageResponse> peekMessage(ProxyContext ctx, PeekMessageRequest request) {
        CompletableFuture<PeekMessageResponse> future = new CompletableFuture<>();

        try {
            validateTopicAndConsumerGroup(request.getTopic(), request.getGroup());
            validateLiteTopic(request.getLiteTopic());

            String parentTopic = request.getTopic().getName();
            String liteTopic = request.getLiteTopic();
            String consumerGroup = request.getGroup().getName();
            int maxMsgNum = request.getMaxMsgNum();

            OffsetOption offsetOption = convertOffsetOption(request);
            PeekDirection direction = convertDirection(request);

            future = this.messagingProcessor.peekLiteMessage(
                    ctx,
                    consumerGroup,
                    parentTopic,
                    liteTopic,
                    maxMsgNum,
                    offsetOption,
                    direction,
                    ctx.getRemainingMs())
                .thenApply(popResult -> buildResponse(popResult));
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    private OffsetOption convertOffsetOption(PeekMessageRequest request) {
        if (!request.hasOffsetOption()) {
            return null;
        }
        apache.rocketmq.v2.OffsetOption protoOption = request.getOffsetOption();
        switch (protoOption.getOffsetTypeCase()) {
            case POLICY:
                return new OffsetOption(OffsetOption.Type.POLICY, protoOption.getPolicyValue());
            case TIMESTAMP:
                return new OffsetOption(OffsetOption.Type.TIMESTAMP, protoOption.getTimestamp());
            case OFFSET:
                throw new IllegalArgumentException("OFFSET is not supported for peek, use TIMESTAMP instead");
            case TAIL_N:
                throw new IllegalArgumentException("TAIL_N is not supported for peek");
            default:
                return null;
        }
    }

    private PeekDirection convertDirection(PeekMessageRequest request) {
        int directionValue = request.getDirectionValue();
        if (directionValue == apache.rocketmq.v2.PeekDirection.BACKWARD_VALUE) {
            return PeekDirection.BACKWARD;
        }
        return PeekDirection.FORWARD;
    }

    private PeekMessageResponse buildResponse(PopResult popResult) {
        if (popResult == null || !PopStatus.FOUND.equals(popResult.getPopStatus())
            || popResult.getMsgFoundList() == null || popResult.getMsgFoundList().isEmpty()) {
            return PeekMessageResponse.newBuilder()
                .setStatus(ResponseBuilder.getInstance().buildStatus(Code.MESSAGE_NOT_FOUND, "no message found"))
                .build();
        }

        List<Message> messages = new ArrayList<>(popResult.getMsgFoundList().size());
        for (MessageExt messageExt : popResult.getMsgFoundList()) {
            messages.add(GrpcConverter.getInstance().buildMessage(messageExt));
        }

        return PeekMessageResponse.newBuilder()
            .setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()))
            .addAllMessages(messages)
            .build();
    }

}
