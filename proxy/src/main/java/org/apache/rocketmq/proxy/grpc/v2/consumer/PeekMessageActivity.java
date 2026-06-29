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
import org.apache.rocketmq.client.consumer.PeekResult;
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

            OffsetOption offsetOption = parseOffsetOption(request);
            PeekDirection direction = parseDirection(request);

            future = this.messagingProcessor.peekLiteMessage(
                    ctx,
                    consumerGroup,
                    parentTopic,
                    liteTopic,
                    maxMsgNum,
                    offsetOption,
                    direction,
                    ctx.getRemainingMs())
                .thenApply(this::buildResponse);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    private OffsetOption parseOffsetOption(PeekMessageRequest request) {
        if (!request.hasOffsetOption()) {
            throw new IllegalArgumentException("OffsetOption is required for peek");
        }
        apache.rocketmq.v2.OffsetOption protoOption = request.getOffsetOption();
        switch (protoOption.getOffsetTypeCase()) {
            case POLICY:
                return new OffsetOption(OffsetOption.Type.POLICY, protoOption.getPolicyValue());
            case TIMESTAMP:
                return new OffsetOption(OffsetOption.Type.TIMESTAMP, protoOption.getTimestamp());
            case CURSOR:
                return OffsetOption.ofCursor(protoOption.getCursor());
            default:
                throw new IllegalArgumentException(
                    "Unsupported offset type for peek: " + protoOption.getOffsetTypeCase());
        }
    }

    private PeekDirection parseDirection(PeekMessageRequest request) {
        switch (request.getDirection()) {
            case BACKWARD:
                return PeekDirection.BACKWARD;
            case FORWARD:
            default:
                return PeekDirection.FORWARD;
        }
    }

    private PeekMessageResponse buildResponse(PeekResult peekResult) {
        // Peek is a read-only query; null/empty result returns OK with empty list, not an error.
        if (peekResult == null) {
            return PeekMessageResponse.newBuilder()
                .setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()))
                .build();
        }

        List<Message> messages = new ArrayList<>();
        if (peekResult.getMsgFoundList() != null) {
            for (MessageExt messageExt : peekResult.getMsgFoundList()) {
                messages.add(GrpcConverter.getInstance().buildMessage(messageExt));
            }
        }

        PeekMessageResponse.Builder responseBuilder = PeekMessageResponse.newBuilder()
            .setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()))
            .addAllMessages(messages);

        String cursor = peekResult.getCursor();
        if (cursor != null && !cursor.isEmpty()) {
            responseBuilder.setCursor(cursor);
        }
        responseBuilder.setRestNum(peekResult.getRestNum());

        return responseBuilder.build();
    }

}
