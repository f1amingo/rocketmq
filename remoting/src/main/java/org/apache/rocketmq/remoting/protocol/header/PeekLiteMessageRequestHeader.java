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

import com.google.common.base.MoreObjects;
import org.apache.rocketmq.common.lite.OffsetOption;
import org.apache.rocketmq.common.lite.PeekDirection;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.common.resource.RocketMQResource;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.rpc.RpcRequestHeader;

public class PeekLiteMessageRequestHeader extends RpcRequestHeader {

    @CFNotNull
    @RocketMQResource(ResourceType.TOPIC)
    private String parentTopic;
    @CFNotNull
    private String liteTopic;
    @CFNotNull
    @RocketMQResource(ResourceType.GROUP)
    private String consumerGroup;
    @CFNotNull
    private int maxMsgNum;
    /**
     * Flattened OffsetOption for remoting serialization.
     * @see OffsetOption.Type name
     */
    @CFNotNull
    private String offsetOptionType;
    /**
     * Flattened OffsetOption value.
     */
    private long offsetOptionValue;
    /**
     * Peek traversal direction name.
     * @see PeekDirection name
     */
    private String peekDirection;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public String getParentTopic() {
        return parentTopic;
    }

    public void setParentTopic(String parentTopic) {
        this.parentTopic = parentTopic;
    }

    public String getLiteTopic() {
        return liteTopic;
    }

    public void setLiteTopic(String liteTopic) {
        this.liteTopic = liteTopic;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public int getMaxMsgNum() {
        return maxMsgNum;
    }

    public void setMaxMsgNum(int maxMsgNum) {
        this.maxMsgNum = maxMsgNum;
    }

    public String getOffsetOptionType() {
        return offsetOptionType;
    }

    public void setOffsetOptionType(String offsetOptionType) {
        this.offsetOptionType = offsetOptionType;
    }

    public long getOffsetOptionValue() {
        return offsetOptionValue;
    }

    public void setOffsetOptionValue(long offsetOptionValue) {
        this.offsetOptionValue = offsetOptionValue;
    }

    public String getPeekDirection() {
        return peekDirection;
    }

    public void setPeekDirection(String peekDirection) {
        this.peekDirection = peekDirection;
    }

    public PeekDirection toPeekDirection() {
        return peekDirection != null ? PeekDirection.valueOf(peekDirection) : PeekDirection.FORWARD;
    }

    public OffsetOption toOffsetOption() {
        if (offsetOptionType == null) {
            return null;
        }
        try {
            OffsetOption.Type type = OffsetOption.Type.valueOf(offsetOptionType);
            return new OffsetOption(type, offsetOptionValue);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("parentTopic", parentTopic)
            .add("liteTopic", liteTopic)
            .add("consumerGroup", consumerGroup)
            .add("maxMsgNum", maxMsgNum)
            .add("offsetOptionType", offsetOptionType)
            .add("offsetOptionValue", offsetOptionValue)
            .add("peekDirection", peekDirection)
            .toString();
    }
}
