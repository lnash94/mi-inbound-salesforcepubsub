/*
 *  Copyright (c) 2025, WSO2 LLC. (https://www.wso2.com).
 *
 *  WSO2 LLC. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.wso2.carbon.inbound.sf.pubsub;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.apache.synapse.MessageContext;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.util.InlineExpressionUtil;
import org.json.JSONArray;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.inbound.endpoint.protocol.generic.GenericPollingConsumer;
import org.wso2.carbon.inbound.sf.pubsub.com.salesforce.eventbus.protobuf.PubSubGrpc;
import org.wso2.carbon.inbound.sf.pubsub.com.salesforce.eventbus.protobuf.ReplayPreset;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Logger;

import java.util.concurrent.TimeUnit;

/**
 * This class is responsible for managing the inbound connection to Salesforce Pub/Sub API.
 * It handles subscribing to topics and managing subscriptions.
 */
public class SFPubSubInboundFactory  extends GenericPollingConsumer {
    private static final Logger LOGGER = Logger.getLogger(SFPubSubInboundFactory.class.getName());
    private final String topic_name;
    private int replay_preset;
    private final int num_requested;
    private ByteString replay_id;
    private final String server;
    private final String port;
    private final String headers;
    private final String username;
    private final String password;
    private final String securityToken;
    private final boolean tlsEnabled;
    private PubSubGrpc.PubSubStub asyncStub = null;
    private ManagedChannel channel = null;
    private final String subscription_id;
    private final String developer_name;
    private final String rpcMethod;
    private static boolean nextPolling = false;
    private final ByteString lastReplayId = null;
    private PubSubGrpc.PubSubBlockingStub blockingStub = null;
    public SFPubSubInboundFactory(Properties properties, String name, SynapseEnvironment synapseEnvironment, long scanInterval, String injectingSeq, String onErrorSeq, boolean coordination, boolean sequential) {
        super(properties, name, synapseEnvironment, scanInterval, injectingSeq, onErrorSeq, coordination, sequential);
        this.topic_name = properties.getProperty("topic_name");
        this.replay_preset = getReplayPresetType(properties.getProperty("replay_preset"));
        this.num_requested = TypeConverter.convert(properties.getProperty("num_requested"), Integer.class);
        this.replay_id = properties.getProperty("replay_id") == null? null :
                ByteString.copyFrom(TypeConverter.convert(properties.getProperty("replay_id"), byte[].class));
        this.server = properties.getProperty("server");
        this.port = properties.getProperty("port");
        this.headers = properties.getProperty("headers");
        this.username = properties.getProperty("username");
        this.password = properties.getProperty("password");
        this.securityToken = properties.getProperty("securityToken");
        this.tlsEnabled = Boolean.parseBoolean(properties.getProperty("tlsEnabled"));
        this.rpcMethod = properties.getProperty("rpcMethod");
        this.subscription_id = properties.getProperty("subscription_id");
        this.developer_name = properties.getProperty("developer_name");
    }

    @Override
    public Object poll() {

        if(nextPolling || (channel != null && (!channel.isTerminated() || !channel.isShutdown()))) {
            return null;
        }

        MessageContext msgCtx = createMessageContext();
        int portInt = Integer.parseInt(port);
        String target = server + ":" + portInt;
        Metadata metadata;

        // Create a gRPC channel
        if (asyncStub == null) {
            try {
                metadata = headers != null ? getHeaderMetadata(headers, msgCtx): null;
            } catch (ConnectException e) {
                throw new RuntimeException(e);
            }
            if ( username != null && password != null) {
                BasicAuthLogin basicAuthLogin = new BasicAuthLogin();
                BasicAuthLogin.LoginResponse loginResponse;
                try {
                    metadata = metadata == null? new Metadata(): metadata;
                    loginResponse = basicAuthLogin.login(username, password, securityToken,
                            "https://login.salesforce.com/services/Soap/u/61.0");
                    metadata.put(Metadata.Key.of("accessToken", Metadata.ASCII_STRING_MARSHALLER), loginResponse.sessionId);
                    metadata.put(Metadata.Key.of("instanceUrl", Metadata.ASCII_STRING_MARSHALLER), loginResponse.instanceUrl);
                    metadata.put(Metadata.Key.of("tenantId", Metadata.ASCII_STRING_MARSHALLER), loginResponse.tenantId);
                }  catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            try {
                channel = createChannel(target, tlsEnabled, metadata);
                asyncStub = PubSubGrpc.newStub(channel);
                blockingStub = PubSubGrpc.newBlockingStub(channel);
            } catch (ConnectException e) {
                throw new RuntimeException(e);
            }
        }

        if (rpcMethod.equals("Subscribe")) {
            LOGGER.info("gRPC subscribe method is called with topic: " + topic_name);
            Subscribe subscribe = new Subscribe(topic_name, replay_preset, num_requested, replay_id,
                    synapseEnvironment, injectingSeq, sequential, blockingStub);
            subscribe.subscribe(msgCtx, asyncStub);
            nextPolling = subscribe.getPendingRequests() == 0;
            nextPolling = subscribe.isActive();
        } else if (rpcMethod.equals("Manage Subscribes")) {
            ManageSubscribe manageSubscribe = new ManageSubscribe(subscription_id, developer_name, num_requested, injectingSeq, sequential,
                    synapseEnvironment, msgCtx);
            manageSubscribe.manageSubscription(asyncStub);
        }
        return null;
    }

    public int getReplayPresetType(String replayPreset) {
        if (replayPreset == null || replayPreset.isEmpty()) {
            return ReplayPreset.EARLIEST_VALUE;
        }
        switch (replayPreset.toUpperCase()) {
            case "LATEST":
                return ReplayPreset.LATEST_VALUE;
            case "CUSTOM":
                return ReplayPreset.CUSTOM_VALUE;
            default:
                return ReplayPreset.EARLIEST_VALUE;
        }
    }

    private ManagedChannel createChannel(String target, boolean useSecure, Metadata metadata) throws ConnectException {
        try {
            if (useSecure && metadata != null) {
                LOGGER.info("gRPC secure channel is created with metadata:"+ target);
                return  NettyChannelBuilder.forTarget(target)
                        .useTransportSecurity()
                        .intercept(new MetadataInterceptor(metadata))
                        .keepAliveTime(30, TimeUnit.SECONDS)
                        .keepAliveTimeout(10, TimeUnit.SECONDS)
                        .keepAliveWithoutCalls(true)
                        .build();
            } else if (useSecure){
                LOGGER.info("gRPC secure channel is created:"+ target);
                return NettyChannelBuilder.forTarget(target)
                        .useTransportSecurity()
                        .keepAliveTime(30, TimeUnit.SECONDS)
                        .keepAliveTimeout(10, TimeUnit.SECONDS)
                        .keepAliveWithoutCalls(true)
                        .build();
            } else if (metadata != null) {
                LOGGER.info("gRPC channel is created with metadata:"+ target);
                return NettyChannelBuilder.forTarget(target)
                        .usePlaintext()
                        .intercept(new MetadataInterceptor(metadata))
                        .keepAliveTime(30, TimeUnit.SECONDS)
                        .keepAliveTimeout(10, TimeUnit.SECONDS)
                        .keepAliveWithoutCalls(true)
                        .build();
            } else {
                LOGGER.info("gRPC channel is created:"+ target);
                return NettyChannelBuilder.forTarget(target)
                        .usePlaintext()
                        .keepAliveTime(30, TimeUnit.SECONDS)
                        .keepAliveTimeout(10, TimeUnit.SECONDS)
                        .keepAliveWithoutCalls(true)
                        .build();
            }
        } catch (Exception e) {
            throw new ConnectException("Failed to create channel: " + e.getMessage());
        }
    }
    private  Metadata getHeaderMetadata(String headers, MessageContext messageContext) throws ConnectException {
        // Extract metadata from JSON
        Metadata metadata = new Metadata();
        JSONArray headerSet;
        try {
            headers = InlineExpressionUtil.processInLineSynapseExpressionTemplate(messageContext, headers);
            headerSet = new JSONArray(headers);
        } catch (Exception e) {
            throw new ConnectException("Error parsing JSON headers: " + e.getMessage());
        }
        for (int i = 0; i < headerSet.length(); i++) {
            JSONArray row = headerSet.getJSONArray(i);

            if (row.length() >= 2) {
                String key = row.getString(0);
                String value = row.getString(1);
                metadata.put(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER), value);
            }
        }
        return metadata;
    }

    /**
     * Create the message context.
     */
    private MessageContext createMessageContext() {

        MessageContext msgCtx = this.synapseEnvironment.createMessageContext();
        org.apache.axis2.context.MessageContext axis2MsgCtx = ((Axis2MessageContext) msgCtx).getAxis2MessageContext();
        axis2MsgCtx.setServerSide(true);
        axis2MsgCtx.setMessageID(String.valueOf(UUID.randomUUID()));
        return msgCtx;
    }

    private void createFile (String filePath) throws IOException {
        File file = new File(filePath);
        file.getParentFile().mkdirs();
        if(!file.exists()) {
            file.createNewFile();
        }
    }
}
