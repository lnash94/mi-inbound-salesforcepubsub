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

import com.google.gson.Gson;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.wso2.carbon.inbound.sf.pubsub.com.salesforce.eventbus.protobuf.ConsumerEvent;
import org.wso2.carbon.inbound.sf.pubsub.com.salesforce.eventbus.protobuf.FetchRequest;
import org.wso2.carbon.inbound.sf.pubsub.com.salesforce.eventbus.protobuf.FetchResponse;


import org.wso2.carbon.inbound.sf.pubsub.com.salesforce.eventbus.protobuf.PubSubGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.axis2.AxisFault;
import org.apache.synapse.MessageContext;

import org.apache.synapse.commons.json.JsonUtil;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.util.InlineExpressionUtil;
import org.json.JSONArray;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.inbound.endpoint.protocol.generic.GenericPollingConsumer;
import org.wso2.carbon.inbound.sf.pubsub.com.salesforce.eventbus.protobuf.ReplayPreset;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import static java.lang.String.format;
import static org.apache.synapse.util.CallMediatorEnrichUtil.handleException;
import java.util.concurrent.TimeUnit;


public class SubscribeMediator extends GenericPollingConsumer {
    private static final Logger LOGGER = Logger.getLogger(SubscribeMediator.class.getName());
    private String topic_name;
    private final int replay_preset;
    private final int num_requested;
    private final byte[] replay_id;
    private String auth_refresh;
    private final String server;
    private final String port;
    private final String headers;
    private final String username;
    private final String password;
    private String serverCrt;
    private String bearerToken;
    private final String GRPC_CHANNEL = "grpc_channel";
    private String name;
    private boolean tlsEnabled;
    public SubscribeMediator(Properties properties, String name, SynapseEnvironment synapseEnvironment, long scanInterval, String injectingSeq, String onErrorSeq, boolean coordination, boolean sequential) {
        super(properties, name, synapseEnvironment, scanInterval, injectingSeq, onErrorSeq, coordination, sequential);
        this.topic_name = properties.getProperty("topic_name");
        this.replay_preset = properties.getProperty("replay_preset") == null ? 0 :
                TypeConverter.convert(properties.getProperty("replay_preset"), Integer.class);
        this.num_requested = TypeConverter.convert(properties.getProperty("num_requested"), Integer.class);
        this.replay_id = properties.getProperty("replay_id") == null? null :
                TypeConverter.convert(properties.getProperty("replay_id"), byte[].class);
        this.auth_refresh = properties.getProperty("auth_refresh") == null? null :
                properties.getProperty("auth_refresh");
        this.server = properties.getProperty("server");
        this.port = properties.getProperty("port");
        this.headers = properties.getProperty("headers");
        this.username = properties.getProperty("username");
        this.password = properties.getProperty("password");
        this.serverCrt = properties.getProperty("serverCrt");
        this.bearerToken = properties.getProperty("bearerToken");
        this.name = name;
        this.tlsEnabled = Boolean.parseBoolean(properties.getProperty("tlsEnabled"));
    }

    public String getServer() {
        return server;
    }

    public String getPort() {
        return port;
    }

    public String getGRPCHeaders() {
        return headers;
    }

    public String getUsername() {
        return username;
    }


    public String getPassword() {
        return password;
    }

    public  String getServerCrt() {
        return serverCrt;
    }


    public  String getBearerToken() {
        return bearerToken;
    }

    public  void setName(String name) {
        this.name = name;
    }

    public  String getName() {
        return name;
    }

    public  boolean isTLS() {
        return tlsEnabled;
    }

    public void setTopic_name(String topic_name) {
            this.topic_name = topic_name;
    }

    public String getTopic_name() {
        return topic_name;
    }

    public int getReplay_preset() {
        return replay_preset;
    }


    public int getNum_requested() {
        return num_requested;
    }


    public byte[] getReplay_id() {
        return replay_id;
    }

    public String getAuth_refresh() {
        return auth_refresh;
    }


    @Override
    public Object poll() {

        MessageContext msgCtx = createMessageContext();
        int portInt = Integer.parseInt(getPort());
        String target = getServer() + ":" + portInt;
        Metadata metadata;
        ManagedChannel channel;

        try {
            metadata = getGRPCHeaders() != null ? getHeaderMetadata(getGRPCHeaders(), msgCtx): null;
            channel = createChannel(target, isTLS(), metadata);
        } catch (ConnectException e) {
            throw new RuntimeException(e);
        }

        PubSubGrpc.PubSubBlockingStub stub;
        PubSubGrpc.PubSubStub asyncStub;
        if ( getUsername() != null && getPassword() != null) {
            BasicCallCredentials basicAuthCredential = new BasicCallCredentials(getUsername(), getPassword());
            asyncStub = PubSubGrpc.newStub(channel).withCallCredentials(basicAuthCredential);
        } else if (getBearerToken() != null) {
            TokenCallCredentials tokenCredential = new TokenCallCredentials(getBearerToken());
            asyncStub = PubSubGrpc.newStub(channel).withCallCredentials(tokenCredential);
        } else {
            asyncStub = PubSubGrpc.newStub(channel);
        }
        connect(msgCtx, asyncStub);
        return null;
    }

    public void connect(MessageContext context, PubSubGrpc.PubSubStub asyncStub) {
        try {
            FetchRequest request = FetchRequest.newBuilder()
//                 .setReplayId(ByteString.copyFrom(replay_id))
                 .setTopicName(topic_name)
//                 .setReplayPreset(ReplayPreset.forNumber(replay_preset))
                 .setReplayPreset(ReplayPreset.EARLIEST)
                 .setNumRequested(num_requested)
//                 .setAuthRefresh(auth_refresh)
                 .build();
            List<FetchRequest> requests = new ArrayList<>();
            requests.add(request);
            List<String> jsonArray =  new ArrayList<>();

            final CountDownLatch finishLatch = new CountDownLatch(2);
            final AtomicReference<StreamObserver<FetchRequest>> requestObserverRef = new AtomicReference<>();

            StreamObserver<FetchResponse> responseStreamObserver = new StreamObserver<FetchResponse>() {
                @Override
                public void onNext(FetchResponse response) {
                    System.out.println("Received response: " + response);
                    Map<String, Object> map = new HashMap<>();
                    map.put("rpc_id", response.getRpcId());
                    map.put("latest_replay_id", response.getLatestReplayId());
                    map.put("pending_num_requested", response.getPendingNumRequested());
                    map.put("events", response.getEventsList());
                    List<ConsumerEvent> eventsList = response.getEventsList();
                    for (ConsumerEvent event : eventsList) {
                        System.out.println(event.getEvent().getId());
                    }
                    int pendingNumRequested = response.getPendingNumRequested();
                    if (pendingNumRequested != 0){
                        FetchRequest fetchRequest = fetchMore(pendingNumRequested);
                        requestObserverRef.get().onNext(fetchRequest);
                    }
                    String jsonPayload = new Gson().toJson(map);
                    jsonArray.add(jsonPayload);
                }

                @Override
                public void onError(Throwable t) {
                    System.out.println("Error: " + t.getMessage());
                    handleException(t.getMessage(), context);
                    finishLatch.countDown();
                    requestObserverRef.get().onCompleted();
                }

                @Override
                public void onCompleted() {
                    System.out.println("Stream completed.");
                    org.apache.axis2.context.MessageContext axisMsgCtx = ((Axis2MessageContext) context).getAxis2MessageContext();
                    try {
                        JsonUtil.getNewJsonPayload(axisMsgCtx, jsonArray.toString(), true, true);
                    } catch (AxisFault e) {
                        handleException(e.getMessage(), context);
                    }
                    axisMsgCtx.setProperty(org.apache.axis2.Constants.Configuration.MESSAGE_TYPE, "application/json");
                    axisMsgCtx.setProperty(org.apache.axis2.Constants.Configuration.CONTENT_TYPE, "application/json");
                    finishLatch.countDown();
                }
            };
            StreamObserver<FetchRequest> requestObserver = asyncStub.subscribe(responseStreamObserver);
            // Store it in the AtomicReference
            requestObserverRef.set(requestObserver);

            requestObserver.onNext(request);

            // Mark the end of requests
//            requestObserver.onCompleted();

            // Wait for the server to finish sending responses
            if (!finishLatch.await(1, TimeUnit.MINUTES)) {
                handleException("Could not finish RPC within 1 minute", context);
            }
        }
        catch (InterruptedException  e) {
           handleException(format("Error in SubscribeMediator:  %s ", e.getMessage()), context);
        } catch (StatusRuntimeException e) {
            handleException(format("Error in SubscribeMediator: code %s , cause: %s ", e.getStatus().getCode().name(), e.getStatus().getDescription()), context);
        }
    }

    /**
     * Creates a gRPC channel based on the provided parameters.
     *
     * @param target The target server address in the format "host:port"
     * @param useSecure Whether to use transport security (TLS)
     * @param metadata Optional metadata for the channel (can be null)
     * @return A configured ManagedChannel
     */
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

    /**
     * Helps keep the subscription active by sending FetchRequests at regular intervals.
     *
     * @param numEvents
     */
    public FetchRequest fetchMore(int numEvents) {
        return FetchRequest.newBuilder().setTopicName(this.topic_name)
                .setNumRequested(numEvents).build();
    }

}
