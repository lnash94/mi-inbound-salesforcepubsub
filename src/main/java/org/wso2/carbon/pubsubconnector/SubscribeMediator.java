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

package org.wso2.carbon.pubsubconnector;

import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import com.salesforce.eventbus.protobuf.FetchRequest;
import com.salesforce.eventbus.protobuf.FetchResponse;


import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.axis2.AxisFault;
import org.apache.synapse.MessageContext;

import org.apache.synapse.commons.json.JsonUtil;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.wso2.carbon.connector.core.AbstractConnector;

import javax.json.Json;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static java.lang.String.format;

public class SubscribeMediator extends AbstractConnector {
    private String topic_name;
    private int replay_preset;
    private int num_requested;
    private byte[] replay_id;
    private String auth_refresh;
    
    public void setTopic_name(String topic_name) {
            this.topic_name = topic_name;
    }

    public String getTopic_name() {
        return topic_name;
    }

    public void setReplay_preset(String replay_preset) {
            this.replay_preset = TypeConverter.toInteger(replay_preset);
    }

    public int getReplay_preset() {
        return replay_preset;
    }

    public void setNum_requested(String num_requested) {
            this.num_requested = TypeConverter.toInteger(num_requested);
    }

    public int getNum_requested() {
        return num_requested;
    }

    public void setReplay_id(String replay_id) {
            this.replay_id = TypeConverter.toByteArray(replay_id);
    }

    public byte[] getReplay_id() {
        return replay_id;
    }

    public void setAuth_refresh(String auth_refresh) {
            this.auth_refresh = auth_refresh;
    }

    public String getAuth_refresh() {
        return auth_refresh;
    }

    @Override
    public void connect(MessageContext context) {
        try {

            FetchRequest request = FetchRequest.newBuilder()
                 .setReplayId(ByteString.copyFrom(replay_id))
                 .setTopicName(topic_name)
                 .setReplayPreset(com.salesforce.eventbus.protobuf.ReplayPreset.valueOf(replay_preset))
                 .setNumRequested(num_requested)
                 .setAuthRefresh(auth_refresh)
                 .build();
            List<FetchRequest> requests = new ArrayList<>();
            requests.add(request);
            List<String> jsonArray =  new ArrayList<>();

            // Keep both blocking for non stream and non-blocking stubs for stream
            // Asynchronous stub
            com.salesforce.eventbus.protobuf.PubSubGrpc.PubSubStub stub = com.salesforce.eventbus.protobuf.
                    PubSubGrpc.newStub((ManagedChannel) context.getProperty("grpc_channel"));

            final CountDownLatch finishLatch = new CountDownLatch(1);

            StreamObserver<FetchRequest> requestObserver =
                    stub.subscribe(new StreamObserver<FetchResponse>() {
                        @Override
                        public void onNext(FetchResponse response) {
                            Map<String, Object> map = new HashMap<>();
                            map.put("rpc_id", response.getRpcId());
                            map.put("latest_replay_id", response.getLatestReplayId());
                            map.put("pending_num_requested", response.getPendingNumRequested());
                            map.put("events", response.getEventsList());
                            String jsonPayload = new Gson().toJson(map);
                            jsonArray.add(jsonPayload);
                        }

                        @Override
                        public void onError(Throwable t) {
                            handleException(t.getMessage(),context);
                            finishLatch.countDown();
                        }

                        @Override
                        public void onCompleted() {
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
                    });

            try {
                for (FetchRequest request1 : requests) {
                    requestObserver.onNext(request1);

                    // Simulate some delay between requests
                    Thread.sleep(200);
                }
            } catch (RuntimeException e) {
                handleException("Error sending request: " + e.getMessage(), context);
                requestObserver.onError(e);
                throw e;
            } catch (InterruptedException ex) {
                handleException(ex.getMessage(), context);
            }

            // Mark the end of requests
            requestObserver.onCompleted();

            // Wait for the server to finish sending responses
            if (!finishLatch.await(1, TimeUnit.MINUTES)) {
                handleException("Could not finish RPC within 1 minute", context);
            }
        } catch (StatusRuntimeException e) {
           handleException(format("Error in SubscribeMediator: code %s , cause: %s ", e.getStatus().getCode().name(), e.getStatus().getDescription()), context);
        } catch (AxisFault e) {
           handleException("Error in SubscribeMediator:", e, context);
        }
    }
}

