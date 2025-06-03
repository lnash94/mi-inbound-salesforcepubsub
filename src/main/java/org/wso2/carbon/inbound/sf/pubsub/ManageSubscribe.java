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
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.apache.axis2.AxisFault;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseException;
import org.apache.synapse.commons.json.JsonUtil;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.mediators.base.SequenceMediator;
import org.wso2.carbon.inbound.sf.pubsub.com.salesforce.eventbus.protobuf.CommitReplayRequest;
import org.wso2.carbon.inbound.sf.pubsub.com.salesforce.eventbus.protobuf.CommitReplayResponse;
import org.wso2.carbon.inbound.sf.pubsub.com.salesforce.eventbus.protobuf.ConsumerEvent;
import org.wso2.carbon.inbound.sf.pubsub.com.salesforce.eventbus.protobuf.ManagedFetchRequest;
import org.wso2.carbon.inbound.sf.pubsub.com.salesforce.eventbus.protobuf.ManagedFetchResponse;
import org.wso2.carbon.inbound.sf.pubsub.com.salesforce.eventbus.protobuf.PubSubGrpc;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

/**
 * This class manages the subscription to the Salesforce Pub/Sub API. //TODO with test implememtation
 */
public class ManageSubscribe implements StreamObserver<ManagedFetchResponse> {
    private final String subscription_id;
    private final String developer_name;
    private final int num_requested;
    private StreamObserver<ManagedFetchRequest> serverStream;
    private final String injectingSeq;
    private final boolean sequential;
    private final SynapseEnvironment synapseEnvironment;
    private final MessageContext context;
    private final CountDownLatch finishLatch = new CountDownLatch(2);


    public ManageSubscribe(String subscription_id, String developer_name, int num_requested, String injectingSeq, boolean sequential,
                           SynapseEnvironment synapseEnvironment, MessageContext context) {
        this.subscription_id = subscription_id;
        this.developer_name = developer_name;
        this.num_requested = num_requested;
        this.injectingSeq = injectingSeq;
        this.sequential = sequential;
        this.synapseEnvironment = synapseEnvironment;
        this.context = context;
    }

    public void manageSubscription(PubSubGrpc.PubSubStub asyncStub) {
        serverStream = asyncStub.managedSubscribe(this);

        // Create a request to manage the subscription
        ManagedFetchRequest.Builder requestBuilder = ManagedFetchRequest.newBuilder();
        if (subscription_id != null && !subscription_id.isEmpty()) {
            requestBuilder.setSubscriptionId(subscription_id);
        }
        if (developer_name != null && !developer_name.isEmpty()) {
            requestBuilder.setDeveloperName(developer_name);
        }
        ManagedFetchRequest request = requestBuilder.setNumRequested(num_requested)
                .build();
        serverStream.onNext(request);
    }

    public void doCommitReplay(ByteString replayId) {
        String newKey = UUID.randomUUID().toString();
        CommitReplayRequest commitReplayRequest = CommitReplayRequest.newBuilder()
                .setCommitRequestId(newKey)
                .setReplayId(replayId)
                .build();
        ManagedFetchRequest request = ManagedFetchRequest.newBuilder()
                .setCommitReplayIdRequest(commitReplayRequest)
                .build();
        serverStream.onNext(request);
    }

    /**
     * Helps keep the subscription active by sending FetchRequests at regular intervals.
     *
     */
    private void fetchMore(int numOfRequestedEvents) {
        ManagedFetchRequest fetchRequest = ManagedFetchRequest
                .newBuilder()
                .setNumRequested(numOfRequestedEvents)
                .build();
        serverStream.onNext(fetchRequest);
    }

    @Override
    public void onNext(ManagedFetchResponse response) {

        List<String> jsonArray =  new ArrayList<>();

        if (response.hasCommitResponse()) {
            CommitReplayResponse commitResponse = response.getCommitResponse();
        }
        List<ConsumerEvent> eventsList = response.getEventsList();
        for (ConsumerEvent event : eventsList) {
            String json = new Gson().toJson(event);
            jsonArray.add(json);
        }
        if (!response.hasCommitResponse()) {
            doCommitReplay(response.getLatestReplayId());
        }
        if (response.getPendingNumRequested() == 0) {
            fetchMore(num_requested);
        }
        org.apache.axis2.context.MessageContext axisMsgCtx = ((Axis2MessageContext) context).getAxis2MessageContext();
        try {
            JsonUtil.getNewJsonPayload(axisMsgCtx, jsonArray.toString(), true, true);
        } catch (AxisFault e) {
            throw new SynapseException("Error while creating JSON payload: " + e.getMessage(), e);
        }
        axisMsgCtx.setProperty(org.apache.axis2.Constants.Configuration.MESSAGE_TYPE, "application/json");
        axisMsgCtx.setProperty(org.apache.axis2.Constants.Configuration.CONTENT_TYPE, "application/json");
        SequenceMediator seq = (SequenceMediator) synapseEnvironment.getSynapseConfiguration().getSequence(
                injectingSeq);
        if (seq == null) {
            throw new SynapseException(
                    "Sequence with name : " + injectingSeq + " is not found to mediate the message.");
        }
        synapseEnvironment.injectInbound(context, seq, sequential);

    }

    @Override
    public void onError(Throwable t) {
        finishLatch.countDown();
        this.onCompleted();
        throw new SynapseException("Error: " + t.getMessage());
    }

    @Override
    public void onCompleted() {
        finishLatch.countDown();
    }
}
