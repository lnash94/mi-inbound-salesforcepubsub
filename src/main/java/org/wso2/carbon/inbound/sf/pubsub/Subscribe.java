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
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.axis2.AxisFault;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseException;
import org.apache.synapse.commons.json.JsonUtil;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.mediators.base.SequenceMediator;
import org.json.JSONObject;
import org.wso2.carbon.inbound.sf.pubsub.com.salesforce.eventbus.protobuf.ConsumerEvent;
import org.wso2.carbon.inbound.sf.pubsub.com.salesforce.eventbus.protobuf.FetchRequest;
import org.wso2.carbon.inbound.sf.pubsub.com.salesforce.eventbus.protobuf.FetchResponse;
import org.wso2.carbon.inbound.sf.pubsub.com.salesforce.eventbus.protobuf.PubSubGrpc;
import org.wso2.carbon.inbound.sf.pubsub.com.salesforce.eventbus.protobuf.ReplayPreset;
import org.wso2.carbon.inbound.sf.pubsub.com.salesforce.eventbus.protobuf.SchemaRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import com.google.protobuf.util.JsonFormat;

/**
 * This class manages the subscription to the Salesforce Pub/Sub API.
 */
public class Subscribe {
    private static final Logger LOGGER = Logger.getLogger(Subscribe.class.getName());
    private static final long FETCH_INTERVAL_SECONDS = 10;
    private static final long RESOURCE_EXHAUSTED_BACKOFF_SECONDS = 300;
    private static final int MAX_CONSECUTIVE_EMPTY_RESPONSES = 5;
    private final String topic_name;
    private final int replay_preset;
    private final int num_requested;
    private ByteString replay_id;
    private final SynapseEnvironment synapseEnvironment;
    private final String injectingSeq;
    private final boolean sequential;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final AtomicBoolean isActive = new AtomicBoolean(true);
    private int consecutiveEmptyResponses = 0;
    private int pendingRequests = 0;
    private final Map<String, Schema> schemaCache = new ConcurrentHashMap<>();
    private final PubSubGrpc.PubSubBlockingStub blockingStub ;
    private boolean firstRequest = true;


    public Subscribe(String topic_name, int replay_preset, int num_requested, ByteString replay_id,
                     SynapseEnvironment synapseEnvironment, String injectingSeq, boolean sequential, PubSubGrpc.PubSubBlockingStub blockingStub) {
        this.topic_name = topic_name;
        this.replay_preset = replay_preset;
        this.num_requested = num_requested;
        this.replay_id = replay_id;
        this.synapseEnvironment = synapseEnvironment;
        this.injectingSeq = injectingSeq;
        this.sequential = sequential;
        this.blockingStub = blockingStub;
    }

    public  int getPendingRequests() {
        return pendingRequests;
    }

    public void subscribe(MessageContext context, PubSubGrpc.PubSubStub asyncStub) {
        String injectingSeq = this.injectingSeq;
        boolean sequential = this.sequential;

        try {
            FetchRequest request;
            if (firstRequest){
                request = buildFetchRequest();
            } else {
                request = fetchMore(num_requested, replay_id);
            }

            // Create a CountDownLatch to wait for the completion of the stream
            final CountDownLatch finishLatch = new CountDownLatch(1);
            final AtomicReference<StreamObserver<FetchRequest>> requestObserverRef = new AtomicReference<>();

            StreamObserver<FetchResponse> responseStreamObserver = new StreamObserver<FetchResponse>() {
                @Override
                public void onNext(FetchResponse response) {
                    try {
                        firstRequest = false;
                        List<ConsumerEvent> eventsList = response.getEventsList();
                        if (!eventsList.isEmpty()) {
                            consecutiveEmptyResponses = 0;
                            LOGGER.info("Received " + eventsList.size() + " events from topic: " + topic_name);
                            // Update replay ID for next request
                            replay_id = response.getLatestReplayId();
                            pendingRequests = response.getPendingNumRequested();
                            // Process the events
                            processEvents(context, eventsList, injectingSeq, sequential);
                        } else {
                            consecutiveEmptyResponses++;
                        }
                        // Schedule next fetch request with delay to avoid overwhelming the server
                        if (pendingRequests == 0) {
                            scheduleNextFetch(requestObserverRef.get());
                        }
                    } catch (Exception e) {
                        LOGGER.severe("Error processing response: " + e.getMessage());
                        onError(e);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    LOGGER.severe("Stream error: " + t.getMessage());
                    // Handle RESOURCE_EXHAUSTED specifically
                    if (t instanceof StatusRuntimeException) {
                        StatusRuntimeException sre = (StatusRuntimeException) t;
                        Status.Code code = sre.getStatus().getCode();
                        if (code == io.grpc.Status.Code.RESOURCE_EXHAUSTED || code == Status.Code.UNAVAILABLE) {
                            if (!scheduleRecoveryAfterResourceExhaustion(context, asyncStub)){
                                isActive.set(false);
                            }
                            return;
                        }
                    }

                    isActive.set(false);
                    finishLatch.countDown();
                    if (requestObserverRef.get() != null) {
                        try {
                            requestObserverRef.get().onCompleted();
                        } catch (Exception e) {
                            LOGGER.warning("Error completing request observer: " + e.getMessage());
                        }
                    }
                    cleanup();
                    throw new SynapseException("Error: " + t.getMessage(), t);
                }

                @Override
                public void onCompleted() {
                    LOGGER.info("Stream completed");
                    isActive.set(false);
                    finishLatch.countDown();
                    cleanup();
                }
            };
            StreamObserver<FetchRequest> requestObserver = asyncStub.subscribe(responseStreamObserver);
            requestObserverRef.set(requestObserver);

            // Send initial request
            LOGGER.info("Sending initial fetch request for topic: " + topic_name);
            requestObserver.onNext(request);

        } catch (StatusRuntimeException e) {
            cleanup();
            throw new SynapseException("Error in Subscribe: code " + e.getStatus().getCode().name() +
                    ", cause: " + e.getStatus().getDescription(), e);
        }
    }

    private FetchRequest buildFetchRequest() {
        FetchRequest.Builder requestBuilder = FetchRequest.newBuilder()
                .setTopicName(topic_name)
                .setNumRequested(num_requested);

        if (replay_preset > 0) {
            requestBuilder.setReplayPreset(ReplayPreset.forNumber(replay_preset));
        } else {
            requestBuilder.setReplayPreset(ReplayPreset.LATEST);
        }
        if (replay_preset == ReplayPreset.CUSTOM_VALUE && replay_id != null) {
            requestBuilder.setReplayId(replay_id);
        }
        return requestBuilder.build();
    }

    private void processEvents(MessageContext context, List<ConsumerEvent> jsonArray,
                               String injectingSeq, boolean sequential) throws IOException {
        for (ConsumerEvent consumerEvent: jsonArray) {
            String jsonString = JsonFormat.printer().includingDefaultValueFields().print(consumerEvent);
            JSONObject jsonObject = new JSONObject(jsonString);
            JSONObject event = (JSONObject) jsonObject.get("event");

            String schemaId = consumerEvent.getEvent().getSchemaId();
            Schema writerSchema = getSchema(schemaId);
            GenericRecord deserialize = SFPubSubInboundUtils.deserialize(writerSchema, consumerEvent.getEvent().getPayload());
            event.put("decodedPayload_", deserialize.toString());
            jsonObject.put("event", event);

            org.apache.axis2.context.MessageContext axisMsgCtx =
                    ((Axis2MessageContext) context).getAxis2MessageContext();
            try {
                JsonUtil.getNewJsonPayload(axisMsgCtx, jsonObject.toString(), true,
                        true);
            } catch (AxisFault e) {
                throw new SynapseException("Error while creating JSON payload: " + e.getMessage(), e);
            }

            axisMsgCtx.setProperty(org.apache.axis2.Constants.Configuration.MESSAGE_TYPE, "application/json");
            axisMsgCtx.setProperty(org.apache.axis2.Constants.Configuration.CONTENT_TYPE, "application/json");

            SequenceMediator seq = (SequenceMediator) synapseEnvironment.getSynapseConfiguration()
                    .getSequence(injectingSeq);
            if (seq == null) {
                throw new SynapseException(
                        "Sequence with name : " + injectingSeq + " is not found to mediate the message.");
            }
            synapseEnvironment.injectInbound(context, seq, sequential);
        }
    }

    private void scheduleNextFetch(StreamObserver<FetchRequest> requestObserver) {
        if (!isActive.get()) {
            return;
        }
        long delay = FETCH_INTERVAL_SECONDS;
        if (consecutiveEmptyResponses > MAX_CONSECUTIVE_EMPTY_RESPONSES) {
            delay = Math.min(FETCH_INTERVAL_SECONDS * (1L << Math.min(consecutiveEmptyResponses - MAX_CONSECUTIVE_EMPTY_RESPONSES, 3)), 120);
        }
        scheduler.schedule(() -> {
            if (isActive.get() && requestObserver != null) {
                try {
                    FetchRequest nextRequest = fetchMore(num_requested, replay_id);
                    requestObserver.onNext(nextRequest);
                } catch (Exception e) {
                    LOGGER.severe("Error sending next fetch request: " + e.getMessage());
                    isActive.set(false);
                }
            }
        }, delay, TimeUnit.SECONDS);
    }

    private boolean scheduleRecoveryAfterResourceExhaustion(MessageContext context, PubSubGrpc.PubSubStub asyncStub) {
        if (!isActive.get()) {
            LOGGER.warning("Subscription is no longer active, skipping recovery attempt.");
            return false;
        }
        scheduler.schedule(() -> {
            if (isActive.get()) {
                try {
                    LOGGER.info("Attempting to restart subscription after resource exhaustion");
                    // Restart the subscription with a new stream
                    subscribe(context, asyncStub);
                } catch (Exception e) {
                    isActive.set(false);
                }
            } else {
                LOGGER.warning("Subscription is no longer active, skipping recovery attempt.");
            }
        }, RESOURCE_EXHAUSTED_BACKOFF_SECONDS, TimeUnit.SECONDS);
        return true;
    }

    /**
     * Helps keep the subscription active by sending FetchRequests at regular intervals.
     *
     * @param numEvents the number of events to fetch in the next request
     * @param replayId  the replay ID to start from
     */
    public FetchRequest fetchMore(int numEvents, ByteString replayId) {
        FetchRequest.Builder builder = FetchRequest.newBuilder()
                .setTopicName(this.topic_name)
                .setNumRequested(numEvents);

        if (replayId != null) {
            builder.setReplayId(replayId);
            builder.setReplayPreset(ReplayPreset.forNumber(ReplayPreset.CUSTOM_VALUE));
        }

        return builder.build();
    }

    private void cleanup() {
        isActive.set(false);
        if (!scheduler.isShutdown()) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    public ByteString getReplayId() {
        return replay_id;
    }

    public Boolean isActive() {
        return isActive.get();
    }


    /**
     * Helper function to get the schema of an event if it does not already exist in the schema cache.
     */
    public Schema getSchema(String schemaId) {
        return schemaCache.computeIfAbsent(schemaId, id -> {
            SchemaRequest request = SchemaRequest.newBuilder().setSchemaId(id).build();
            String schemaJson = blockingStub.getSchema(request).getSchemaJson();
            return (new Schema.Parser()).parse(schemaJson);
        });
    }
}

