package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class Replica extends AbstractReplica {
    /** List of positions in a replica */
    private final int[] positions = new int[AbstractReplica.POSITIONS_LIST_LENGTH];
    private final UpdateClock updateClock = new UpdateClock();
    private int coordinator_id = 0;
    private Map<Integer, ActorRef> replicas;
    private Map<UpdateIndex,UpdateData> history = new HashMap<>();
    private UpdateIndex update_idx;

    /** List of coordinator filed */
    private Map<UpdateIndex,Integer> UpdateACKCounter = new HashMap<>();

    public Replica(int id) {
        this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL, Optional.empty());
    }

    public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
        super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
        this.update_idx = new UpdateIndex();
        // TODO: implement
    }

    public static Props props(int id, int minLatency, int maxLatency, int coordinatorBeatInterval) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, ActorRef listener) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.ofNullable(listener)));
    }

    @Override
    public int getSystemNumberOfActors() {
        // TODO: implement
        return 0;
    }

    @Override
    public void crash(AbstractReplica.Crash how_to_crash) {
        // TODO: implement
    }

    @Override
    public void initSystem(InitSystem sysInit) {
        // TODO: implement
        this.replicas = sysInit.group;
        int coordinator_id = sysInit.coordinator_id;
        this.coordinator_id = coordinator_id;
        log("I set as coordinator: "+coordinator_id);
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(AbstractClient.WriteRequest.class, this::onWriteRequest)
                .match(Replica.UpdateRequest.class,       this::onUpdateRequest)
                .match(Replica.UpdateACK.class,           this::onUpdateACK)
                .match(Replica.WriteOK.class,             this::onWriteOK)
                // TODO add your message handlers here .match(, )
                .build();
    }

    void multicast(Serializable m) {
        for (ActorRef r : replicas.values()) {
            r.tell(m, this.getSelf());
        }
    }

    // =================================================================================
    // Messages classes
    // =================================================================================
    public static class UpdateRequest implements Serializable {
        ActorRef coordinator;
        UpdateIndex identifier;
        int index;
        int value;

        public UpdateRequest(int index, int value, ActorRef coordinator, UpdateIndex identifier) {
            this.identifier = identifier;
            this.value = value;
            this.index = index;
            this.coordinator = coordinator;
        }
    }

    public static class UpdateACK implements Serializable {
        UpdateIndex identifier;
        public UpdateACK(UpdateIndex identifier){
            this.identifier = identifier;
        }
    }

    public static class WriteOK implements Serializable {
        UpdateIndex identifier;
        public WriteOK(UpdateIndex identifier){
            this.identifier = identifier;
        }
    }

    // =================================================================================
    // Messages handler functions
    // =================================================================================

    private void onWriteRequest(AbstractClient.WriteRequest msg) {

//        // schedule a Timeout message in specified time
//        void setTimeout(int time) {
//            getContext().system().scheduler().scheduleOnce(
//                    Duration.create(time, TimeUnit.MILLISECONDS),
//                    getSelf(),
//                    new Timeout(), // the message to send
//                    getContext().system().dispatcher(), getSelf()
//            );
//        }


        log("Recived a Write request by "+ getSender().path().name() + " with content: {index:"+msg.index+", value:"+msg.value+"}");
        if (this.coordinator_id == this.id) {
            log("I'm the coordinator; Sending the update message");
            this.update_idx.incrementI();
            UpdateRequest updateRequest = new UpdateRequest(msg.index, msg.value, this.getSelf(),this.update_idx);
            this.UpdateACKCounter.put(updateRequest.identifier, 0);
            multicast(updateRequest);

        }
        else {
            log("Sending an update request to the coordinator (ID: " + this.coordinator_id + ")" + " with content: {index:" + msg.index + ", value:" + msg.value + "}");
            ActorRef coordinator = this.replicas.get(this.coordinator_id);
            coordinator.tell(msg, this.getSelf());
        }
    }

    private void onUpdateRequest(Replica.UpdateRequest msg) {
        log("Recived an Update request from coordinator with content: {index:"+msg.index+", value:"+msg.value+"}");
        // add update to history
        history.put(msg.identifier,new UpdateData(msg.index,msg.value));
        debug("My history is"+history.keySet()+history.values());
        msg.coordinator.tell(new UpdateACK(msg.identifier), this.getSelf());
    }

    private void onUpdateACK(Replica.UpdateACK msg) {
        debug("recived ack from "+ getSender().path().name());
//        int ACKnumber = UpdateACKCounter.getOrDefault(msg.identifier,0) + 1;
        Integer ACKnumber = UpdateACKCounter.get(msg.identifier);
        if (ACKnumber == null) {
            return;
        }
        ACKnumber += 1;
        int quorum = replicas.size()/2 +1;
        if (ACKnumber >= quorum) {
            WriteOK writeOK = new WriteOK(msg.identifier);
            multicast(writeOK);
            this.UpdateACKCounter.remove(msg.identifier);
        } else {
            UpdateACKCounter.put(
                    msg.identifier,
                    ACKnumber
            );
        }

    }

    private void onWriteOK(Replica.WriteOK msg) {
        log("Recived a WriteOK message from the coordinator, applying the update");
        UpdateIndex identifier = msg.identifier;
        UpdateData updateData = this.history.get(identifier);
        this.positions[updateData.getIndex()] = updateData.getValue();
        debug("New positions is: "+ Arrays.toString(this.positions));
    }

}
