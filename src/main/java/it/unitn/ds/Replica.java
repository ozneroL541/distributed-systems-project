package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.*;

public class Replica extends AbstractReplica {
    /** List of positions in a replica */
    private final int[] positions = new int[AbstractReplica.POSITIONS_LIST_LENGTH];
    private int coordinator_id = 0;
    /** List of all the replicas of the system. Integer is the id of the replica, ActorRef is the reference of the Replica inside AKKA */
    private Map<Integer, ActorRef> replicas;
    /** Replica specific filds */
    private final Map<UpdateClock, AbstractClient.WriteRequest> history = new HashMap<>();
    private UpdateClock update_clock;
    private final Queue<AbstractClient.WriteRequest> pendingWrites = new ArrayDeque<>();

    /** List of coordinator filed */
    private final Map<UpdateClock, Integer> UpdateACKCounter = new HashMap<>();

    /** Replica used for "transporting" message ref inside the pendingWrite queue */
    public record PendingWrite(ActorRef clientRef, int index, int value) {}

    public Replica(int id) {
        this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL, Optional.empty());
    }

    public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
        super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
        this.update_clock = new UpdateClock();
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
//        return 0;
        return this.replicas.size();
    }

    @Override
    public void crash(AbstractReplica.Crash how_to_crash) {
        getContext().become(crashed());
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
                .match(AbstractClient.ReadRequest.class,  this::onReadRequest)
                .match(AbstractClient.WriteRequest.class, this::onWriteRequest)
                .match(Replica.UpdateRequest.class,       this::onUpdateRequest)
                .match(Replica.UpdateACK.class,           this::onUpdateACK)
                .match(Replica.WriteOK.class,             this::onWriteOK)
                // TODO add your message handlers here .match(, )
                .build();
    }

    public Receive crashed() {
        return receiveBuilder()
                .matchAny(msg -> {})
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
        UpdateClock identifier;
        ActorRef coordinator;
        AbstractClient.WriteRequest writeRequest;

        public UpdateRequest(UpdateClock identifier, ActorRef coordinator, AbstractClient.WriteRequest writeRequest) {
            this.identifier = identifier;
            this.coordinator = coordinator;
            this.writeRequest = writeRequest;
        }
    }

    public static class UpdateACK implements Serializable {
        UpdateClock identifier;
        public UpdateACK(UpdateClock identifier){
            this.identifier = identifier;
        }
    }

    public static class WriteOK implements Serializable {
        UpdateClock identifier;
        public WriteOK(UpdateClock identifier){
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

        if (getSender() == msg.replica) {
            debug("Inserting the request inside my pending write queue");
            this.pendingWrites.add(msg);
        }
        log("Recived a Write request by "+ getSender().path().name() + " with content: {index:"+msg.index+", value:"+msg.value+"}");
        if (this.coordinator_id == this.id) {
            log("I'm the coordinator; Sending the update message");
            this.update_clock.incrementI();
            UpdateRequest updateRequest = new UpdateRequest(this.update_clock, this.getSelf(), msg);
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
        log("Recived an Update request from coordinator with content: {index:"+msg.writeRequest.index+", value:"+msg.writeRequest.value+"}");
        // add update to history
        history.put(msg.identifier, msg.writeRequest);
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
        UpdateClock identifier = msg.identifier;
        AbstractClient.WriteRequest writeRequest = this.history.get(identifier);
        this.positions[writeRequest.index] = writeRequest.value;
        this.update_clock = identifier;
        this.callbackOnUpdateApplied(writeRequest.index,writeRequest.value);
        debug("New positions is: "+ Arrays.toString(this.positions));
        debug("pending wirte"+ pendingWrites.toString());

        this.pendingWrites.stream()
                .filter(p -> (p.index == writeRequest.index && p.value == writeRequest.value))
                .findFirst()
                .ifPresent(
                        cpw -> cpw.replica.tell(
                                new AbstractClient.WriteResult(true, writeRequest.index, writeRequest.value, this.id),
                                this.getSelf()));
    }

    private void onReadRequest(AbstractClient.ReadRequest msg) {
        debug("Recived a Read request from client");
        int position = this.positions[msg.index];
        msg.replica.tell(new AbstractClient.ReadResult(true,msg.index, position, this.id), this.getSelf());

    }

}
