package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Replica extends AbstractReplica {
    /** List of positions in a replica */
    private final int[] positions = new int[AbstractReplica.POSITIONS_LIST_LENGTH];
    /** The update clock for the replica */
    private final UpdateClock updateClock;
    /** The id of the coordinator */
    private int coordinatorID = 0;
    /** The number of replicas in the system */
    private int numberOfReplicas;
    /** 
     * List of all alive replicas of the system. 
     * Integer is the id of the replica, 
     * ActorRef is the reference of the Replica inside AKKA 
     */
    private Map<Integer, ActorRef> replicas;
    /** Replica specific fields */
    private final Map<UpdateClock, AbstractClient.WriteRequest> history = new HashMap<>();
    /** Queue for pending write requests */
    private final Queue<AbstractClient.WriteRequest> pendingWrites = new ArrayDeque<>();
    /** List of coordinator filed */
    private final Map<UpdateClock, Integer> UpdateACKCounter = new HashMap<>();
    /**
     * Constructor for Replica
     * @param id the id of the replica
     */
    public Replica(int id) {
        this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL, Optional.empty());
    }
    /**
     * Constructor for Replica
     * @param id the id of the replica
     * @param minLatency the minimum latency
     * @param maxLatency the maximum latency
     * @param coordinatorBeatInterval the interval for coordinator beats
     * @param listener the listener for the replica
     */
    public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
        super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
        this.updateClock = new UpdateClock();
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
        this.numberOfReplicas = sysInit.group.size();
        int coordinator_id = sysInit.coordinator_id;
        this.coordinatorID = coordinator_id;
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
                .match(TimeOut.class,                     this::onTimeOut)
                // TODO add your message handlers here .match(, )
                .build();
    }

    public Receive crashed() {
        return receiveBuilder()
                .matchAny(msg -> {})
                .build();
    }
    // =================================================================================
    // Helper functions
    // =================================================================================
    /**
     * Helper function to schedule a timeout
     * @param time the time to wait before sending the timeout message (in milliseconds)
     * @param timeOut the timeOut message to schedule
     * @return A cancellable reference to the timeout that can be used to cancel the timeout if needed
     */
    Cancellable setTimeout(int time, TimeOut timeOut) {
        return getContext().system().scheduler().scheduleOnce(
                Duration.create(time, TimeUnit.MILLISECONDS),
                getSelf(),
                timeOut,
                getContext().system().dispatcher(), getSelf()
        );
    }
    /**
     * Multicast a message to all the replicas
     * @param m the message to multicast
     */
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
        log("Received a Write request by "+ getSender().path().name() + " with content: {index:"+msg.index+", value:"+msg.value+"}");
        if (this.coordinatorID == this.id) {
            log("I'm the coordinator; Sending the update message");
            this.updateClock.incrementI();
            UpdateRequest updateRequest = new UpdateRequest(this.updateClock, this.getSelf(), msg);
            this.UpdateACKCounter.put(updateRequest.identifier, 0);
            multicast(updateRequest);

        }
        else {
            log("Sending an update request to the coordinator (ID: " + this.coordinatorID + ")" + " with content: {index:" + msg.index + ", value:" + msg.value + "}");
            ActorRef coordinator = this.replicas.get(this.coordinatorID);
            coordinator.tell(msg, this.getSelf());
        }
    }

    private void onUpdateRequest(Replica.UpdateRequest msg) {
        log("Received an Update request from coordinator with content: {index:"+msg.writeRequest.index+", value:"+msg.writeRequest.value+"}");
        // add update to history
        history.put(msg.identifier, msg.writeRequest);
        debug("My history is"+history.keySet()+history.values());
        msg.coordinator.tell(new UpdateACK(msg.identifier), this.getSelf());
    }

    private void onUpdateACK(Replica.UpdateACK msg) {
        debug("received ack from "+ getSender().path().name());
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
        log("Received a WriteOK message from the coordinator, applying the update");
        UpdateClock identifier = msg.identifier;
        AbstractClient.WriteRequest writeRequest = this.history.get(identifier);
        this.positions[writeRequest.index] = writeRequest.value;
        if (this.updateClock.compareTo(identifier) > 0 ) {
            return;
        }
        this.updateClock.syncClock(identifier);
        this.callbackOnUpdateApplied(writeRequest.index,writeRequest.value);
        debug("New positions is: "+ Arrays.toString(this.positions));
        debug("pending write"+ pendingWrites.toString());

        this.pendingWrites.stream()
                .filter(p -> (p.index == writeRequest.index && p.value == writeRequest.value))
                .findFirst()
                .ifPresent(
                        cpw -> cpw.replica.tell(
                                new AbstractClient.WriteResult(true, writeRequest.index, writeRequest.value, this.id),
                                this.getSelf()));
    }

    private void onReadRequest(AbstractClient.ReadRequest msg) {
        debug("Received a Read request from client");
        int position = this.positions[msg.index];
        msg.replica.tell(new AbstractClient.ReadResult(true,msg.index, position, this.id), this.getSelf());

    }

    /**
     * Handle a timeout message
     * @param msg the timeout message
     */
    private void onTimeOut(TimeOut msg) {
        switch (msg.type) {
            case TimeOut.TimeoutType.SendRead:
                log("Timeout on read request");
                break;
            case TimeOut.TimeoutType.SendWrite:
                log("Timeout on write request");
                break;
            case TimeOut.TimeoutType.UpdateRequest:
                log("Timeout on update request");
                break;
            case TimeOut.TimeoutType.WriteRequest:
                log("Timeout on write request");
                break;
            case TimeOut.TimeoutType.Heartbeat:
                log("Timeout on heartbeat");
                break;
            case TimeOut.TimeoutType.Election:
                log("Timeout on election");
                break;
            default:
                break;
        }
    }
    /**
     * Handle a coordinator crash by removing it from the 
     * list of replicas and starting an election.
     */
    private void onCoordinatorCrash() {
        this.nodeCrashed(this.coordinatorID);
        this.startElection();
    }

    /**
     * Handle a node crash by removing it from the list of replicas.
     * @param id the id of the crashed node
     */
    private void nodeCrashed(int id){
        this.replicas.remove(id);
    }
    /**
     * Get the next replica ID in a ring topology.
     * @param id the current replica ID
     * @return the next replica ID
     */
    private int nextReplicaID(int id) {
        return (id + 1) % this.numberOfReplicas;
    }
    /**
     * Get the next alive replica ID in a ring topology, skipping crashed nodes.
     * @return the next alive replica ID
     */
    private int getNextAliveReplicaID() {
        /** Proposed next replica ID */
        int nextReplicaID = this.id;
        do {
            nextReplicaID = nextReplicaID(nextReplicaID);
        } while (!this.replicas.containsKey(nextReplicaID));
        return nextReplicaID;
    }
    /**
     * Send a message to the next alive replica in a ring topology.
     * @param msg the message to send
     */
    private void sendToNextReplica(Serializable msg) {
        int nextReplicaID = this.getNextAliveReplicaID();
        ActorRef nextReplica = this.replicas.get(nextReplicaID);
        nextReplica.tell(msg, this.getSelf());
        // TODO: If the next replica doesn't respond, we should try the next one in the ring. This is not implemented yet.
    }
    /**
     * Start an election.
     */
    private void startElection() {
        ElectionStarted electionStarted = new ElectionStarted(this.id, this.coordinatorID);
        this.sendToNextReplica(electionStarted);
    }
}
