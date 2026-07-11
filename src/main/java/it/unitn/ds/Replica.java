package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import it.unitn.ds.TimeOut.TimeoutType;
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
    // ________________________________
    // Replica specific variables
    // ________________________________
    /** HashMap to store the history of update commited by the replica */
    private final Map<UpdateClock, AbstractClient.WriteRequest> history = new HashMap<>();
    /** HashMap to store the update ready to be commited by the replica but waiting for the WriteOK message*/
    private final Map<UpdateClock, AbstractClient.WriteRequest> waitingForWriteOK = new HashMap<>();
    /** Queue for pending write requests */
    private final Queue<ClientWrite> pendingWrites = new ArrayDeque<>();
    /**
     * Record to represent a write request from a client
     * @param clientRef the reference of the client that sent the write request
     * @param writeRequest the write request sent by the client
     */
    private record ClientWrite(ActorRef clientRef, AbstractClient.WriteRequest writeRequest){ }
    /** HashMap to store Cancellable for timeout on the writeRequest message send to the coordinator */
    private final HashMap<AbstractClient.WriteRequest, Queue<Cancellable>> writeRequestTimeouts = new HashMap<>();
    /** HashMap to store Cancellable for timeout on the election message send to the coordinator */
    private final HashMap<Serializable, Queue<Cancellable>> electionTimeouts = new HashMap<>();
    /** HashMap to store Cancellable for timeout on the updateRequest message send to the coordinator */
    private final HashMap<UpdateClock, Cancellable> updateRequestTimeouts = new HashMap<>();
    /** Cancellable for timeout on the coordinator heartbeat */
    private Cancellable coordinatorHeartbeatTimeout = null;
    // ________________________________
    // Coordinator specific variables
    // ________________________________
    /** Hashmap to count the number of ACK received for each message sent by the coordinator */
    private final Map<UpdateClock, Integer> UpdateACKCounter = new HashMap<>();
    /** 
     * Current election status, 
     * null if no election is in progress 
     * else the ID of the election starter
     */
    private Integer electionInProgress = null;
    /** The ID of the next replica in a ring topology */
    private Integer nextReplicaID = null;
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
        //  return 0;
        return this.numberOfReplicas;
        //return this.replicas.size();
    }

    @Override
    public void crash(AbstractReplica.Crash how_to_crash) {
        getContext().become(crashed());
    }

    @Override
    public void initSystem(InitSystem sysInit) {
        this.replicas = sysInit.group;
        this.numberOfReplicas = sysInit.group.size();
        int coordinator_id = sysInit.coordinator_id;
        this.coordinatorID = coordinator_id;
        log("I set as coordinator: "+coordinator_id);
        if (this.isCoordinator()) {
            this.sendHeartbeat();
        }
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
                .match(ElectionMessage.class,             this::onElectionMessage)
                .match(CoordinatorElected.class,          this::onCoordinatorElected)
                .match(ElectionAck.class,                 this::onElectionAck)
                .match(CoordinatorHeartbeat.class,        this::onHeartbeat)
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
    Cancellable setTimeout(long time, TimeOut timeOut) {
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

    public static class ClientACK implements Serializable {
        ActorRef actorRef;
        Serializable msg;

        public ClientACK(ActorRef actorRef, Serializable msg) {
            this.actorRef = actorRef;
            this.msg = msg;

        }
    }

    // =================================================================================
    // Messages handler functions
    // =================================================================================

    private void onWriteRequest(AbstractClient.WriteRequest msg) {
        if (getSender() != msg.replica) {
            debug("Inserting the request inside my pending write queue");
            this.pendingWrites.add(new ClientWrite(getSender(), msg));
        }
        log("Received a Write request by "+ getSender().path().name() + " with content: {index:"+msg.index+", value:"+msg.value+"}");
        if (this.isCoordinator()) {
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
            this.writeRequestTimeouts.computeIfAbsent(msg, k -> new ArrayDeque<>())
                    .add(setTimeout(this.getMaxLatencyPlusTolerance()+500000,new TimeOut(TimeOut.TimeoutType.WriteRequest)));
                    // TODO how much time to wait for coordinator?
        }
    }

    private void onUpdateRequest(Replica.UpdateRequest msg) {
        log("Received an Update request from coordinator with content: {index:"+msg.writeRequest.index+", value:"+msg.writeRequest.value+"}");
        // cancel the WriteRequest timeout
        Queue<Cancellable> timeouts = this.writeRequestTimeouts.get(msg.writeRequest);
        if ( timeouts != null) {
            timeouts.poll().cancel();
            if (this.writeRequestTimeouts.get(msg.writeRequest).isEmpty()) {
                this.writeRequestTimeouts.remove(msg.writeRequest);
            }
        }
        // add update to history
        waitingForWriteOK.put(msg.identifier, msg.writeRequest);
        msg.coordinator.tell(new UpdateACK(msg.identifier), this.getSelf());
        this.updateRequestTimeouts
                .putIfAbsent(msg.identifier, setTimeout(this.getMaxLatencyPlusTolerance()+1000000,new TimeOut(TimeOut.TimeoutType.UpdateRequest)));
                // TODO how much time to wait for coordinator?
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
        // cancel the UpdateRequest timeout
        Cancellable timeout = this.updateRequestTimeouts.remove(msg.identifier);
        if ( timeout != null) {
            timeout.cancel();

        }
        UpdateClock identifier = msg.identifier;
        AbstractClient.WriteRequest writeRequest = this.waitingForWriteOK.remove(identifier);
        this.positions[writeRequest.index] = writeRequest.value;
        this.history.put(identifier,writeRequest);
        if (this.updateClock.compareTo(identifier) > 0 ) {
            return;
        }
        this.updateClock.syncClock(identifier);
        this.callbackOnUpdateApplied(writeRequest.index,writeRequest.value);
        debug("New positions is: "+ Arrays.toString(this.positions));
        debug("pending write"+ pendingWrites.toString());

        Optional<ClientWrite> pendingWrite = this.pendingWrites.stream()
                .filter(p -> (p.writeRequest.index == writeRequest.index && p.writeRequest.value == writeRequest.value))
                .findFirst();
        if (pendingWrite.isPresent()) {
            pendingWrite.get().clientRef.tell(
                    new Replica.ClientACK(this.getSelf(), new AbstractClient.WriteResult(true, writeRequest.index, writeRequest.value, this.id)),
//                    new AbstractClient.WriteResult(true, writeRequest.index, writeRequest.value, this.id),
                    this.getSelf());
            this.pendingWrites.remove(pendingWrite.get());
        }
    }

    private void onReadRequest(AbstractClient.ReadRequest msg) {
        debug("Received a Read request from client");
        int position = this.positions[msg.index];
        getSender().tell( new Replica.ClientACK(this.getSelf(), new AbstractClient.ReadResult(true,msg.index, position, this.id)), this.getSelf());
//                , this.getSelf());

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
            case TimeOut.TimeoutType.WriteRequest:
                this.onCoordinatorCrash();
                break;
            case TimeOut.TimeoutType.Heartbeat:
                this.onHeartbeatTimeout();
                break;
            case TimeOut.TimeoutType.Election:
                this.onElectionTimeout();
                break;
            default:
                break;
        }
    }
    /**
     * ElectionMessage
     * This class represents a message used in the election process to determine the coordinator among replicas.
     */
    class ElectionMessage implements Serializable {
        /** The ID of the node that started the election */
        public final int electionStarter;
        /** A map of all candidates in the election, with their IDs as keys and clock values as values */
        public final Map<Integer, UpdateClock> candidates = new HashMap<Integer, UpdateClock>();
        /** The ID of the node that send the message */
        public int msgSenderId;
        /**
         * Constructor for ElectionMessage
         * @param electionStarter the ID of the node that started the election
         * @param nodeClock the clock value of the node that started the election
         */
        public ElectionMessage(int electionStarter, UpdateClock nodeClock) {
            this.electionStarter = electionStarter;
            this.msgSenderId = electionStarter;
            this.candidates.put(electionStarter, nodeClock);
        }
        /**
         * Update the message with the information of a replica
         * @param replica the replica that updates the message
         */
        public void updateMsg(Replica replica) {
            this.candidates.put(replica.id, replica.updateClock);
            this.msgSenderId = replica.id;
            this.deleteCrashedNodesFromCandidates(replica.replicas);
        }
        /**
         * Get the best candidate for the election based on the highest clock and ID.
         * The best candidate is the one with the highest clock value, 
         * and in case of a tie, the one with the highest ID.
         * @return the ID of the best candidate, or null if there are no candidates
         */
        public Integer getBestCandidate() {
            /** Best candidate */
            Integer bestCandidate = null;
            /** List of all candidates with the same best clock */
            List<Integer> candidatesList =  null;
            /** Best clock in the election */
            UpdateClock bestClock = candidates.values().stream().max(UpdateClock::compareTo).orElse(null);
            // If there is no best clock, return null
            if (bestClock == null) {
                return null;
            }
            // Get all candidates with the best clock
            candidatesList = candidates.entrySet().stream()
                    .filter(entry -> entry.getValue().equals(bestClock))
                    .map(Map.Entry::getKey)
                    .toList();
            if (candidatesList.isEmpty()) {
                return null;
            }
            // Get the candidate with the highest ID between the candidates with the best clock
            bestCandidate = candidatesList.stream().max(Integer::compareTo).orElse(null);
            return bestCandidate;
        }
        /**
         * Check if the election is over based on the election starter.
         * @param electionInitiator the ID of the election initiator
         * @return true if the election is over, false otherwise
         */
        public boolean isElectionOver(Integer electionInitiator) {
            if (electionInitiator == null) {
                return false;
            }
            return this.electionStarter == electionInitiator;
        }
        /**
         * Delete crashed nodes from the election message 
         * based on the current list of replicas.
         * Delete the id and the clock of the crashed nodes from the election message
         * where the id is not present in the list of alive replicas.
         * @param replicas list of alive replicas according to the current node
         */
        public void deleteCrashedNodesFromCandidates(Map<Integer, ActorRef> replicas) {
            if (replicas == null || replicas.isEmpty()) {
                return;
            }
            this.candidates.keySet().removeIf(id -> !replicas.containsKey(id));
        }
        /**
         * Delete crashed nodes from replicas list based on the current list of candidates.
         * @param replicas list of alive replicas according to the current node
         * @return the updated list of candidates after removing crashed nodes
         */
        public Map<Integer, ActorRef> deleteCrashedNodesFromList(Map<Integer, ActorRef> replicas) {
            if (replicas == null || replicas.isEmpty() || this.candidates == null || this.candidates.isEmpty()) {
                return replicas;
            }
            replicas.keySet().removeIf(id -> !this.candidates.containsKey(id));
            return replicas;
        }
    }
    public static class ElectionAck implements Serializable {
        public final ElectionMessage electionMessage;
        public ElectionAck(ElectionMessage electionMessage) {
            this.electionMessage = electionMessage;
        }
    }
    /**
     * Handle a coordinator crash by removing it from the 
     * list of replicas and starting an election.
     */
    private void onCoordinatorCrash() {
        this.nodeCrashed(this.coordinatorID);
    }
    /**
     * Handle the event when this replica becomes the coordinator.
     */
    private void onBecameCoordinator() {
        this.callbackOnCoordinatorElected(this.coordinatorID);
        this.sendHeartbeat();
        // TODO: implement any additional logic needed when this replica becomes the coordinator
    }
    /**
     * Handle a coordinator elected message by updating the coordinator ID and
     * forwarding the message to the next replica if necessary.
     * @param msg the coordinator elected message
     */
    private void onCoordinatorElected(CoordinatorElected msg) {
        this.coordinatorID = msg.newCoordinatorId;
        if (msg.replicaId == this.id || !this.isElectionInProgress()) {
            return;
        }
        if (msg.newCoordinatorId == this.id) {
            this.onBecameCoordinator();
        }
        this.electionInProgress = null;
        this.sendToNextReplica(msg);
        this.electionTimeouts.computeIfAbsent(this.nextReplicaID, k -> new ArrayDeque<>())
                .add(setTimeout(this.getMaxLatencyPlusTolerance(),new TimeOut(TimeoutType.Election)));
    }
    /**
     * Send an acknowledgment message to the sender of an election message.
     * @param msg the election message
     */
    private void sendAckToSender(ElectionMessage msg) {
        /** Acknowledgment message */
        ElectionAck ack = new ElectionAck(msg);
        /** Sender ID */
        Integer senderId = msg.msgSenderId;
        if (senderId != null && this.replicas.containsKey(senderId)) {
            ActorRef senderReplica = this.replicas.get(senderId);
            senderReplica.tell(ack, this.getSelf());
        }
    }
    /**
     * Handle an election message by updating the election state and 
     * forwarding the message to the next replica.
     * @param msg the election message
     */
    private void onElectionMessage(ElectionMessage msg) {
        if (msg.isElectionOver(electionInProgress)) {
            this.electionInProgress = null;
            /** Best candidate for coordinator */
            Integer bestCandidate = msg.getBestCandidate();
            if (bestCandidate != null) {
                this.replicas = msg.deleteCrashedNodesFromList(this.replicas);
                /** Coordinator elected message */
                CoordinatorElected coordinatorElected = new CoordinatorElected(bestCandidate, this.id);
                this.sendToNextReplica(coordinatorElected);
            }
        } else if (!this.isElectionInProgress() || msg.electionStarter < this.electionInProgress) {
            this.electionInProgress = msg.electionStarter;
            msg.updateMsg(this);
            this.sendToNextReplica(msg);
        }
        this.sendAckToSender(msg);
    }
    /**
     * Handle a node crash by removing it from the list of replicas.
     * @param id the id of the crashed node
     */
    private void nodeCrashed(int id){
        try {
            this.replicas.remove((Integer) id);
            if (this.coordinatorID == id) {
                this.startElection();
            }
        } catch (Exception e) {
            debug("Error while removing crashed node: " + e.getMessage());
        }
    }
    /**
     * Get the next replica ID in a ring topology.
     * @param id the current replica ID
     * @return the next replica ID
     */
    private int correctNextReplicaID(int id) {
        return (id + 1) % this.numberOfReplicas;
    }
    /**
     * Get the next alive replica ID in a ring topology, skipping crashed nodes.
     * @return the next alive replica ID
     */
    private synchronized int getNextAliveReplicaID() {
        if (this.replicas == null || this.replicas.isEmpty()) {
            debug("No replicas available. Returning -1 as next alive replica ID.");
            return this.nextReplicaID;
        }
        if (this.nextReplicaID == null) {
            this.nextReplicaID = this.correctNextReplicaID(this.id + 1);
        }
        while (!this.replicas.containsKey(this.nextReplicaID)) {
            this.nextReplicaID = this.correctNextReplicaID(this.nextReplicaID + 1);
        } 
        return this.nextReplicaID;
    }
    /**
     * Handle an election timeout.
     */
    private void onElectionTimeout() {
        debug("Election timeout. Removing the next replica from the list of replicas.");
        // get and remove the first inserted ElectionMessage in the electionTimeouts map
        this.nodeCrashed(this.nextReplicaID);
        if (this.electionTimeouts != null && !this.electionTimeouts.isEmpty()) {
            Serializable first = this.electionTimeouts.keySet().iterator().next();
            this.electionTimeouts.remove(first);
            this.sendToNextReplica(first);
        }
    }
    /**
     * Handle an election acknowledgment message by canceling the corresponding timeout.
     * @param msg the election acknowledgment message
     */
    private void onElectionAck(ElectionAck msg) {
        debug("Received an ElectionAck");
        this.electionTimeouts.computeIfAbsent(msg.electionMessage, k -> new ArrayDeque<>())
                .poll().cancel();
    }
    /**
     * Send a message to the next alive replica in a ring topology.
     * @param msg the message to send
     */
    private void sendToNextReplica(Serializable msg) {
        ActorRef nextReplica = this.replicas.get(this.getNextAliveReplicaID());
        nextReplica.tell(msg, this.getSelf());
        // Set a timeout for the next replica to respond to the election message
        this.electionTimeouts.computeIfAbsent(msg, k -> new ArrayDeque<>())
                .add(setTimeout(this.getMaxLatencyPlusTolerance(),new TimeOut(TimeoutType.Election)));
    }
    /**
     * Start an election.
     */
    private void startElection() {
        ElectionMessage electionMessage = new ElectionMessage(this.id, this.updateClock);
        if (this.isElectionInProgress() && this.electionInProgress <= this.id) {
            // An election is already in progress
            return;
        }
        this.electionInProgress = this.id;
        this.callbackOnElectionStarted(this.coordinatorID);
        this.sendToNextReplica(electionMessage);
        debug("Starting an election with message: " + electionMessage.toString());
    }
    /**
     * CoordinatorHeartbeat
     * This class represents a heartbeat message sent by the coordinator to indicate that it is alive.
     */
    public static class CoordinatorHeartbeat implements Serializable {
        /** The ID of the coordinator sending the heartbeat */
        public final int coordinatorId;
        /** The timestamp of the heartbeat */
        public CoordinatorHeartbeat(int coordinatorId) {
            this.coordinatorId = coordinatorId;
        }
    }
    /**
     * Handle a heartbeat message from the coordinator.
     * @param msg the heartbeat message
     */
    private void onHeartbeat(CoordinatorHeartbeat msg) {
        /** Handle a heartbeat message from the coordinator */
        long timeout = (long)COORDINATOR_BEAT_INTERVAL;
        // If this replica is the coordinator
        // set half the timeout to let it expire faster and trigger its own heartbeat
        if (this.isCoordinator()) {
            timeout /= 2;
        }
        // Cancel the previous heartbeat timeout
        if (this.coordinatorHeartbeatTimeout != null) {
            this.coordinatorHeartbeatTimeout.cancel();
        }
        // Set a new heartbeat timeout
        this.coordinatorHeartbeatTimeout = this.setTimeout(timeout, new TimeOut(TimeoutType.Heartbeat));
    }
    /**
     * Handle a heartbeat timeout.
     * If the coordinator is this replica, send a heartbeat message.
     * Otherwise, handle the coordinator crash.
     */
    private void onHeartbeatTimeout() {
        if (this.isCoordinator()) {
            this.sendHeartbeat();
        } else {
            this.onCoordinatorCrash();
        }
    }
    /**
     * Send a heartbeat message to all replicas.
     */
    private void sendHeartbeat() {
        this.multicast(new CoordinatorHeartbeat(this.id));
    }
    /**
     * Check if this replica is the coordinator.
     * @return true if this replica is the coordinator, false otherwise
     */
    public boolean isCoordinator() {
        return this.coordinatorID == this.id;
    }
    /**
     * Check if an election is in progress.
     * @return true if an election is in progress, false otherwise
     */
    public boolean isElectionInProgress() {
        return this.electionInProgress != null;
    }
}
