package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.*;

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
    /** Replica specific filds */
    private final Map<UpdateClock, AbstractClient.WriteRequest> history = new HashMap<>();
    /** Queue for pending write requests */
    private final Queue<AbstractClient.WriteRequest> pendingWrites = new ArrayDeque<>();
    /** List of coordinator filed */
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
        return this.numberOfReplicas;
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
                .match(ElectionMessage.class,             this::onElectionMessage)
                .match(CoordinatorElected.class,          this::onCoordinatorElected)
                .match(ElectionAck.class,                 this::onElectionAck)
                // TODO add your message handlers here .match(, )
                .build();
    }

    public Receive crashed() {
        return receiveBuilder()
                .matchAny(msg -> {})
                .build();
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
        log("Recived a Write request by "+ getSender().path().name() + " with content: {index:"+msg.index+", value:"+msg.value+"}");
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
        if (this.updateClock.compareTo(identifier) > 0 ) {
            return;
        }
        this.updateClock.syncClock(identifier);
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
                this.onCoordinatorCrash();
                break;
            case TimeOut.TimeoutType.WriteRequest:
                this.onCoordinatorCrash();
                break;
            case TimeOut.TimeoutType.Heartbeat:
                this.onCoordinatorCrash();
                break;
            case TimeOut.TimeoutType.Election:
                this.onElectionTimout();
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
            List<Integer> candidatesList = null;
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
            this.candidates.keySet().removeIf(id -> !replicas.containsKey(id));
        }
        /**
         * Delete crashed nodes from replicas list based on the current list of candidates.
         * @param replicas list of alive replicas according to the current node
         * @return the updated list of candidates after removing crashed nodes
         */
        public Map<Integer, ActorRef> deleteCrashedNodesFromList(Map<Integer, ActorRef> replicas) {
            replicas.keySet().removeIf(id -> !this.candidates.containsKey(id));
            return replicas;
        }
    }
    class ElectionAck implements Serializable {
        public final int ackSenderId;
        public ElectionAck(int ackSenderId) {
            this.ackSenderId = ackSenderId;
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
     * Handle the event when this replica becomes the coordinator.
     */
    private void onBecameCoordinator() {
        log("I am the new coordinator");
        //TODO: Implement the logic for when this replica becomes the coordinator, such as sending heartbeats or managing pending writes.
    }
    /**
     * Handle a coordinator elected message by updating the coordinator ID and
     * forwarding the message to the next replica if necessary.
     * @param msg the coordinator elected message
     */
    private void onCoordinatorElected(CoordinatorElected msg) {
        this.coordinatorID = msg.newCoordinatorId;
        if (msg.replicaId == this.id || this.electionInProgress == null) {
            return;
        }
        this.callbackOnCoordinatorElected(this.coordinatorID);
        if (msg.newCoordinatorId == this.id) {
            this.onBecameCoordinator();
        }
        this.electionInProgress = null;
        this.sendToNextReplica(msg);
        // TODO: If the next replica doesn't respond, we should try the next one in the ring. This is not implemented yet.
    }
    /**
     * Send an acknowledgment message to the sender of an election message.
     * @param msg the election message
     */
    private void sendAckToSender(ElectionMessage msg) {
        /** Acknowledgment message */
        ElectionAck ack = new ElectionAck(this.id);
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
        } else {
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
        this.replicas.remove(id);
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
        if (this.replicas.size() == 0) {
            throw new IllegalStateException("No alive replicas available");
        }
        if (this.nextReplicaID == null) {
            this.nextReplicaID = this.correctNextReplicaID(this.id + 1);
        }
        while (!this.replicas.containsKey(this.nextReplicaID)) {
            this.nextReplicaID = this.correctNextReplicaID(this.nextReplicaID + 1);
        } 
        return this.nextReplicaID;
    }
    private void onElectionTimout() {
        debug("Election timeout, starting a new election");
        this.startElection();
    }
    private void onElectionAck(ElectionAck msg) {
        debug("Recived an ElectionAck from "+msg.ackSenderId);
    }
    /**
     * Send a message to the next alive replica in a ring topology.
     * @param msg the message to send
     */
    private void sendToNextReplica(Serializable msg) {
        ActorRef nextReplica = this.replicas.get(this.getNextAliveReplicaID());
        nextReplica.tell(msg, this.getSelf());
        // TODO: If the next replica doesn't respond, we should try the next one in the ring. This is not implemented yet.
    }
    /**
     * Start an election.
     */
    private void startElection() {
        ElectionMessage electionMessage = new ElectionMessage(this.id, this.updateClock);
        if (this.electionInProgress != null && this.electionInProgress <= this.id) {
            // An election is already in progress
            return;
        }
        this.electionInProgress = this.id;
        this.callbackOnElectionStarted(this.coordinatorID);
        this.sendToNextReplica(electionMessage);
    }
}
