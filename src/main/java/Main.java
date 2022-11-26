import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Main {


    static ArrayList<Partition> topicpartitions1 = new ArrayList<>();
    static ArrayList<Partition> topicpartitions2 = new ArrayList<>();
    private static final Logger log = LogManager.getLogger(Main.class);

    static Instant lastUpScaleDecision = Instant.now();
    static Instant lastDownScaleDecision = Instant.now();
    static int size = 1;
    static double dynamicAverageMaxConsumptionRate = 0.0;
    static double wsla = 5.0;
    static List<Consumer> assignment = new ArrayList<>();


    public static void main(String[] args) throws InterruptedException {
        for (int i = 0; i <= 4; i++) {
            topicpartitions1.add(new Partition(i, 0, 0));
            topicpartitions2.add(new Partition(i, 0, 0));
        }

        log.info("Warming for 30 seconds.");
        Thread.sleep(30000);
        while (true) {
            log.info("Querying Prometheus");
            Main.QueryingPrometheus();
            log.info("Sleeping for 5 seconds");
            log.info("========================================");
            Thread.sleep(5000);
        }
    }


    static void QueryingPrometheus() {

        HttpClient client = HttpClient.newHttpClient();
        ////////////////////////////////////////////////////
        List<URI> partitions = new ArrayList<>();
        try {
            partitions = Arrays.asList(
                    new URI(Constants.topic1p0),
                    new URI(Constants.topic1p1),
                    new URI(Constants.topic1p2),
                    new URI(Constants.topic1p3),
                    new URI(Constants.topic1p4)
            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        List<URI> partitionslag = new ArrayList<>();
        try {
            partitionslag = Arrays.asList(
                    new URI(Constants.topic1p0lag),
                    new URI(Constants.topic1p1lag),
                    new URI(Constants.topic1p2lag),
                    new URI(Constants.topic1p3lag),
                    new URI(Constants.topic1p4lag)
            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        ///////////////////////////////////////////////////
        //launch queries for topic 1 lag and arrival get them from prometheus
        List<CompletableFuture<String>> partitionsfutures = partitions.stream()
                .map(target -> client
                        .sendAsync(
                                HttpRequest.newBuilder(target).GET().build(),
                                HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body))
                .collect(Collectors.toList());


        List<CompletableFuture<String>> partitionslagfuture = partitionslag.stream()
                .map(target -> client
                        .sendAsync(
                                HttpRequest.newBuilder(target).GET().build(),
                                HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body))
                .collect(Collectors.toList());


        int partition = 0;
        double totalarrivalstopic1 = 0.0;
        double partitionArrivalRate = 0.0;
        for (CompletableFuture<String> cf : partitionsfutures) {
            try {
                partitionArrivalRate = Util.parseJsonArrivalRate(cf.get(), partition);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            topicpartitions1.get(partition).setArrivalRate(partitionArrivalRate);

            totalarrivalstopic1 += partitionArrivalRate;
            partition++;
        }

        log.info("totalArrivalRate for  topic 1 {}", totalarrivalstopic1);


        partition = 0;
        double totallag = 0.0;
        long partitionLag = 0L;
        for (CompletableFuture<String> cf : partitionslagfuture) {
            try {
                partitionLag = Util.parseJsonArrivalLag(cf.get(), partition).longValue();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            topicpartitions1.get(partition).setLag(partitionLag);
            totallag += partitionLag;
            partition++;

        }

        log.info("totalLag for topic 1 {}", totallag);


        for (int i = 0; i <= 4; i++) {
            log.info("partition {} for topic 1 has the following arrival rate {} and lag {}", i, topicpartitions1.get(i).getArrivalRate(),
                    topicpartitions1.get(i).getLag());
        }

        if (Duration.between(lastUpScaleDecision, Instant.now()).getSeconds() > 15) {
            scaleAsPerBinPack(size);
        }


        ///////////////////////////////////////////////////////////////////////////////////////////////


        List<URI> partitions2 = new ArrayList<>();
        try {
            partitions2 = Arrays.asList(
                    new URI(Constants.topic2p0),
                    new URI(Constants.topic2p1),
                    new URI(Constants.topic2p2),
                    new URI(Constants.topic2p3),
                    new URI(Constants.topic2p4)
            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        List<URI> partitionslag2 = new ArrayList<>();
        try {
            partitionslag2 = Arrays.asList(
                    new URI(Constants.topic2p0lag),
                    new URI(Constants.topic2p1lag),
                    new URI(Constants.topic2p2lag),
                    new URI(Constants.topic2p3lag),
                    new URI(Constants.topic2p4lag)
            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        ///////////////////////////////////////////////////

        List<CompletableFuture<String>> partitionsfutures2 = partitions2.stream()
                .map(target -> client
                        .sendAsync(
                                HttpRequest.newBuilder(target).GET().build(),
                                HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body))
                .collect(Collectors.toList());


        List<CompletableFuture<String>> partitionslagfuture2 = partitionslag2.stream()
                .map(target -> client
                        .sendAsync(
                                HttpRequest.newBuilder(target).GET().build(),
                                HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body))
                .collect(Collectors.toList());


        int partition2 = 0;
        double totalarrivalstopic2 = 0.0;
        double partitionArrivalRate2 = 0.0;
        for (CompletableFuture<String> cf : partitionsfutures2) {
            try {
                partitionArrivalRate2 = Util.parseJsonArrivalRate(cf.get(), partition2);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            topicpartitions2.get(partition2).setArrivalRate(partitionArrivalRate2);

            totalarrivalstopic2 += partitionArrivalRate2;
            partition2++;
        }
        log.info("totalArrivalRate for  topic 2 {}", totalarrivalstopic2);


        partition2 = 0;
        double totallag2 = 0.0;
        long partitionLag2 = 0L;

        for (CompletableFuture<String> cf : partitionslagfuture2) {
            try {
                partitionLag2 = Util.parseJsonArrivalLag(cf.get(), partition2).longValue();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            topicpartitions2.get(partition2).setLag(partitionLag2);
            totallag2 += partitionLag2;
            partition2++;
        }


        log.info("totalLag for topic 2 {}", totallag2);


        for (int i = 0; i <= 4; i++) {
            log.info("topic 2 partition {} has the following arrival rate {} and lag {}", i, topicpartitions2.get(i).getArrivalRate(),
                    topicpartitions2.get(i).getLag());
        }
    }


    private static int binPackAndScale() {
        log.info("Inside binPackAndScale ");
        List<Consumer> consumers = new ArrayList<>();
        int consumerCount = 0;
        List<Partition> parts = new ArrayList<>(topicpartitions1);
        dynamicAverageMaxConsumptionRate = 90.0;

        long maxLagCapacity;
        maxLagCapacity = (long) (dynamicAverageMaxConsumptionRate * wsla);
        consumers.add(new Consumer((String.valueOf(consumerCount)), maxLagCapacity, dynamicAverageMaxConsumptionRate));

        //if a certain partition has a lag higher than R Wmax set its lag to R*Wmax
        // atention to the window
        for (Partition partition : parts) {
            if (partition.getLag() > maxLagCapacity) {
                log.info("Since partition {} has lag {} higher than consumer capacity times wsla {}" +
                        " we are truncating its lag", partition.getId(), partition.getLag(), maxLagCapacity);
                partition.setLag(maxLagCapacity);
            }
        }
        //if a certain partition has an arrival rate  higher than R  set its arrival rate  to R
        //that should not happen in a well partionned topic
        for (Partition partition : parts) {
            if (partition.getArrivalRate() > dynamicAverageMaxConsumptionRate) {
                log.info("Since partition {} has arrival rate {} higher than consumer service rate {}" +
                                " we are truncating its arrival rate", partition.getId(),
                        String.format("%.2f", partition.getArrivalRate()),
                        String.format("%.2f", dynamicAverageMaxConsumptionRate));
                partition.setArrivalRate(dynamicAverageMaxConsumptionRate);
            }
        }
        //start the bin pack FFD with sort
        Collections.sort(parts, Collections.reverseOrder());
        Consumer consumer = null;
        for (Partition partition : parts) {
            for (Consumer cons : consumers) {
                //TODO externalize these choices on the inout to the FFD bin pack
                // TODO  hey stupid use instatenous lag instead of average lag.
                // TODO average lag is a decision on past values especially for long DI.
                if (cons.getRemainingLagCapacity() >= partition.getLag() &&
                        cons.getRemainingArrivalCapacity() >= partition.getArrivalRate()) {
                    cons.assignPartition(partition);
                    // we are done with this partition, go to next
                    break;
                }
                //we have iterated over all the consumers hoping to fit that partition, but nope
                //we shall create a new consumer i.e., scale up
                if (cons == consumers.get(consumers.size() - 1)) {
                    consumerCount++;
                    consumer = new Consumer((String.valueOf(consumerCount)), (long) (dynamicAverageMaxConsumptionRate * wsla),
                            dynamicAverageMaxConsumptionRate);
                    consumer.assignPartition(partition);
                }
            }
            if (consumer != null) {
                consumers.add(consumer);
                consumer = null;
            }
        }
        log.info(" The BP scaler recommended {}", consumers.size());
        // copy consumers and partitions for fair assignment
        List<Consumer> fairconsumers = new ArrayList<>(consumers.size());
        List<Partition> fairpartitions = new ArrayList<>();

        for (Consumer cons : consumers) {
            fairconsumers.add(new Consumer(cons.getId(), maxLagCapacity, dynamicAverageMaxConsumptionRate));
            fairpartitions.addAll(cons.getAssignedPartitions());
        }

        //sort partitions in descending order for debugging purposes
        fairpartitions.sort(new Comparator<>() {
            @Override
            public int compare(Partition o1, Partition o2) {
                return Double.compare(o2.getArrivalRate(), o1.getArrivalRate());
            }
        });

        //1. list of consumers that will contain the fair assignment
        //2. list of consumers out of the bin pack.
        //3. the partition sorted in their decreasing arrival rate.
        assignPartitionsFairly(fairconsumers, consumers, fairpartitions);
        for (Consumer cons : fairconsumers) {
            log.info("fair consumer {} is assigned the following partitions", cons.getId());
            for (Partition p : cons.getAssignedPartitions()) {
                log.info("fair Partition {}", p.getId());
            }
        }
        assignment = fairconsumers;
        return consumers.size();
    }


    public static void assignPartitionsFairly(
            final List<Consumer> assignment,
            final List<Consumer> consumers,
            final List<Partition> partitionsArrivalRate) {
        if (consumers.isEmpty()) {
            return;
        }// Track total lag assigned to each consumer (for the current topic)
        final Map<String, Double> consumerTotalArrivalRate = new HashMap<>(consumers.size());
        final Map<String, Integer> consumerTotalPartitions = new HashMap<>(consumers.size());
        final Map<String, Double> consumerAllowableArrivalRate = new HashMap<>(consumers.size());
        for (Consumer cons : consumers) {
            consumerTotalArrivalRate.put(cons.getId(), 0.0);
            consumerAllowableArrivalRate.put(cons.getId(), 90.0);
            consumerTotalPartitions.put(cons.getId(), 0);

        }

        // might want to remove, the partitions are sorted anyway.
        //First fit decreasing
        partitionsArrivalRate.sort((p1, p2) -> {
            // If lag is equal, lowest partition id first
            if (p1.getArrivalRate() == p2.getArrivalRate()) {
                return Integer.compare(p1.getId(), p2.getId());
            }
            // Highest arrival rate first
            return Double.compare(p2.getArrivalRate(), p1.getArrivalRate());
        });
        for (Partition partition : partitionsArrivalRate) { //highest to lowest
            // Assign to the consumer with least number of partitions, then smallest total lag, then smallest id arrival rate
            // returns the consumer with lowest assigned partitions, if all assigned partitions equal returns the min total arrival rate
            final String memberId = Collections
                    .min(consumerTotalArrivalRate.entrySet(), (c1, c2) ->
                            Double.compare(c1.getValue(), c2.getValue()) != 0 ?
                                    Double.compare(c1.getValue(), c2.getValue()) : c1.getKey().compareTo(c2.getKey())).getKey();

            int memberIndex;
            for (memberIndex = 0; memberIndex < consumers.size(); memberIndex++) {
                if (assignment.get(memberIndex).getId().equals(memberId)) {
                    break;
                }
            }

            assignment.get(memberIndex).assignPartition(partition);
            consumerTotalArrivalRate.put(memberId, consumerTotalArrivalRate.getOrDefault(memberId, 0.0) + partition.getArrivalRate());
            consumerTotalPartitions.put(memberId, consumerTotalPartitions.getOrDefault(memberId, 0) + 1);
            log.info(
                    "Assigned partition {} to consumer {}.  partition_arrival_rate={}, consumer_current_total_arrival_rate{} ",
                    partition.getId(),
                    memberId,
                    String.format("%.2f", partition.getArrivalRate()),
                    consumerTotalArrivalRate.get(memberId));
        }
    }


    public static void scaleAsPerBinPack(int currentsize) {
        log.info("Currently we have this number of consumers {}", currentsize);
        int neededsize = binPackAndScale();
        log.info("We currently need the following consumers (as per the bin pack) {}", neededsize);

        int replicasForscale = neededsize - currentsize;
        // but is the assignmenet the same
        if (replicasForscale == 0) {
            log.info("No need to autoscale");
          /*  if(!doesTheCurrentAssigmentViolateTheSLA()) {
                //with the same number of consumers if the current assignment does not violate the SLA
                return;
            } else {
                log.info("We have to enforce rebalance");
                //TODO skipping it for now. (enforce rebalance)
            }*/
        } else if (replicasForscale > 0) {
            if (Duration.between(lastUpScaleDecision, Instant.now()).toSeconds() < 15) return;

            //TODO IF and Else IF can be in the same logic
            log.info("We have to upscale by {}", replicasForscale);
            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(neededsize);
                log.info("I have Upscaled you should have {}", neededsize);
                lastUpScaleDecision = Instant.now();
            }
        } else {
            if (Duration.between(lastDownScaleDecision, Instant.now()).toSeconds() < 30) return;
            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(neededsize);
                log.info("I have Downscaled you should have {}", neededsize);
                lastDownScaleDecision = Instant.now();
            }
        }
    }


}
