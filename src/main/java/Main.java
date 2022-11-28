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



    private static final Logger log = LogManager.getLogger(Main.class);



    public static void main(String[] args) throws InterruptedException {
        for (int i = 0; i <= 4; i++) {
            Scale.topicpartitions1.add(new Partition(i, 0, 0));
            Scale.topicpartitions2.add(new Partition(i, 0, 0));
            Scale2.topicpartitions1.add(new Partition(i, 0, 0));
            Scale2.topicpartitions2.add(new Partition(i, 0, 0));
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

            Scale.topicpartitions1.get(partition).setArrivalRate(partitionArrivalRate);

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

            Scale.topicpartitions1.get(partition).setLag(partitionLag);
            totallag += partitionLag;
            partition++;

        }

        log.info("totalLag for topic 1 {}", totallag);


        for (int i = 0; i <= 4; i++) {
            log.info("partition {} for topic 1 has the following arrival rate {} and lag {}", i, Scale.topicpartitions1.get(i).getArrivalRate(),
                    Scale.topicpartitions1.get(i).getLag());
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

            Scale2.topicpartitions2.get(partition2).setArrivalRate(partitionArrivalRate2);

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

            Scale2.topicpartitions2.get(partition2).setLag(partitionLag2);
            totallag2 += partitionLag2;
            partition2++;
        }


        log.info("totalLag for topic 2 {}", totallag2);


        for (int i = 0; i <= 4; i++) {
            log.info("topic 2 partition {} has the following arrival rate {} and lag {}", i, Scale2.topicpartitions2.get(i).getArrivalRate(),
                    Scale2.topicpartitions2.get(i).getLag());
        }




 /*       if (Duration.between(Scale.lastUpScaleDecision, Instant.now()).getSeconds() > 15) {
            Scale.scaleAsPerBinPack(Scale.size);
        }*/

        if (Duration.between(Scale2.lastUpScaleDecision, Instant.now()).getSeconds() > 15) {
            Scale2.scaleAsPerBinPack(Scale2.size);
        }



    }









}
