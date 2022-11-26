import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Main {


    static ArrayList<Partition> topicpartitions1 = new ArrayList<>();
    static ArrayList<Partition> topicpartitions2 = new ArrayList<>();
    private static final Logger log = LogManager.getLogger(Main.class);



    public static void main(String[] args) throws InterruptedException {

        for (int i = 0; i<=4; i++) {
            topicpartitions1.add(new Partition(i, 0, 0));
            topicpartitions2.add(new Partition(i, 0, 0));

        }


        log.info("Warming for 30 seconds.");
        Thread.sleep(30000);

        while (true) {
            log.info("Querying Prometheus");
            Main.QueryingPrometheus();
            log.info("Sleeping for 5 seconds");
            Thread.sleep(5000);
        }
    }



     static void QueryingPrometheus() {

        HttpClient client = HttpClient.newHttpClient();
        ////////////////////////////////////////////////////
        List<URI> partitions= new ArrayList<>();
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
        List<URI> partitionslag= new ArrayList<>();
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



        int partitionn = 0;
        double totalarrivals=0.0;
        for (CompletableFuture<String> cf : partitionsfutures) {
            try {
                topicpartitions1.get(partitionn).setArrivalRate(Util.parseJsonArrivalRate(cf.get(), partitionn));
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            try {
                totalarrivals += Util.parseJsonArrivalRate( cf.get(), partitionn);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            partitionn++;
        }
        log.info("totalArrivalRate {}", totalarrivals);



        partitionn = 0;
        double totallag=0.0;
        for (CompletableFuture<String> cf : partitionslagfuture) {
            try {
                topicpartitions1.get(partitionn).setLag(Util.parseJsonArrivalLag( cf.get(), partitionn).longValue());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            try {
                totallag += Util.parseJsonArrivalLag( cf.get(), partitionn);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            partitionn++;
        }


        log.info("totalLag for topic 1 {}", totallag);
       /* Instant end = Instant.now();
        log.info("Duration in seconds to query prometheus for " +
                        "arrival rate and lag and parse result {}",
                Duration.between(start,end).toMillis());*/


        for (int i = 0; i<=4; i++) {
            log.info("partition {} for topic 1 has the following arrival rate {} and lag {}",  i, topicpartitions1.get(i).getArrivalRate(),
                    topicpartitions1.get(i).getLag()) ;
        }



    ///////////////////////////////////////////////////////////////////////////////////////////////




    List<URI> partitions2= new ArrayList<>();
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
    List<URI> partitionslag2= new ArrayList<>();
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



    int partitionn2 = 0;
    double totalarrivals2=0.0;
        for (CompletableFuture<String> cf : partitionsfutures2) {
        try {
            topicpartitions2.get(partitionn2).setArrivalRate(Util.parseJsonArrivalRate((String) cf.get(), partitionn2));
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        try {
            totalarrivals2 += Util.parseJsonArrivalRate((String) cf.get(), partitionn2);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        partitionn2++;
    }
        log.info("totalArrivalRate for topic 2{}", totalarrivals2);



    partitionn2 = 0;
    double totallag2=0.0;
        for (CompletableFuture<String> cf : partitionslagfuture2) {
        try {
            topicpartitions2.get(partitionn2).setLag(Util.parseJsonArrivalLag((String) cf.get(), partitionn2).longValue());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        try {
            totallag2 += Util.parseJsonArrivalLag((String) cf.get(), partitionn2);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        partitionn2++;
    }


        log.info("totalLag for topic 2 {}", totallag2);



        for (int i = 0; i<=4; i++) {
        log.info("topic 2 partition {} has the following arrival rate {} and lag {}",  i, topicpartitions2.get(i).getArrivalRate(),
                topicpartitions2.get(i).getLag()) ;
    }
}




}
