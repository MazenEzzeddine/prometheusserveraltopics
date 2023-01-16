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


    public static void main(String[] args) throws InterruptedException, ExecutionException {
        for (int i = 0; i <= 4; i++) {
            Scale.topicpartitions1.add(new Partition(i, 0, 0));
            Scale2.topicpartitions2.add(new Partition(i, 0, 0));
            ArrivalRates.topicpartitions4.add(new Partition(i, 0, 0));
            ArrivalRates.topicpartitions3.add(new Partition(i, 0, 0));
            Scale5.topicpartitions5.add(new Partition(i, 0, 0));
            Scale5.topicpartitions5lag.add(new Partition(i, 0, 0));
            Scalep.topicpartitions1.add(new Partition(i, 0, 0));
            Scale2p.topicpartitions2.add(new Partition(i, 0, 0));
            Scale5p.topicpartitions5.add(new Partition(i, 0, 0));
            Scale5p.topicpartitions5avg.add(new Partition(i, 0, 0));


        }
/*        log.info("Warming for 3 minutes seconds.");
        Thread.sleep(180000);*/
        log.info("Warming for 30 seconds.");
        Thread.sleep(30000);
        while (true) {
            log.info("Querying Prometheus");
            Main.QueryingPrometheus();
            log.info("Sleeping for 3 seconds");
            log.info("========================================");
            Thread.sleep(5000);
        }
    }


    static void QueryingPrometheus() throws ExecutionException, InterruptedException {


        ///////////////////////////////////////////////////////////////////////////////////////////////

        ArrivalRates.arrivalRateTopic1();
       ArrivalRates.arrivalRateTopic2();
        ArrivalRates.arrivalRateTopic3();
         //ArrivalRates.arrivalRateTopic4();
        ArrivalRates.arrivalRateTopic5();
        //arrivalRateTopic5Avg();

        if (Duration.between(Scale.lastUpScaleDecision, Instant.now()).getSeconds() > 15) {
            //QueryRate.queryConsumerGroup();
            Scalep.scaleAsPerBinPack(Scalep.size);
        }

        if (Duration.between(Scale2.lastUpScaleDecision, Instant.now()).getSeconds() > 15) {
            //QueryRate.queryConsumerGroup();
            Scale2p.scaleAsPerBinPack(Scale2p.size);
        }

        if (Duration.between(Scale5.lastUpScaleDecision, Instant.now()).getSeconds() > 15) {
           // QueryRate.queryConsumerGroup();
            Scale5p.scaleAsPerBinPack(Scale5p.size);
        }
    }






}
