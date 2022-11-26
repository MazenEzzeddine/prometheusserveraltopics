import java.net.http.HttpClient;
import java.time.Instant;

public class Constants {


    //HttpClient client = HttpClient.newHttpClient();
  static  String topic1ar = "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,namespace=%22default%22%7D%5B1m%5D))%20by%20(topic)";
   static String topic1p0 =   "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,partition=%220%22,namespace=%22default%22%7D%5B20s%5D))";
   static  String topic1p1 =   "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,partition=%221%22,namespace=%22default%22%7D%5B20s%5D))";
    static String topic1p2 =   "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,partition=%222%22,namespace=%22default%22%7D%5B20s%5D))";
    static String topic1p3 =   "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,partition=%223%22,namespace=%22default%22%7D%5B20s%5D))";
    static String topic1p4 =   "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,partition=%224%22,namespace=%22default%22%7D%5B20s%5D))";



    //  "sum(kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22, namespace=%22kubernetes_namespace%7D)%20by%20(consumergroup,topic)"
    //sum(kafka_consumergroup_lag{consumergroup=~"$consumergroup",topic=~"$topic", namespace=~"$kubernetes_namespace"}) by (consumergroup, topic)

    static String topic1lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "sum(kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22,namespace=%22default%22%7D)%20by%20(consumergroup,topic)";
    static String topic1p0lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22,partition=%220%22,namespace=%22default%22%7D";
    static String topic1p1lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22,partition=%221%22,namespace=%22default%22%7D";
    static String topic1p2lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22,partition=%222%22,namespace=%22default%22%7D";
    static String topic1p3lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22,partition=%223%22,namespace=%22default%22%7D";
    static String topic1p4lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22,partition=%224%22,namespace=%22default%22%7D";



    ////////////////////////topic2


    static String topic2ar = "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic2%22,namespace=%22default%22%7D%5B1m%5D))%20by%20(topic)";
    static String topic2p0 =   "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic2%22,partition=%220%22,namespace=%22default%22%7D%5B20s%5D))";
    static String topic2p1 =   "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic2%22,partition=%221%22,namespace=%22default%22%7D%5B20s%5D))";
    static String topic2p2 =   "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic2%22,partition=%222%22,namespace=%22default%22%7D%5B20s%5D))";
    static String topic2p3 =   "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic2%22,partition=%223%22,namespace=%22default%22%7D%5B20s%5D))";
    static String topic2p4 =   "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic2%22,partition=%224%22,namespace=%22default%22%7D%5B20s%5D))";



    //  "sum(kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22, namespace=%22kubernetes_namespace%7D)%20by%20(consumergroup,topic)"
    //sum(kafka_consumergroup_lag{consumergroup=~"$consumergroup",topic=~"$topic", namespace=~"$kubernetes_namespace"}) by (consumergroup, topic)

    static String topic2lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "sum(kafka_consumergroup_lag%7Bconsumergroup=%22testgroup2%22,topic=%22testtopic2%22,namespace=%22default%22%7D)%20by%20(consumergroup,topic)";
    static String topic2p0lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup2%22,topic=%22testtopic2%22,partition=%220%22,namespace=%22default%22%7D";
    static String topic2p1lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup2%22,topic=%22testtopic2%22,partition=%221%22,namespace=%22default%22%7D";
    static String topic2p2lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup2%22,topic=%22testtopic2%22,partition=%222%22,namespace=%22default%22%7D";
    static  String topic2p3lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup2%22,topic=%22testtopic2%22,partition=%223%22,namespace=%22default%22%7D";
    static String topic2p4lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup2%22,topic=%22testtopic2%22,partition=%224%22,namespace=%22default%22%7D";
}
