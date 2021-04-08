package com.example.kafkaexample.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Component
@Slf4j
public class KafkaUtil {

    AdminClient admin;

    @PostConstruct
    private void postConstruct() {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        admin = AdminClient.create(config);
    }

    public List<String> getKafkaGroupIds(){
        List<String> allGroupIds = null;
        try {
            allGroupIds = admin.listConsumerGroups()
                                            .valid()
                                            .thenApply(r -> r.stream()
                                                    .map(ConsumerGroupListing::groupId)
                                                    .collect(Collectors.toList())
                                            ).get();
        } catch (InterruptedException e) {
            log.error("### KafkaUtil.java >> getKafkaGroupIds() >> InterruptedException ###",e);
        } catch (ExecutionException e) {
            log.error("### KafkaUtil.java >> getKafkaGroupIds() >> ExecutionException ###",e);
        }
        return allGroupIds;
    }

    public Map<String, Long> getOffsetDetails(String groupId){
        Map<String, Long> resultMap = null;
        try {
            Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
            //System.out.println(topicPartitionOffsetAndMetadataMap);
            resultMap = topicPartitionOffsetAndMetadataMap.entrySet()
                                                          .stream()
                                                          .collect(Collectors.toMap(k -> k.getKey().toString(), e -> e.getValue().offset()));
        } catch (InterruptedException e) {
            log.error("### KafkaUtil.java >> getOffsetDetails() >> InterruptedException ###",e);
        } catch (ExecutionException e) {
            log.error("### KafkaUtil.java >> getOffsetDetails() >> ExecutionException ###",e);
        }
        return resultMap;
    }

    public void getkafkaDetails() throws ExecutionException, InterruptedException {
        log.info("############################### Starting Kafka Details ###############################");
        for (Node node : admin.describeCluster().nodes().get()) {
            System.out.println("-- node: " + node.id() + " --");
            ConfigResource cr = new ConfigResource(ConfigResource.Type.BROKER, "0");
            DescribeConfigsResult dcr = admin.describeConfigs(Collections.singleton(cr));
            dcr.all().get().forEach((k, c) -> {
                c.entries()
                        .forEach(configEntry -> {System.out.println(configEntry.name() + "= " + configEntry.value());});
            });
        }
        log.info("############################### Starting Kafka Details ###############################");
        log.info("############################### Starting Topic Details ###############################");
        for (TopicListing topicListing : admin.listTopics().listings().get()) {
            log.info("### Topic Name : {} ###", topicListing.name());
            String topicName = topicListing.name();
            if (topicName.contains("advice-topic")){
                log.info("############################### Start Describing Topic {} ###############################",topicName);
                admin.describeTopics(Collections.singleton(topicName)).all().get()
                        .forEach((topic, desc) -> {
                            System.out.println("Topic: " + topic);
                            System.out.printf("Partitions: %s, partition ids: %s%n", desc.partitions().size(),
                                    desc.partitions()
                                            .stream()
                                            .map(p -> Integer.toString(p.partition()))
                                            .collect(Collectors.joining(",")));
                        });
                log.info("############################### End Describing Topic {} ###############################",topicName);
            }
        }
        log.info("############################### Ending Topic Details ###############################");
    }
}
