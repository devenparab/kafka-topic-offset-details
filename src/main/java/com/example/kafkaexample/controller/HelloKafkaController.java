package com.example.kafkaexample.controller;

import com.example.kafkaexample.model.PracticalAdvice;
import com.example.kafkaexample.util.KafkaUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@RestController
@Slf4j
public class HelloKafkaController {

    private final KafkaTemplate<String, Object> template;
    private final String topicName;
    private final int messagesPerRequest;
    private CountDownLatch latch;

    @Autowired
    private KafkaUtil kafkaUtil;

    public HelloKafkaController(final KafkaTemplate<String, Object> template,
                                @Value("${tpd.topic-name}") final String topicName,
                                @Value("${tpd.messages-per-request}") final int messagesPerRequest) {
        this.template = template;
        this.topicName = topicName;
        this.messagesPerRequest = messagesPerRequest;
    }

    @GetMapping("/hello")
    public String hello() throws InterruptedException {
        latch = new CountDownLatch(messagesPerRequest);
        log.info("### called hello REST ###");
        IntStream.range(0, messagesPerRequest)
                 .forEach(i -> this.template.send(topicName, String.valueOf(i), new PracticalAdvice("A Practical Advice",i)));
        latch.await(60, TimeUnit.SECONDS);
        log.info("All messages received");
        return "Hello Kafka";
    }

    @GetMapping("/offset/groupId/{groupId}")
    public Map<String, Long> getCurrentOffset(@PathVariable String groupId) throws InterruptedException, ExecutionException {
        log.info("### called getCurrentOffset REST ###");
        Map<String, Long> offsetDetails = kafkaUtil.getOffsetDetails(groupId);
        return offsetDetails;
    }
}
