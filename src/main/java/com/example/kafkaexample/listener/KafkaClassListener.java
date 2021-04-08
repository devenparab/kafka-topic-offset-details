package com.example.kafkaexample.listener;

import com.example.kafkaexample.model.PracticalAdvice;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;
import java.util.stream.StreamSupport;

@Component
@Slf4j
public class KafkaClassListener {

    @Value("${tpd.messages-per-request}")
    private int messagePerRequest;

    private CountDownLatch latch = new CountDownLatch(messagePerRequest);

    @KafkaListener(topics = "advice-topic", clientIdPrefix = "json", containerFactory = "kafkaListenerContainerFactory")
    public void listenAsObject(ConsumerRecord<String, PracticalAdvice> cr, @Payload PracticalAdvice payload) {
        log.info("Logger [JSON] received key [{}]: Type [{}] | Payload: [{}] | Record: [{}] | Offset: [{}]", cr.key(), typeIdHeader(cr.headers()), payload, cr.toString(), cr.offset());
        latch.countDown();
    }

    private static String typeIdHeader(Headers headers) {
        return StreamSupport.stream(headers.spliterator(), false)
                .filter(header -> header.key().equals("__TypeId__"))
                .findFirst().map(header -> new String(header.value())).orElse("N/A");
    }

}
