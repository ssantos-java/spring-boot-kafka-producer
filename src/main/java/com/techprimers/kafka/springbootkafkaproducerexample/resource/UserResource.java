package com.techprimers.kafka.springbootkafkaproducerexample.resource;

import com.techprimers.kafka.springbootkafkaproducerexample.model.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("kafka")
public class UserResource {

    @Autowired
    private KafkaTemplate<String, User> kafkaTemplate;

    private static final String TOPIC = "Kafka_Example";
    String ret="";

    @GetMapping("/publish/{name}")
    public String post(@PathVariable("name") final String name) {

	  //  kafkaTemplate.send(TOPIC, new User(name, "Technology", 12000L));
        
        ListenableFuture<SendResult<String, User>> future = 
            kafkaTemplate.send(TOPIC, new User(name, "Technology", 12000L));
        future.addCallback(new ListenableFutureCallback<SendResult<String, User>>() {
            @Override
            public void onSuccess(SendResult<String, User> result) {
                ret = "Published successfully  :"+result;
            }

            @Override
            public void onFailure(Throwable ex) {
                ret = "failed :"+ex.getMessage();
            }
        });
        
        return ret;
    }
}
