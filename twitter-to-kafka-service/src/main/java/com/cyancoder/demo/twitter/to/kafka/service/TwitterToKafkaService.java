package com.cyancoder.demo.twitter.to.kafka.service;

import com.cyancoder.demo.twitter.to.kafka.service.config.TwitterToKafkaServiceConfigData;
import com.cyancoder.demo.twitter.to.kafka.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class TwitterToKafkaService implements CommandLineRunner {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterToKafkaService.class);
    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;
    private final StreamRunner streamRunner;
    //    private final StreamInitializer streamInitializer;

    public TwitterToKafkaService(TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData, StreamRunner streamRunner) {
        this.twitterToKafkaServiceConfigData = twitterToKafkaServiceConfigData;
        this.streamRunner = streamRunner;
    }

    public static void main(String[] args) {
        SpringApplication.run(TwitterToKafkaService.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        LOG.info("App starts...");
//        streamInitializer.init();
        streamRunner.start();
    }
}