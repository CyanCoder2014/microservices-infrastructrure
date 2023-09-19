package com.cyancoder.demo.twitter.to.kafka.service.runner.impl;

import com.cyancoder.demo.twitter.to.kafka.service.TwitterToKafkaService;
import com.cyancoder.demo.twitter.to.kafka.service.config.TwitterToKafkaServiceConfigData;
import com.cyancoder.demo.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import com.cyancoder.demo.twitter.to.kafka.service.runner.StreamRunner;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import java.util.Arrays;

@RequiredArgsConstructor
@Component
public class TwitterKafkaStreamRunner implements StreamRunner {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterToKafkaService.class);


    private final TwitterKafkaStatusListener twitterKafkaStatusListener;
    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;

    private TwitterStream twitterStream;

    @Override
    public void start() throws TwitterException {

        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(twitterKafkaStatusListener);
        addFilter();
    }

    @PreDestroy
    public void shutdown(){
        if (twitterStream!=null){
            LOG.info("shutdown stream");
            twitterStream.shutdown();
        }
    }


    private void addFilter() {
        String[] keywords = twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0]);
        FilterQuery filterQuery = new FilterQuery(keywords);
        twitterStream.filter(filterQuery);
        LOG.info("keywords {}", Arrays.toString(keywords));
    }


}
