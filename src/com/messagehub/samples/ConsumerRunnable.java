/**
 * Copyright 2015 IBM
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
/**
 * Licensed Materials - Property of IBM
 * (c) Copyright IBM Corp. 2015
 */
package com.messagehub.samples;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

import javax.websocket.*;
import javax.websocket.server.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.ibm.test.MHSubsEndPoint;

public class ConsumerRunnable implements Runnable {
    private static final Logger logger = Logger.getLogger(ConsumerRunnable.class);
    private KafkaConsumer<byte[], byte[]> kafkaConsumer;
    private ArrayList<String> topicList;
    private boolean closing;
    private Session session;
    private  ArrayList<Session > closedSessions= new ArrayList<>();
    public ConsumerRunnable(Properties props,String topic,Session session) {
    	closing = false;
        topicList = new ArrayList<String>();
        this.session=session;
        System.out.println("Inside consumer runnable");
        // Provide configuration and deserialisers
        // for the key and value fields received.
        kafkaConsumer = new KafkaConsumer<byte[], byte[]>(props,new ByteArrayDeserializer(), new ByteArrayDeserializer());
        System.out.println("+++++++++++++++++++++++++Consumer Subscribing to topic:: " +topic);
        topicList.add(topic);
        kafkaConsumer.subscribe(topicList);
    }

    @Override
    public void run() {
        logger.log(Level.INFO, ConsumerRunnable.class.toString() + " is starting.");
        System.out.println("$$$$$$$$$$$$$$$$$$$$$Message Consumer called $$$$$$$$$$$$$$$$$$$$$$$$");
        
        while (!closing) {
        	System.out.println("Inside While to read message from Kafka");
            try {
                // Poll on the Kafka consumer every second.
                Iterator<ConsumerRecord<byte[], byte[]>> it = kafkaConsumer
                        .poll(1000).iterator();

                // Iterate through all the messages received and print their
                // content.
                // After a predefined number of messages has been received, the
                // client
                // will exit.
                while (it.hasNext()) {
                    ConsumerRecord<byte[], byte[]> record = it.next();
                    final String message = new String(record.value(),
                            Charset.forName("UTF-8"));
       		        if(!session.isOpen())
    		        {
    		           System.err.println("Closed session: "+session.getId());
    		           closedSessions.add(session);
    		        }else{
    		           System.out.println("Sending Message to Web Sockets Client");
    	               session.getBasicRemote().sendText(message);
    		        }  
       		        MHSubsEndPoint.removeClosesSession(closedSessions);
                    System.out.println("Message:: " +message);
                    if(message.equalsIgnoreCase("EOF")){
                    	closing=true;
                    	break;
                    }
                    logger.log(Level.INFO, "Message: " + message);
                }

                kafkaConsumer.commitSync();

                Thread.sleep(1000);
            } catch (final InterruptedException e) {
                logger.log(Level.ERROR, "Producer/Consumer loop has been unexpectedly interrupted");
                shutdown();
            } catch (final Exception e) {
                logger.log(Level.ERROR, "Consumer has failed with exception: " + e);
                shutdown();
            }
        }
        System.out.println("Outside while of Consumer. All data has bee consumed");
        kafkaConsumer.close();
    }

    public void shutdown() {
    	System.out.println("Consume shutdown called");
        closing = true;
    }
}
