package com.ibm.test;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;

import javax.websocket.*;
import javax.websocket.server.*;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.messagehub.samples.ConsumerRunnable;
import com.messagehub.samples.env.MessageHubCredentials;
import com.messagehub.samples.env.MessageHubEnvironment;
import com.messagehub.samples.ConsumerRunnable;

@ServerEndpoint("/testsrv")
public class MHSubsEndPoint {
	//queue holds the list of connected clients
	    private static Queue<Session> queue = new ConcurrentLinkedQueue<Session>();
	    private static Thread rateThread ; //rate publisher thread
	    private static final long HOUR_IN_MILLISECONDS = 3600000L;
	    private static final Logger logger = Logger.getLogger(MHSubsEndPoint.class);
	    private static String userDir, resourceDir;
	    private static boolean isDistribution;
        private Thread consumerThread, producerThread;
        private String topic = "facilities";
        private String kafkaHost = null;
        private String apiKey = null;
        private String username,password;
	    private static String kafkaHostUS = "kafka01-prod01.messagehub.services.us-south.bluemix.net:9093,"
	            + "kafka02-prod01.messagehub.services.us-south.bluemix.net:9093,"
	            + "kafka03-prod01.messagehub.services.us-south.bluemix.net:9093,"
	            + "kafka04-prod01.messagehub.services.us-south.bluemix.net:9093,"
	            + "kafka05-prod01.messagehub.services.us-south.bluemix.net:9093";

	    private static String kafkaHostEU="kafka01-prod02.messagehub.services.eu-gb.bluemix.net:9093,"
	            +"kafka02-prod02.messagehub.services.eu-gb.bluemix.net:9093,"
	            +"kafka03-prod02.messagehub.services.eu-gb.bluemix.net:9093,"
	            +"kafka04-prod02.messagehub.services.eu-gb.bluemix.net:9093,"
	            +"kafka05-prod02.messagehub.services.eu-gb.bluemix.net:9093";    


	 @OnMessage
	 public void onMessage(Session session, String msg){

	   //provided for completeness, in out scenario clients don't send any msg.
		  try {   
		       System.out.println("received msg "+msg+" from "+session.getId());
		  } catch (Exception e) {
		 	   e.printStackTrace();
		  }
	 }

	@OnOpen
	 public void open(Session session) {
			    System.out.println("Open Session Called");
		        queue.add(session);
                System.setProperty("java.security.auth.login.config", "");
                Properties props=getClientConfiguration("MHPROPS");
	            kafkaHost=props.getProperty("bootstrap.servers");
	            topic = props.getProperty("topic");
	            apiKey=props.getProperty("apiKey");
	            System.out.println("*************************************");
	            System.out.println("kafkaHost:: " +kafkaHost);
	            System.out.println("topic:: " +topic);
	            System.out.println("apiKey:: " +apiKey);
	            System.out.println("Calling createMessageConsumer");
               //updateJaasConfiguration(username,password);
                consumerThread = createMessageConsumer(props,topic,session);
                System.out.println("Return from createMessageConsumer");
            if (consumerThread != null) {
                consumerThread.start();
            } else {
                logger.log(Level.ERROR, "Consumer thread is null. Make sure all provided details are valid.");
            }
	       
            System.out.println("New session opened: "+session.getId());
	 }

	@OnError
	 public void error(Session session, Throwable t) {
	   queue.remove(session);
	   System.err.println("Error on session "+session.getId());  
	 }

	 @OnClose
	 public void closedConnection(Session session) { 
	  queue.remove(session);
	  System.out.println("session closed: "+session.getId());
	 }
	 

	    public static void removeClosesSession( ArrayList<Session > closedSessions){
	    	 queue.removeAll(closedSessions);
	    }
	 
	    public final Properties getClientConfiguration(String propertyName) {
	    	String RESTUrl="https://dafc8da0-0306-4ac4-bff1-30d182da8dda-bluemix.cloudant.com/messagehubprops/_all_docs?include_docs=true&conflicts=true";
            System.out.println("propertyName:: " +propertyName);
            if(propertyName.equalsIgnoreCase("MHPROPS")){
            	System.out.println("Returning client configuration for MHPROPS");
            	RESTUrl="https://dafc8da0-0306-4ac4-bff1-30d182da8dda-bluemix.cloudant.com/messagehubprops/_all_docs?include_docs=true&conflicts=true";
            }else{
            	System.out.println("Invalid Property Description----------");
            	System.exit(0);
            }
	    	
	    	Properties properties = new Properties();
	        String userpass = "dafc8da0-0306-4ac4-bff1-30d182da8dda-bluemix:77d29546fc84dc809931977e071df9f1e0b32ef651e54caa832a6289f5e950d3";
	        String basicAuth = "Basic " + new String((Base64.getEncoder()).encode(userpass.getBytes()));
            try{
    	    URL url = new URL(RESTUrl);
	        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestProperty ("Authorization", basicAuth);
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Accept", "application/json");

			if (conn.getResponseCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : "
						+ conn.getResponseCode());
			}

			BufferedReader br = new BufferedReader(new InputStreamReader(
				(conn.getInputStream())));

			String output;
			StringBuffer sbf=new StringBuffer();
			System.out.println("Output from Server .... \n");
			while ((output = br.readLine()) != null) {
				sbf.append(output);
				//System.out.println(output);
			}
			String json=sbf.toString();
			JSONObject obj = new JSONObject(json);
			JSONObject jsonObj=obj.getJSONArray("rows").getJSONObject(0);
			System.out.println("jsonObject:: " +jsonObj);

			JSONObject jsonObj1=jsonObj.getJSONObject("doc");
			System.out.println("jsonObj1:: " +jsonObj1);
			HashMap map=new HashMap();
			 Iterator<String> keysItr = jsonObj1.keys();
			    while(keysItr.hasNext()) {
			        String key = keysItr.next();
			        if(key.equalsIgnoreCase("_id")||key.equalsIgnoreCase("_rev"))continue;
			        Object value = jsonObj1.get(key);
			        map.put(key,value);
			    }
			    properties.putAll(map);	    	
            }catch(Exception t){t.printStackTrace();}
	        return properties;
	    }
	    
	    public Thread createMessageConsumer(Properties props,String topic,Session session) {
	    	System.out.println("Inside message consumer");
	        ConsumerRunnable consumerRunnable = new ConsumerRunnable(props,topic,session);
	        return new Thread(consumerRunnable);
	    }
}
