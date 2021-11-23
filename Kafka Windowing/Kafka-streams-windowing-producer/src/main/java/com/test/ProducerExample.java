package com.test;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Producer;
import java.io.IOException;
import java.util.Properties;


public class ProducerExample {


  public static void main(final String[] args) throws IOException {

		  //Config Parameters
		  Properties props = new Properties();
		  props.put("bootstrap.servers", "localhost:29092");
		  props.put("acks", "all");
		  props.put("retries", 0);
		  props.put("compression.type", "snappy");
		  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		  
		  //Create a producer instance
		  Producer<String, String> producer = new KafkaProducer<String, String>(props);
		
		  int N_SAMPLES=11;
		  
		  //Tumbling window
		  //1 event per window
		  //int[] timeKey=new int[]{0,5100,5100,5100,5100,5100,5100,5100,5100,5100,5100};
		  //2 events per window
		  //int[] timeKey=new int[]{0,2500,2400,2500,2400,2500,2400,2500,2400,2500,2400};
		  //Variable pattern
		  //int[] timeKey=new int[]{0,2500,2400,1000,1000,2500,2400,2500,2400,2500,2400};

		  
		  //Session window
		  int[] timeKey=new int[]{0,100,100,100,7200,500,100,100,100,7200,100};
		  
		  String topicName="windowStreamTopic4";
		  for (int i = 0; i < N_SAMPLES; i++) {
			    	
		  String padding ="";
		  if (i<10) padding ="00";
		  else if (i<100) padding ="0";
		  
		  String key="key2";
		  String value="Msg n. " + padding + i;
				  
		  
		  try {
			Thread.sleep(timeKey[i]);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		  
		  ProducerRecord<String, String> sendingRecord = new ProducerRecord<String, String>(topicName, key, value);
		  producer.send(sendingRecord);
		  System.out.printf("Produced record: Key: %s\tValue: %s after %d ms %n","key1", value, timeKey[i]);

		  /*
		  if (i%2==0) {
			  sendingRecord = new ProducerRecord<String, String>(topicName, "key2", value);
			  producer.send(sendingRecord);		
			  System.out.printf("Produced record: Key: %s\tValue: %s after %d ms %n", "key2", value, timeKey[i]);
		  }*/
		  
		  
		  
	    }
	
	    producer.flush();
	
	    System.out.printf(N_SAMPLES + " messages were produced to topic");
	
	    producer.close();
	  }

}
