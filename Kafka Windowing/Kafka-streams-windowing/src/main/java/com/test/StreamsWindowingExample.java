package com.test;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;

import org.apache.kafka.streams.kstream.Windowed;

import java.time.Duration;
import java.util.Properties;

//Michele Stecca

//Original source code taken from Confluent's official documentation

public class StreamsWindowingExample {

    private static Properties streamsConfiguration = new Properties();
	
    public static void setUp() {
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }
    
    
    public static void simpleTumblingWindow() throws InterruptedException {
        
    	String wordCountTopic = "windowStreamTopic1";

        
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream(wordCountTopic,
          Consumed.with(Serdes.String(), Serdes.String()));
        
        KStream<String, String> result = textLines
          .groupByKey()
          .windowedBy(TimeWindows.of(Duration.ofSeconds(5)).grace(Duration.ZERO))
          .count()
          .suppress(Suppressed.untilWindowCloses(unbounded()))//see only "final" result for each window
          .toStream()
          .map((Windowed<String> key, Long count) -> new KeyValue<>(key.key(), count.toString()));
        
        
        /* Additional debug info about window size
        KStream<String, String> result = textLines
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofSeconds(5)).grace(Duration.ZERO))
                .count()
                .suppress(Suppressed.untilWindowCloses(unbounded()))
                .toStream()
                .map((windowedKey, count) ->  { 
		            String start = windowedKey.window().startTime().toString();
		            String end = windowedKey.window().endTime().toString();
		            String windowInfo = String.format("Window: started: %s ended: %s with count %s", start, end, count);
		            return KeyValue.pair(windowedKey.key(), windowInfo);
        });*/
        
        result.foreach((word, count) -> System.out.println("Word: " + word + " -> " + count));
 
        result.to("windowOutputTopic1",
                Produced.with(Serdes.String(), Serdes.String()));
        
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "simple-window");
        
        final Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);

        streams.cleanUp();
        streams.start();

        Thread.sleep(300000);
        streams.close();

    }

    public static void simpleHoppingWindow() throws InterruptedException {
        
    	String wordCountTopic = "windowStreamTopic2";

        
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream(wordCountTopic,
          Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> result = textLines
          .groupByKey()
          .windowedBy(TimeWindows.of(Duration.ofSeconds(5)).advanceBy(Duration.ofSeconds(1)).grace(Duration.ZERO))
          .count()
          .toStream()
          .map((Windowed<String> key, Long count) -> new KeyValue<>(key.key(), count.toString()));

        result.foreach((word, count) -> System.out.println("Word: " + word + " -> " + count));
 
        result.to("windowOutputTopic2",
                Produced.with(Serdes.String(), Serdes.String()));
        
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "simple-hopping-window");
        
        final Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);

        streams.cleanUp();
        streams.start();

        Thread.sleep(300000);
        streams.close();

    }
    
    public static void simpleSlidingWindow() throws InterruptedException {
        
    	String wordCountTopic = "windowStreamTopic3";

        
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream(wordCountTopic,
          Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> result = textLines
          .groupByKey()
          .windowedBy(SlidingWindows.withTimeDifferenceAndGrace(Duration.ofSeconds(5), Duration.ofSeconds(1)))
          .count()
          .suppress(Suppressed.untilWindowCloses(unbounded()))
          .toStream()
          .map((Windowed<String> key, Long count) -> new KeyValue<>(key.key(), count.toString()));

        result.foreach((word, count) -> System.out.println("Word: " + word + " -> " + count));
 
        result.to("windowOutputTopic3",
                Produced.with(Serdes.String(), Serdes.String()));
        
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "simple-sliding-window2");
        
        final Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);

        streams.cleanUp();
        streams.start();

        Thread.sleep(300000);
        streams.close();

    }
    
    
    public static void simpleSessionWindow() throws InterruptedException {
        
    	String wordCountTopic = "windowStreamTopic4";

        
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream(wordCountTopic,
          Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> result = textLines
          .groupByKey()
          .windowedBy(SessionWindows.with(Duration.ofSeconds(7)).grace(Duration.ZERO))
          .count()
          .suppress(Suppressed.untilWindowCloses(unbounded()))
          .toStream()
          .map((Windowed<String> key, Long count) -> new KeyValue<>(key.key(), count.toString()));
          
          /*
          .map((windowedKey, count) ->  { 
              String start = windowedKey.window().startTime().toString();
              String end = windowedKey.window().endTime().toString();
              String sessionInfo = String.format("Session info: started: %s ended: %s with count %s", start, end, count);
              return KeyValue.pair(windowedKey.key(), sessionInfo);
          });*/

        result.foreach((word, count) -> System.out.println("Word: " + word + " -> " + count));
 
        result.to("windowOutputTopic4",
                Produced.with(Serdes.String(), Serdes.String()));
        
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "simple-session-window");
        
        final Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);

        streams.cleanUp();
        streams.start();

        Thread.sleep(300000);
        streams.close();

    }
     
    
	  public static void main(final String[] args) throws InterruptedException {

		  setUp();
		  simpleTumblingWindow();
		  //simpleHoppingWindow();
		  //simpleSlidingWindow();
		  //simpleSessionWindow();
	  }
	
}
