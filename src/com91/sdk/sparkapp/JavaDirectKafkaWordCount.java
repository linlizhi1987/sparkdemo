package com91.sdk.sparkapp;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;
import java.util.regex.Pattern;

import scala.Tuple2;
import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.Durations;

import com.google.common.base.Optional;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount. Usage:
 * JavaDirectKafkaWordCount <brokers> <topics> <brokers> is a list of one or
 * more Kafka brokers <topics> is a list of one or more kafka topics to consume
 * from
 * 
 * Example: $ bin/run-example streaming.JavaDirectKafkaWordCount
 * broker1-host:port,broker2-host:port topic1,topic2
 */

public final class JavaDirectKafkaWordCount {
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) {
		if (args.length < 2) {
			System.err
					.println("Usage: JavaDirectKafkaWordCount <brokers> <topics>\n"
							+ "  <brokers> is a list of one or more Kafka brokers\n"
							+ "  <topics> is a list of one or more kafka topics to consume from\n\n");
			System.exit(1);
		}

		String brokers = args[0];
		String topics = args[1];

		// Create context with a 2 seconds batch interval
		SparkConf sparkConf = new SparkConf()
				.setAppName("JavaDirectKafkaWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
				Durations.seconds(2));

		jssc.checkpoint(".");
		HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics
				.split(",")));
		HashMap<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", brokers);

		// Create direct kafka stream with brokers and topics
		JavaPairInputDStream<String, String> messages = KafkaUtils
				.createDirectStream(jssc, String.class, String.class,
						StringDecoder.class, StringDecoder.class, kafkaParams,
						topicsSet);
		

		// Get the lines, split them into words, count the words and print
		JavaDStream<String> lines = messages
				.map(new Function<Tuple2<String, String>, String>() {
					@Override
					public String call(Tuple2<String, String> tuple2) {
						return tuple2._2();
					}
				});
		JavaDStream<String> words = lines
				.flatMap(new FlatMapFunction<String, String>() {
					@Override
					public Iterable<String> call(String x) {
						return Arrays.asList(SPACE.split(x));
					}
				});
		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
				new PairFunction<String, String, Integer>() {
					@Override
					public Tuple2<String, Integer> call(String s) {
						return new Tuple2<String, Integer>(s, 1);
					}
				}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> mappingFunc = new Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> call(String word,
					Optional<Integer> one, State<Integer> state) {
				int sum = one.or(0) + (state.exists() ? state.get() : 0);
				Tuple2<String, Integer> output = new Tuple2<>(word, sum);
				state.update(sum);
				return output;
			}
		};

		JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> stateDstream = wordCounts
				.mapWithState(StateSpec.function(mappingFunc));

		stateDstream.print();

		// Start the computation
		jssc.start();
		jssc.awaitTermination();
	}
}