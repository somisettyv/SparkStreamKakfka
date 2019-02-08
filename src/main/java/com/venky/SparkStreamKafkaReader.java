package com.venky;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import scala.Tuple2;

public class SparkStreamKafkaReader {

	public static void main(String[] str) throws InterruptedException {

		AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();

		//cc_payments
		Map<String, Object> kafkaParams = new HashMap<String, Object>();
		//kafkaParams.put("bootstrap.servers", "sandbox-hdp.hortonworks.com:6667");
		kafkaParams.put("bootstrap.servers", "127.0.0.1:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);
		
		 // Set this property, if auto commit should happen.
        kafkaParams.put("enable.auto.commit", "true");
        // Auto commit interval, kafka would commit offset at this interval.
        kafkaParams.put("auto.commit.interval.ms", "101");
        // This is how to control number of records being read in each poll
        kafkaParams.put("max.partition.fetch.bytes", "135");
        // Set this if you want to always read from beginning.
        // kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("heartbeat.interval.ms", "3000");

		Collection<String> topics = Arrays.asList("demo");

		SparkConf sparkConf = new SparkConf().setAppName("CVeky")
				.setMaster("local");

		JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(streamingContext,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

		/*
		 * 
		 * JavaDStream<String> stream2 = stream.transform( // Make sure you can get
		 * offset ranges from the rdd new Function<JavaRDD<ConsumerRecord<String,
		 * String>>, JavaRDD<ConsumerRecord<String, String>>>() {
		 * 
		 * @Override public JavaRDD<ConsumerRecord<String, String>> call(
		 * JavaRDD<ConsumerRecord<String, String>> rdd ) { OffsetRange[] offsets =
		 * ((HasOffsetRanges) rdd.rdd()).offsetRanges(); offsetRanges.set(offsets);
		 * //Assert.assertEquals("demo", offsets[0].topic()); return rdd; } } ).map( new
		 * Function<ConsumerRecord<String, String>, String>() {
		 * 
		 * @Override public String call(ConsumerRecord<String, String> r) { return
		 * r.value(); } } );
		 */

		/*
		 * OffsetRange[] offsetRanges = { // topic, partition, inclusive starting
		 * offset, exclusive ending offset OffsetRange.create("test", 0, 0, 100),
		 * OffsetRange.create("test", 1, 0, 100) };
		 * 
		 * JavaRDD<ConsumerRecord<String, String>> rdd = KafkaUtils.createRDD(
		 * sparkContext, kafkaParams, offsetRanges,
		 * LocationStrategies.PreferConsistent() );
		 */

		JavaPairDStream<Object, Object> values = stream
				.mapToPair(record -> new Tuple2<Object, Object>(record.key(), record.value()));

		// stream.print();

		stream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
			@Override
			public void call(JavaRDD<ConsumerRecord<String, String>> rdd) {
				final OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
				System.out.println("==========rdd============");
				rdd.foreachPartition(new VoidFunction<Iterator<ConsumerRecord<String, String>>>() {
					@Override
					public void call(Iterator<ConsumerRecord<String, String>> consumerRecords) {
						OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
						System.out.println(
								o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());

						consumerRecords.forEachRemaining(e -> System.out.println("   ----   " + e.value()));
					}
				});
			}
		});

		/*
		 * stream.foreachRDD(r -> { System.out.println("========================");
		 * System.out.println(r); VoidFunction<Iterator<ConsumerRecord<String, String>>>
		 * f; r.foreachPartition(new VoidFunction<Iterator<ConsumerRecord<String,
		 * String>>>(){
		 * 
		 * @Override public void call(Iterator<ConsumerRecord<String, String>> t) throws
		 * Exception { // TODO Auto-generated method stub
		 * t.forEachRemaining(e->System.out.println("   ----   " + e.value()));
		 * 
		 * }
		 * 
		 * }); });
		 */

		// Execute the Spark workflow defined above
		streamingContext.start();
		streamingContext.awaitTermination();

	}
}