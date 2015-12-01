import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import kafka.serializer.StringDecoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

import com.google.gson.Gson;

public class EventComntsStreamProcessor {

	public static void main(String[] args) {

		if (args.length < 2) {
			System.err.println("Usage: EventComntsStreamProcessor <brokers> <topics>\n" +
					"  <brokers> is a list of one or more Kafka brokers\n" +
					"  <topics> is a list of one or more kafka topics to consume from\n\n");
			System.exit(1);
		}

		String brokers = args[0];
		String topics = args[1];

		// Create context with a 2 seconds batch interval
		SparkConf sparkConf = new SparkConf().setAppName("EventComntsStreamProcessor");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

		HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
		HashMap<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", brokers);

		// Create direct kafka stream with brokers and topics
		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
				jssc,
				String.class,
				String.class,
				StringDecoder.class,
				StringDecoder.class,
				kafkaParams,
				topicsSet
				);

		JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
			public String call(Tuple2<String, String> tuple2) {
				return tuple2._2();
			}
		});

		// create connection with HBase
		Configuration config = null;
		try {
			config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.quorum", "127.0.0.1");
			config.set("hbase.zookeeper.property.clientPort","5181");
			//config.set("hbase.master", "127.0.0.1:60000");
			HBaseAdmin.checkHBaseAvailable(config);
			System.out.println("HBase is running!");
		} 
		catch (MasterNotRunningException e) {
			System.out.println("HBase is not running!");
			System.exit(1);
		}catch (Exception ce){ 
			ce.printStackTrace();
		}
		String tablename = "event_comms";
		config.set(TableOutputFormat.OUTPUT_TABLE, tablename);

		// new Hadoop API configuration
		final Job newAPIJobConfiguration1;
		try {
			newAPIJobConfiguration1 = Job.getInstance(config);
			newAPIJobConfiguration1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tablename);
			newAPIJobConfiguration1.getConfiguration().set("mapreduce.output.fileoutputformat.outputdir", "/user/user01/out");
			newAPIJobConfiguration1.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);

			JavaPairDStream<ImmutableBytesWritable, Put> hbasePuts= lines.mapToPair(
					new PairFunction<String, ImmutableBytesWritable, Put>(){

						public Tuple2<ImmutableBytesWritable, Put> call(String line) {

							Gson gson = new Gson();
							EventComments eventDetails = gson.fromJson(line, EventComments.class);

							Float score = getScore(eventDetails);

							Put put = new Put(Bytes.toBytes(eventDetails.getId()));
							try{
								put.addColumn(Bytes.toBytes("comm"), Bytes.toBytes("comm"), Bytes.toBytes(eventDetails.getComment()));
							}catch(Exception e){
								System.err.println("comm => " + eventDetails.getComment());
								System.err.println("comm => " +line);
								e.printStackTrace();
							}
							
							try{
							put.addColumn(Bytes.toBytes("comm"), Bytes.toBytes("score"), Bytes.toBytes(score.toString()));
							}catch(Exception e){
								System.err.println("score => " + eventDetails.getComment());
								System.err.println("score => " +line);
							}
							
							try{
							put.addColumn(Bytes.toBytes("event"), Bytes.toBytes("event_name"), Bytes.toBytes(eventDetails.getEvent_name()));
							}catch(Exception e){
								System.err.println("event_name => " + eventDetails.getComment());
								System.err.println("event_name => " + line);
							}
							
							try{
							put.addColumn(Bytes.toBytes("event"), Bytes.toBytes("event_id"), Bytes.toBytes(eventDetails.getEvent_id()));
							}catch(Exception e){
								System.err.println("event_id => " + eventDetails.getComment());
								System.err.println("event_id => " +line);
							}
							
							try{
							put.addColumn(Bytes.toBytes("member"), Bytes.toBytes("member_id"), Bytes.toBytes(eventDetails.getMember_id()));
							}catch(Exception e){
								System.err.println("member_id => " + eventDetails.getComment());
								System.err.println("member_id => " + line);
							}
							
							try{
							put.addColumn(Bytes.toBytes("member"), Bytes.toBytes("member_name"), Bytes.toBytes(eventDetails.getMember_name()));
							}catch(Exception e){
								System.err.println("member_name => " + eventDetails.getComment());
								System.err.println("member_name => " + line);
							}
							
							try{
							put.addColumn(Bytes.toBytes("group"), Bytes.toBytes("group_city"), Bytes.toBytes(eventDetails.getGroup_city()));
							}catch(Exception e){
								System.err.println("group_city => " + eventDetails.getComment());
								System.err.println("group_city => " +line);
							}
							
							try{
							put.addColumn(Bytes.toBytes("group"), Bytes.toBytes("group_country"), Bytes.toBytes(eventDetails.getGroup_country()));
							}catch(Exception e){
								System.err.println("group_country => " + eventDetails.getComment());
								System.err.println("group_country => " +line);
							}
							
							try{
							put.addColumn(Bytes.toBytes("group"), Bytes.toBytes("group_state"), Bytes.toBytes(eventDetails.getGroup_state()));
							}catch(Exception e){
								System.err.println("group_state => " + eventDetails.getComment());
								System.err.println("group_state => " +line);
							}
							
							try{
							put.addColumn(Bytes.toBytes("group"), Bytes.toBytes("group_id"), Bytes.toBytes(eventDetails.getGroup_id()));
							}catch(Exception e){
								System.err.println("group_id => " + eventDetails.getComment());
								System.err.println("group_id => " +line);
							}
							
							try{
							put.addColumn(Bytes.toBytes("group"), Bytes.toBytes("group_name"), Bytes.toBytes(eventDetails.getGroup_name()));
							}catch(Exception e){
								System.err.println("group_name => " + eventDetails.getComment());
								System.err.println("group_name => " + line);
							}
							
							try{
							put.addColumn(Bytes.toBytes("group"), Bytes.toBytes("group_lon"), Bytes.toBytes(eventDetails.getGroup_lon()));
							}catch(Exception e){
								System.err.println("group_lon => " + eventDetails.getComment());
								System.err.println("group_lon => " +line);
							}
							
							try{
							put.addColumn(Bytes.toBytes("group"), Bytes.toBytes("group_lat"), Bytes.toBytes(eventDetails.getGroup_lat()));
							}catch(Exception e){
								System.err.println("group_lat => " + eventDetails.getComment());
								System.err.println("group_lat => " + line);
							}
							
							try{
							put.addColumn(Bytes.toBytes("group"), Bytes.toBytes("group_cat"), Bytes.toBytes(eventDetails.getGroup_cat()));
							}catch(Exception e){
								System.err.println("group_cat => " + eventDetails.getComment());
								System.err.println("group_cat => " +line);
							}
							
							try{
							put.addColumn(Bytes.toBytes("group"), Bytes.toBytes("group_cat_id"), Bytes.toBytes(eventDetails.getGroup_cat_id()));
							}catch(Exception e){
								System.err.println("group_cat_id => " + eventDetails.getComment());
								System.err.println("group_cat_id => " +line);
							}
							
							return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
						}
						
						private Float getScore(EventComments eventDetails) {
							String content = eventDetails.getComment();
							// Sentiment Analysis
							// remove numbers and special chars
							content.replaceAll("[^a-zA-Z\\s]", "").trim().toLowerCase();

							// exclude stop words
							List<String> stopWords = StopWords.getWords();
							for (String word : stopWords) {
								content = content.replaceAll("\\b" + word + "\\b", "");
							}

							// get words as an array
							String[] words = content.split(" ");
							int numWords = words.length;

							// get number of positive words
							Set<String> posWords = PositiveWords.getWords();
							int numPosWords = 0;
							for (String word : words) {
								if (posWords.contains(word))
									numPosWords++;
							}

							// get number of negative words
							Set<String> negWords = NegativeWords.getWords();
							int numNegWords = 0;
							for (String word : words) {
								if (negWords.contains(word))
									numNegWords++;
							}

							// range: -1 to +1
							Float score = (numPosWords - numNegWords) / ((float) numWords);
							return score;
						}
						
					});


			hbasePuts.foreachRDD(new Function<JavaPairRDD<ImmutableBytesWritable, Put>, Void>() {

				public Void call(JavaPairRDD<ImmutableBytesWritable, Put> hbasePutJavaPairRDD) throws Exception {
					hbasePutJavaPairRDD.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());
					return null;
				}
			});
		} catch (IOException e) {
			e.printStackTrace();
		}
		jssc.start();
		jssc.awaitTermination();

	}
}



