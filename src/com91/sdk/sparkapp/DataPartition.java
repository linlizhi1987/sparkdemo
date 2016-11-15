package com91.sdk.sparkapp;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class DataPartition {

	private static final Pattern SPACE = Pattern.compile(" ");
	
	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.err.println("Usage: JavaWordCount <infile>");
			System.exit(1);
		}

		SparkConf sparkConf = new SparkConf().setAppName("DataPartition");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = ctx.textFile(args[0], 1);

		JavaPairRDD<String, String> ones = lines
				.mapToPair(new PairFunction<String, String, String>() {
					@Override
					public Tuple2<String, String> call(String s) {
						String[] arrline = SPACE.split(s);
						return new Tuple2<String, String>(arrline[0],
								arrline[1]);
					}
				});

		final List<String> arrList = ones.keys().distinct().collect();

		JavaPairRDD<String, String> twos=ones.partitionBy(new Partitioner() {

			@Override
			public int numPartitions() {
				// TODO Auto-generated method stub
				return arrList.size();
			}

			@Override
			public int getPartition(Object arg0) {
				// TODO Auto-generated method stub
				return arrList.indexOf(arg0);
			}
		});
		
		twos.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<String,String>>, Iterator<String>>() {

			@Override
			public Iterator<String> call(Integer arg0,
					Iterator<Tuple2<String, String>> arg1) throws Exception {
				// TODO Auto-generated method stub
				/*Configuration conf = new Configuration();
		        FileSystem fs = FileSystem.get(conf);          
		        FSDataOutputStream out = fs.append(new Path(""));*/
				
				String pathString=arrList.get(arg0);
				java.io.FileWriter fw=new java.io.FileWriter(pathString);
				while(arg1.hasNext())
				{
					fw.write(arg1.next()._2()+"\n");
				}
				fw.close();
					
				
				return new ArrayList<String>().iterator();
			}
		}, true).collect();
		ctx.stop();
	}
}
