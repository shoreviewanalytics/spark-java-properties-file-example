package com.java.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import java.util.Arrays;
import java.util.Properties;

import org.apache.spark.SparkConf;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;

import scala.Tuple2;
import java.util.regex.Pattern;

import java.io.FileInputStream;


public class SparkJavaPropertiesFileExample {
	
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkJavaPropertiesFileExample.class);
	
	public static void main(String[] args) {
			 
		Pattern SPACE = Pattern.compile(" ");
		
		/*
		 * run the job within a basic try / catch block, so issues can be discovered and written to the log 
		 */
		try {  
			
			Properties appProps = new Properties();
			/*
			 * set the directory for the .properties file.   
			*/
			String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
			/*
			 * set the complete path to the .properties file.  
			 */
			
			String appConfigPath = rootPath + "config/spark-properties-file-example.properties";
			
			appProps.load(new FileInputStream(appConfigPath));	
			/*
			 * get the job name from the .properties file for the job
			 */
		
			String jobName = appProps.getProperty("app3");
			/*
			 * get the path to the logback.xml file
			 */
			
			String logbackfile = appProps.getProperty("logbackfile");
			
			/*
			 * set the name of the job to be used in logback.xml
			 */
			System.setProperty("jobname",jobName);
			
			/*
			 * set the logger context so you can use job specific logging throughout job processing
			 */
			LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
		    
		    try {
		      JoranConfigurator configurator = new JoranConfigurator();
		      configurator.setContext(context);
		      context.reset(); 
		      configurator.doConfigure(logbackfile);
		    } catch (JoranException je) {
		      // StatusPrinter below will handle 
		    }
		    /*
		     * this line catches any issues from the above try catch
		     */
		    StatusPrinter.printInCaseOfErrorsOrWarnings(context);
		    
		    
		    LOGGER.info("The root path for the job is " + rootPath);
		    LOGGER.info("The full path to the spark-properties-file-example.properties file is " + appConfigPath);
		    LOGGER.info("Logback file location. " + logbackfile);
		    LOGGER.info("Job name is " + jobName);
		     
		   			
			SparkConf conf = new SparkConf().setAppName("LoggerSample");
			JavaSparkContext sc = new JavaSparkContext(conf);
			SparkSession spark = SparkSession.builder().appName("LoggerSample").getOrCreate();
			SysStreamsLogger.bindSystemStreams();

			LOGGER.info("Sample Info Logging: Starting up the job.");

			/**
			 * to test against a distributed environment place a file named words.csv on each node.
			 * 
			 */
			JavaRDD<String> lines = spark.read().textFile("file:///home/one/data/words.csv").javaRDD();

			JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

			LOGGER.info("Sample Info Logging of transformation of words after lines.flatMap.");

			words.take(5).forEach(System.out::println);

			JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

			LOGGER.info("Sample Info Logging of transformation of ones RDD after being set to words.mapToPair.");

			ones.take(5).forEach(System.out::println);

			JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

			LOGGER.info("Sample Info Logging of the RDD - counts.toDebugString() " + counts.toDebugString());

			// sample info logging of transformation using SysStreamsLogger
			LOGGER.info("Sample Info Logging: Print out the first five key / value pairs of counts RDD");

			counts.take(5).forEach(System.out::println);			 

			sc.close();

						
		}catch(Exception e){
			
			LOGGER.error("An Error has occurred: Exiting the job. " + e.getMessage());
		}
				
		
	}

}
