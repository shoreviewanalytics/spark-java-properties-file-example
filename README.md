
This example uses a .properties file with a spark job. It can be run on a single node DataStax Enterprise (DSE) cluster with Analytics enabled or a multi-node (DSE) cluster with Analytics enabled.   

A properties file allows you to pass various configuration values to the job at runtime.  Similar to spark-logback-example-two job, this job doesn't require the use of --driver-java-options "=Dlogback.configurationFile=" option with spark-submit as found in spark-logback-example-one.  The reference to a specific logback.xml file can be shared between jobs or you could create custom logback.xml file for each job using a name like application_name.logback.xml.  The logback.xml file is configured with a parameter ${jobname}, which takes a value set within the job using System.setProperty to provide the job name.  The job name is set in the .properties file.   

To run:

dse -u cassandra -p yourpassword spark-submit --class com.java.spark.SparkPropertiesFileExample /pathtojobsfolderonyourserver/spark-properties-file-example-0.1.jar

Configuration and Data Files:

Configuration and data files are located in the src/main/resources folder.  You will need to place these files in the appropriate location based upon your development and server environment.  