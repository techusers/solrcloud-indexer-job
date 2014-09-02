solrcloud-indexer-job
=====================
1.
/**
 * Map reduce job to read log files on hdfs and create solr documents for search.
 * Mapper is used to read the log files. Each log message is wrapped into LogMessageWritable.
 * 
 * Reducer receives a list of LogMessageWritable and adds documents to Solr.
 * A reducer is used here to process solr documents in batch.
 * 
 * Note: This code commits and updates live solr index so changes are visible immedialtely.
 * 		 Please check with your requirements regarding commit requirements and preformance.
 * 		 This might cause a bottleneck for bulk indexing.
 * 		 Take a look at soft/hard commits and its effects for bulk indexing.
 * 
 * @author techusers
 *
 */

2. An example log file logfile.snippet.txt, this job uses as input is added under src/main/resources.
3. Example solr schema solr.schema.snippet.txt, this job used is added under src/main/resources.

4. This job can be run on the hadoop cluster using the command line:

	hadoop jar solrcloud-indexer-job-0.0.1-SNAPSHOT.jar  my.loganalyzer.LogAnalyzerLoadJob  /input/logfiles/ /output/logoutput 1

