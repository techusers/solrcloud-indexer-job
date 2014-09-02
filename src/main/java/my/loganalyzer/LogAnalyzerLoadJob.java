package my.loganalyzer;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import my.loganalyzer.dto.LogMessageWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class LogAnalyzerLoadJob extends Configured implements Tool {

	private static final Logger LOG = LoggerFactory.getLogger(LogAnalyzerLoadJob.class);
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new LogAnalyzerLoadJob(), args);
	}
	
	public int run(String args[]) throws Exception {
		
		Job job;
		
		try {
			Configuration conf = getConf();
			
			if (conf == null)
				conf = new Configuration();

			int numReducerTasks = 2;//default
			
			try {
				numReducerTasks= Integer.parseInt(args[2]);
			} catch(Exception e) {
				LOG.error("Exception occurred getting reducers "+e.getMessage());
				e.printStackTrace();
			}
			
			conf.setInt("mapred.reduce.tasks", numReducerTasks);
			conf.set("dfs.replication", "2");
			conf.set("mapred.map.tasks.speculative.execution", "false");		
			conf.set("mapred.reduce.tasks.speculative.execution", "false");
			
			job = new  Job(conf, "LogAnalyzerLoadJob");
			
			job.setJarByClass(LogAnalyzerLoadJob.class);
			job.setMapperClass(LogLoadMapper.class);
			job.setReducerClass(LogLoadReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(LogMessageWritable.class);

			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			
			job.setInputFormatClass(TextInputFormat.class);

			FileInputFormat.setInputPaths(job, args[0]);
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			job.waitForCompletion(true);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return 0;
	}
	
	/**
	 * Mapper to read log file and bundle each line the log into a LogMessageWritable.
	 * 
	 * @author techusers
	 *
	 */
	public static class LogLoadMapper extends Mapper<LongWritable, Text, Text, LogMessageWritable> {
		
		@Override
		public void map(LongWritable key, Text line, Context context) 
				throws IOException, InterruptedException {
			
			LogMessageWritable logmessage = parseLog(line);
			
			if(logmessage != null) {
				context.write(logmessage.getUser(), logmessage);
			}
		}
		
		private LogMessageWritable parseLog(Text line) {
			LogMessageWritable log = null;
			
			if((line.toString() != null) && (!line.toString().isEmpty())) {
				String tokens[] = line.toString().split(",");
				
				if(tokens.length == 6) {
					log = new LogMessageWritable();
					log.setTimestamp(new Text(tokens[0]));
					log.setLogLevel(new Text(tokens[1]));
					log.setUser(new Text(tokens[2]));
					log.setTid(new Text(tokens[3]));
					log.setAction(new Text(tokens[4]));
					log.setStatus(tokens[5]);
				}
			}
			
			return log;
		}
	}
	
	/**
	 * Reducer to add documents to solr in batch.
	 * This reducer does not output anything eventhough a text output is defined.
	 * If required document counts can be output. This is left out intentionally.
	 * A batchSize of 1000 is used. Choose a batch size appropriately and after testing its effects.
	 * @author techusers
	 *
	 */
	public static class LogLoadReducer extends Reducer<Text, LogMessageWritable, NullWritable, Text> {
		
		private CloudSolrServer solrServer = null;
		private SimpleDateFormat sdf = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss");
		private int docBatchSize = 1000;
		
		@Override
		protected void setup(Context context)
			throws IOException, InterruptedException
		{ 
			super.setup(context);
			
			try {
				//instantiate ClourSolrServer. arguments to the constructor are the zookeeper node names and port
				solrServer = new CloudSolrServer("myzknode1:2181,myzknode2:2181,myzknode3:2181/solr");
				solrServer.setDefaultCollection("log_collection");
			} catch(Exception e) {
				LOG.error("Exception occurred creating solrserver instance ",e);
			}
		}
		
		@Override
		public void reduce(Text key, Iterable<LogMessageWritable> values, Context context) throws IOException, InterruptedException {
			
			List<SolrInputDocument> doclist = new ArrayList<SolrInputDocument>();
			
			for(LogMessageWritable value:values) {
				SolrInputDocument doc = createSolrDocument(value);
				doclist.add(doc);
				
				if(doclist.size() == docBatchSize) {
					addBatchToSolr(doclist);
					doclist.clear();
				}
				
				if(doclist.size() > 0) {
					addBatchToSolr(doclist);//last documents less than batch size if any
				}
			}
			
		}
		
		private void addBatchToSolr (List<SolrInputDocument> list) throws IOException {
			try {
				solrServer.add(list, 10000); //To make the documents immediately available for search
			} catch (SolrServerException se) {
				LOG.error("Error occurred updating documents to Solr", se);
			}
		}
		
		private SolrInputDocument createSolrDocument(LogMessageWritable log) {
					
			SolrInputDocument doc = new SolrInputDocument();
			
			try {
				doc.addField("id", log.getUser().toString() + "-" + log.getTid().toString());
				doc.addField("timestamp_t", log.getTimestamp().toString());//not a good choice for facet queries
				doc.addField("loglevel_t", log.getLogLevel().toString());
				doc.addField("username_t", log.getUser().toString());
				doc.addField("actionType", log.getAction().toString());
				doc.addField("status_t", log.getStatus());
				
				try {
					Date currentdate = sdf.parse(log.getTimestamp().toString());
					doc.addField("logDateTime_dt", currentdate);//not a good choice for facet queries
				} catch(Exception e) {
					LOG.warn("Exception occurred parsing date"+e.getMessage());
				}
				
			} catch(Exception ex) {
				LOG.error("Exception occurred creating document", ex);
				doc = null;
			}
			
			return doc;
			
		}
	}
	
}