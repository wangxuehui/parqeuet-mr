package org.parquet.main;




import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.parquet.mr.BasketParquetWriterMap;

import parquet.hadoop.example.ExampleInputFormat;


/**
 * 
 * 最后输出的数据为<productid,count>
 * @author love_peace
 *
 */
public class BasketParquetWriterApp extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		
		// 载入相关配置文件
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "BasketParquetWriterApp");

		job.setJarByClass(BasketParquetWriterApp.class);
		job.setMapperClass(BasketParquetWriterMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(ExampleInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job,"/basketparquet/part_20150716103149");
		FileOutputFormat.setOutputPath(job,new Path("/tmp"));

		ControlledJob conCtrl = new ControlledJob(conf);
		conCtrl.setJob(job);
		JobControl jobCtrl = new JobControl("jobControl");
		jobCtrl.addJob(conCtrl);


		// 在线程启动
		Thread t = new Thread(jobCtrl);
		t.start();

		while (true) {
			Thread.sleep(100);
			if (jobCtrl.allFinished()) {
				jobCtrl.stop();
				break;
			}
			if (jobCtrl.getFailedJobList().size() > 0) {
				jobCtrl.stop();
				break;
			}
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		try {
			int res = ToolRunner.run(new BasketParquetWriterApp(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(255);
		}
	}
}
