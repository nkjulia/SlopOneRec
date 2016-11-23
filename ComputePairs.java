package MahourRec;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import MahourRec.Common;
import MahourRec.Util;;
public class ComputePairs extends Configured implements Tool {
	public static class MyMap extends Mapper<LongWritable,Text,Text,Text>{
		@Override
		public void map(LongWritable key,Text value,Context ctx) throws IOException,InterruptedException{
			String[] s = value.toString().split("\t");
			for(int i = 1; i < s.length -1;i ++){
				String[] pair1 = s[i].split(":");
				for(int j = i + 1; j < s.length; j++){
					String[] pair2 = s[j].split(":");
					String k = Util.get_key(Integer.valueOf(pair1[0]), Integer.valueOf(pair2[0])) ;
					float v = Util.get_value(Integer.valueOf(pair1[0]), Integer.valueOf(pair2[0]),Float.parseFloat(pair1[1]),Float.parseFloat(pair2[1]));
					ctx.write(new Text(k),new Text(String.valueOf(v)));
				}
			}
		}
		
	}
	public static class MyReduce extends Reducer<Text,Text,Text,Text>{
		@Override
		public void reduce(Text key,Iterable<Text> values,Context ctx) throws IOException,InterruptedException{
			int cnt = 0;
			float sum = 0;
			for(Text e:values){
				sum += Float.parseFloat(e.toString());
				cnt +=1;
			}
			ctx.write(key, new Text(String.valueOf(cnt) + "\t" + String.valueOf(( sum  + 1 / (cnt + 1 )))));
		}
	}
	@Override
	public int run(String[] arg0) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJarByClass(ComputePairs.class);
		
		job.setMapperClass(MyMap.class);
		job.setReducerClass(MyReduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		System.out.println("Compute Pairs Parameters....................");
		System.out.println(Common.K_MID_DIR + "/" +Common.K_FLATTERN_DIR+"/part-r-00000");
		System.out.println(Common.K_MID_DIR + "/" + Common.K_PAIRS_DIR);
		
		FileInputFormat.setInputPaths(job, new Path(Common.K_MID_DIR + "/" +Common.K_FLATTERN_DIR+"/part-r-00000"));
		FileOutputFormat.setOutputPath(job, new Path(Common.K_MID_DIR + "/" + Common.K_PAIRS_DIR));
	    boolean sucess = job.waitForCompletion(true);
		// TODO Auto-generated method stub
		return sucess ?0:1;
	}

}
