package MahourRec;
/*
 * 把用户评分归并一起
 **/
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

public class FlatternUser extends Configured implements Tool{
	public static class MyMap extends Mapper<LongWritable,Text,Text,Text>{
		@Override
		public void map(LongWritable key,Text value,Context ctx) throws IOException,InterruptedException{
			String[] s = value.toString().split("\t");
			ctx.write(new Text(s[0]), new Text(s[1]+":" + s[2]));
		}
	}
    public static class MyReducer extends Reducer<Text,Text,Text,Text>{
    	@Override
    	public void reduce(Text key,Iterable<Text> values,Context ctx) throws IOException,InterruptedException{
            StringBuilder sb = new StringBuilder();
    		for(Text e :values){
    			sb.append(e.toString()+"\t");
    		}
    		sb.delete(sb.length()-1, sb.length());
    		ctx.write(key, new Text(sb.toString()));
    	}
    }
  
	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Job job = Job.getInstance(getConf());
		job.setJarByClass(FlatternUser.class);
		
		job.setMapperClass(MyMap.class);
		job.setReducerClass(MyReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		System.out.println("Flatern User Parameters....................");
		System.out.println(Common.K_INPUT_DIR +"/" +Common.K_INPUT_FILENAME);
		System.out.println(Common.K_MID_DIR + "/" + Common.K_FLATTERN_DIR);
		
		FileInputFormat.addInputPath(job,  new Path(Common.K_INPUT_DIR +"/" +Common.K_INPUT_FILENAME));
		FileOutputFormat.setOutputPath(job, new Path(Common.K_MID_DIR + "/" + Common.K_FLATTERN_DIR));
		 
			
		 boolean success = job.waitForCompletion(true);
		 return success ? 0 : 1;
	
	}

}
