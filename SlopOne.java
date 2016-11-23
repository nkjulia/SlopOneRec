package MahourRec;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class SlopOne extends Configured implements Tool{
	public static class MyMap extends Mapper<LongWritable,Text,Text,Text>{
		private Map<String,Integer> pairs_cnt = null;
		private Map<String,Float> pairs_value = null;
		@Override
		public void setup(Context ctx){
			pairs_cnt = new HashMap<String,Integer>();
			pairs_value = new HashMap<String,Float>();
			
			String file = Common.K_MID_DIR+ "/" + Common.K_PAIRS_DIR + "/part-r-00000";
			

		 

			try {
				String ln;
				FileSystem fileSystem = FileSystem.get(ctx.getConfiguration());
			    Path path = new Path(file); 
			    if (!fileSystem.exists(path)) { 
			        System.out.println("File does not exists"); 
			        return; 
			    }
				FSDataInputStream in = fileSystem.open(path); 
				BufferedReader br = new BufferedReader(new InputStreamReader(in));//ctx.getConfiguration().get(file)));
				while((ln=br.readLine())!=null){
					String[] s = ln.split("\t");
					pairs_cnt.put(s[0], Integer.valueOf(s[1]));
					pairs_value.put(s[0], Float.valueOf(s[2]));
				}
				br.close();
				
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.out.println(file + "\tnum_of " + String.valueOf(pairs_cnt.size()) + "\t" + String.valueOf(pairs_value.size()));
		}
		@Override
		public void map(LongWritable key,Text value,Context ctx) throws IOException,InterruptedException{
			
			String[] s = value.toString().split("\t");
			for(int i = 1;i < s.length ; i ++ ){
				String[] pair1 = s[i].split(":");
				int item1 = Integer.parseInt(pair1[0]);
				float value1 = Float.parseFloat(pair1[1]);
				float sum = 1;
				int cnt_sum = 1;
				for(int j = 1 ; j < s.length ; j ++){
					if(j == i ){
						continue;
					}
					String[] pair2 = s[j].split(":");
					int item2 = Integer.parseInt(pair2[0]);
					float value2 = Float.parseFloat(pair2[1]);
					String k = Util.get_key(item1, item2);
					if(pairs_cnt.containsKey(k)){
						float v = pairs_value.get(k);
						float cnt = pairs_cnt.get(k);
						float real_value = Util.get_real_value(item1, item2, v);
						sum += ( real_value + value2 ) * cnt;  
						cnt_sum += cnt;
					}
					//ctx.write(new Text(s[0]), new Text (String.valueOf(value1) + "\t" + String.valueOf(sum / cnt_sum) + "\t" + k + "\t" + String.valueOf(pairs_cnt.containsKey(k))));
				}
				ctx.write(new Text(s[0]), new Text (String.valueOf(value1) + "\t" + String.valueOf(sum / cnt_sum) ));
			}
		}
	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJarByClass(ComputePairs.class);
		
		job.setMapperClass(MyMap.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		System.out.println("SlopOne Paramters .......................");
		System.out.println(Common.K_MID_DIR + "/" + Common.K_FLATTERN_DIR+"/part-r-00000");
		System.out.println(Common.K_OUTPUT_DIR);
		FileInputFormat.setInputPaths(job, new Path(Common.K_MID_DIR + "/" + Common.K_FLATTERN_DIR+"/part-r-00000"));
		FileOutputFormat.setOutputPath(job, new Path(Common.K_OUTPUT_DIR));
	    boolean sucess = job.waitForCompletion(true);
		// TODO Auto-generated method stub
		return sucess ?0:1;
	}
	

}
