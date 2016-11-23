package MahourRec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import MahourRec.FlatternUser;
import MahourRec.ComputePairs;
import MahourRec.SlopOne;

public class MainRun extends Configured implements Tool {


	@Override
	public int run(String[] args) throws Exception {
		
		//FileSystem fs = FileSystem.get(getConf());
		//FileStatus stats[] = fs.listStatus(filePath); 
		FlatternUser fu = new FlatternUser();
		if(ToolRunner.run(new Configuration(), fu,args) == 1){
			throw new Exception("Flattern User Failed!");
		}
		
		ComputePairs cp = new ComputePairs();
		if(ToolRunner.run(new Configuration(), cp,args) == 1){
			throw new Exception("ComputePair Failed!");
		}
		
		SlopOne so = new SlopOne();
		if( ToolRunner.run(new Configuration(), so,args) == 1){
			throw new Exception("SlopOne Failed!");
		}
		// TODO Auto-generated method stub

		/*System.out.println("Flatern User Parameters....................");
		System.out.println(Common.K_INPUT_DIR +"/" +Common.K_INPUT_FILENAME);
		System.out.println(Common.K_MID_DIR + "/" + Common.K_FLATTERN_DIR);
		
		System.out.println("Compute Pairs Parameters....................");
		System.out.println(Common.K_MID_DIR + "/" +Common.K_FLATTERN_DIR+"/part-r-00000");
		System.out.println(Common.K_MID_DIR + "/" + Common.K_PAIRS_DIR);
		
		System.out.println("SlopOne Paramters .......................");
		System.out.println(Common.K_MID_DIR + "/" + Common.K_FLATTERN_DIR+"/part-r-00000");
		System.out.println(Common.K_OUTPUT_DIR);*/
		
		return 0;
	}
	public static void main(String[] args) throws Exception{
		int res = ToolRunner.run(new Configuration(), new MainRun(), args);
		System.exit(res);
	}

}
