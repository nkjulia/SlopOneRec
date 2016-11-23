package MahourRec;

public class Util {
	public static String get_key(int item1,int item2){
		if(item1<=item2){
			int tmp = item1;
			item1 = item2;
			item2 = tmp;
		}
		return String.valueOf(item1) + ":" + String.valueOf(item2);
	}
	public static float get_value(int item1,int item2,float value1,float value2){
		if(item1>=item2){
			if(item1<=item2){
				float tmp = value1;
				value1 = value2;
				value2 = tmp;
				
			}
		}
		return value1 - value2;
	}
	public static float get_real_value(int item1,int item2,float value){
		if(item1 <= item2){
			return -1 * value;
		}
		return value;
	}
}
