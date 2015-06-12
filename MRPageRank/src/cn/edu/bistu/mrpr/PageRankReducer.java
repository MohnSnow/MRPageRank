package cn.edu.bistu.mrpr;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/**
 * @author twinsen
 */
public class PageRankReducer extends Reducer<Text, PRWritable, NullWritable, Text> {		
	//private static final Log log = LogFactory.getLog(PageRankReducer.class);
	private Text value = new Text();
	
	@Override
	protected void reduce(Text key, Iterable<PRWritable> values, Context context)
			throws IOException, InterruptedException {
			String src = key.toString();
			double sum = 0;//不考虑汇点和阻尼系数／摩擦系数所计算出来的部分PageRank值
			double prevPr = 0;//前一轮所计算出来的PageRank
			StringBuffer buf = new StringBuffer();
			for(PRWritable value:values){
				Writable v = value.get();
				if(v instanceof DoubleWritable){
					DoubleWritable dv = (DoubleWritable)v;
					sum+=dv.get();
				}else{
					LLWritable llwv = (LLWritable)v;
					prevPr = llwv.getPrevPR().get();//当前页面在前一轮时的PageRank值
					Writable[] textValues = llwv.getLinkList().get();//获取页面的邻接链表
					if(textValues!=null&&textValues.length>0){
						for(int i=0;i<textValues.length;i++){
							Text destId = (Text)textValues[i];
							buf.append(","+destId.toString());
						}
					}
				}
			}
			value.set(src+","+prevPr+","+sum+buf.toString());
			context.write(null, value);
	}
}
