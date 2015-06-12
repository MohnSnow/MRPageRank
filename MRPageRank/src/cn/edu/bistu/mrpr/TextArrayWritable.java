/**
 * 
 */
package cn.edu.bistu.mrpr;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * 对邻接链表的抽象，邻接链表本身是一个PageID的列表，PageID是以长整型表示的
 * @author chenruoyu
 *
 */
public class TextArrayWritable extends ArrayWritable {
	public TextArrayWritable() {
		super(Text.class);
	}
	
	public TextArrayWritable(String[] textValues) {
		super(LongWritable.class);
		Text[] values = new Text[textValues.length];
		for (int i = 0; i < textValues.length; i++) {
			values[i] = new Text(textValues[i]);
		}
		set(values);
	}
}
