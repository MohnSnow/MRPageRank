/**
 * 
 */
package cn.edu.bistu.mrpr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * @author chenruoyu
 *
 */
public class LLWritable implements Writable {

	private Text pageId;

	/**
	 * 当前节点在前一轮的PageRank值，DoubleWritable类型
	 */
	private DoubleWritable prevPR;
	
	// 当前节点的邻接链表
	private TextArrayWritable linkList;

	public LLWritable() {
		this.pageId = new Text();
		this.prevPR = new DoubleWritable(0);
		this.linkList = new TextArrayWritable();
	}

	public LLWritable(Text pageId, DoubleWritable prevPR,
			TextArrayWritable linkList) {
		this.pageId = pageId;
		this.prevPR = prevPR;
		this.linkList = linkList;
	}

	public LLWritable(String pageId, double prevPR, String[] linkList) {
		this.pageId = new Text(pageId);
		this.prevPR = new DoubleWritable(prevPR);
		this.linkList = new TextArrayWritable(linkList);
	}

	public Text getPageId() {
		return pageId;
	}

	public void setPageId(Text pageId) {
		this.pageId = pageId;
	}

	public DoubleWritable getPrevPR() {
		return prevPR;
	}

	public void setPrevPR(DoubleWritable prevPR) {
		this.prevPR = prevPR;
	}

	public TextArrayWritable getLinkList() {
		return linkList;
	}

	public void setLinkList(TextArrayWritable linkList) {
		this.linkList = linkList;
	}

	public void write(DataOutput dataOutput) throws IOException {
		pageId.write(dataOutput);
		prevPR.write(dataOutput);
		linkList.write(dataOutput);
	}

	public void readFields(DataInput dataInput) throws IOException {
		pageId.readFields(dataInput);
		prevPR.readFields(dataInput);
		linkList.readFields(dataInput);
	}

	@Override
	public int hashCode() {
		return pageId.hashCode();
	}
}
