package my.loganalyzer.dto;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class LogMessageWritable implements Writable {
	
	private Text timestamp, logLevel, user, tid, action;
	private String status;
	
	public LogMessageWritable() {
		
		this.timestamp = new Text();
		this.logLevel = new Text();
		this.user = new Text();
		this.tid = new Text();
		this.action = new Text();
		status = "";
	}

	public Text getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Text timestamp) {
		this.timestamp = timestamp;
	}

	public Text getLogLevel() {
		return logLevel;
	}

	public void setLogLevel(Text logLevel) {
		this.logLevel = logLevel;
	}

	public Text getUser() {
		return user;
	}

	public void setUser(Text user) {
		this.user = user;
	}

	public Text getTid() {
		return tid;
	}

	public void setTid(Text tid) {
		this.tid = tid;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public Text getAction() {
		return action;
	}

	public void setAction(Text action) {
		this.action = action;
	}

	public void readFields(DataInput in) throws IOException {
		
		timestamp.readFields(in);
		logLevel.readFields(in);
		user.readFields(in);
		tid.readFields(in);
		action.readFields(in);
		status = in.readUTF();
		
	}
	
	public void write(DataOutput out) throws IOException {
		
		timestamp.write(out);
		logLevel.write(out);
		user.write(out);
		tid.write(out);
		action.write(out);
		out.writeUTF(status);
	}
	
	
	
}