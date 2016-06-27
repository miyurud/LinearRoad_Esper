/**
 * 
 */
package org.linear.esper.events;

import java.util.EventObject;

/**
 * @author miyuru
 *
 */
public class ExpenditureEvent {
	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public int getVid() {
		return vid;
	}

	public void setVid(int vid) {
		this.vid = vid;
	}

	public int getQid() {
		return qid;
	}

	public void setQid(int qid) {
		this.qid = qid;
	}

	public byte getxWay() {
		return xWay;
	}

	public void setxWay(byte xWay) {
		this.xWay = xWay;
	}

	public int getDday() {
		return dday;
	}

	public void setDday(int day) {
		this.dday = day;
	}

	@Override
	public String toString() {
		return "ExpenditureEvent [time=" + time + ", vid=" + vid + ", qid="
				+ qid + ", xWay=" + xWay + "]";
	}

	public long time; //A timestamp measured in seconds since the start of the simulation
	public int vid; //vehicle identifier
	public int qid; //Query ID
	public byte xWay; //Express way number 0 .. 9
	public int dday; //The day for which the daily expenditure value is needed
	
	public ExpenditureEvent(){
		
	}
	
	public ExpenditureEvent(String[] fields) {	
		this.time = Long.parseLong(fields[1]);//Seconds since start of simulation
		this.vid = Integer.parseInt(fields[2]);//Car ID
		this.qid = Integer.parseInt(fields[9]);//Query ID
		this.xWay = Byte.parseByte(fields[4]);//Expressway number 
		this.dday = Integer.parseInt(fields[14]);//Day
	}

}
