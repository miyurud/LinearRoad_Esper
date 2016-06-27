/**
 * 
 */
package org.linear.esper.events;

/**
 * @author miyuru
 *
 */
public class AccountBalanceEvent {

	public long getTtime() {
		return ttime;
	}

	public void setTtime(long ttime) {
		this.ttime = ttime;
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

	@Override
	public String toString() {
		return "AccountBalanceEvent [time=" + ttime + ", vid=" + vid + ", qid="
				+ qid + "]";
	}

	public long ttime; //A timestamp measured in seconds since the start of the simulation
	public int vid; //Vehicle identifier
	public int qid; //Query ID
	
	public AccountBalanceEvent(String[] fields) {	
		this.ttime = Long.parseLong(fields[1]);//Seconds since start of simulation
		this.vid = Integer.parseInt(fields[2]);//Car ID
		this.qid = Integer.parseInt(fields[9]);//Query ID
	}

	public AccountBalanceEvent(long ttime, int tvid, int tqid) {	
		this.ttime = ttime;//Seconds since start of simulation
		this.vid = tvid;//Car ID
		this.qid = tqid;//Query ID
	}

	public AccountBalanceEvent() {

	}
}

