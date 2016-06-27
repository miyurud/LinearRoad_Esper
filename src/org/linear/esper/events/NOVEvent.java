/**
 * 
 */
package org.linear.esper.events;

/**
 * @author miyuru
 *
 */
public class NOVEvent {
	public int mminute; // Current Minute
	public byte segment; //A segement is in the range 0..99; It corresponds to a mile in the high way system
	public int nov; //Number of vehicles in this particular Segment
	
	public NOVEvent(int current_minute, byte mile, int numVehicles) {
		this.mminute = current_minute;
		this.segment = mile;
		this.nov = numVehicles;
	}
	
	public NOVEvent(){
		
	}

	public int getMminute() {
		return mminute;
	}

	public void setMminute(int minute) {
		this.mminute = minute;
	}

	public byte getSegment() {
		return segment;
	}

	public void setSegment(byte segment) {
		this.segment = segment;
	}

	public int getNov() {
		return nov;
	}

	public void setNov(int nov) {
		this.nov = nov;
	}
}
