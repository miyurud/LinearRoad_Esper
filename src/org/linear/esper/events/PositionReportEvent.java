/**
 * 
 */
package org.linear.esper.events;

/**
 * @author miyuru
 *
 */
public class PositionReportEvent{
	public long ttime; //A timestamp measured in seconds since the start of the simulation
	public int vid; //vehicle identifier
	public byte speed; // An integer number of miles per hour between 0 and 100 
	public byte xway; //Express way number 0 .. 9
	public byte mile; //Mile number 0..99
	public short poffset; // Yards since last Mile Marker 0..1759
	public byte lane; //Travel Lane 0..7. The lanes 0 and 7 are entrance/exit ramps
	public byte dir; //Direction 0(West) or 1 (East)
	
	public PositionReportEvent(String[] fields) {	
		this.ttime = Long.parseLong(fields[1]);//Seconds since start of simulation
		this.vid = Integer.parseInt(fields[2]);//Car ID
		this.speed = Byte.parseByte(fields[3]);//An integer number of miles per hour
		this.xway = Byte.parseByte(fields[4]);//Expressway number 
		this.mile = Byte.parseByte(fields[7]);//Mile (This corresponds to the seg field in the original table)
		this.poffset  = (short)(Integer.parseInt(fields[8]) - (this.mile * 5280)); //Distance from the last mile post
		this.lane = Byte.parseByte(fields[5]); //The lane number
		this.dir = Byte.parseByte(fields[6]); //Direction (west = 0; East = 1)
	}
	
	public PositionReportEvent() {
		
	}

	public String toString(){
		return "PositionReportEvent -> Time : " + this.ttime + " vid : " + this.vid + " speed : " + this.speed + " xway : " + this.xway + "...";
	}
	
	public long getTtime(){
		return ttime;
	}
	
	public int getVid(){
		return vid;
	}
	
	public byte getSpeed(){
		return speed;
	}
	
	public byte getXway(){
		return xway;
	}
	
	public byte getMile(){
		return mile;
	}
	
	public short getPoffset(){
		return poffset;
	}

	public byte getLane(){
		return lane;
	}
	
	public byte getDir(){
		return dir;
	}
}
