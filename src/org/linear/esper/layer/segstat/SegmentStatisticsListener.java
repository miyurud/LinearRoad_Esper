/**************************************************************************************
 * Copyright (C) 2008 EsperTech, Inc. All rights reserved.                            *
 * http://esper.codehaus.org                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package org.linear.esper.layer.segstat;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;

import com.espertech.esper.client.EPRuntime;

import javax.jms.DeliveryMode;
import javax.jms.MessageListener;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.JMSException;

import com.espertech.esper.example.servershell.jms.JMSContext;
import com.espertech.esper.example.servershell.jms.JMSContextFactory;

import javax.jms.MessageProducer;
import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.linear.db.LinearRoadDBComm;
import org.linear.esper.events.PositionReportEvent;
import org.linear.esper.input.InputEventInjectorClient;
import org.linear.esper.util.Constants;

/**
 * @author miyuru
 * 
 * --Richard Thesis
 *  Systems implementing Linear Road must maintain 10 weeks worth of statistics data for each segment on the Linear Road express way.
 *  This data is used for calculating tolls, computing travel time, and toll estimates. The data that must be maintained for each of the L X 200 segments
 *  includes the following
 *  
 *  NOV, LAV
 *  
 * --Uppsala
 * 
 * The segment statistics is responsible for maintianing the statistics for every segment 
 * on every expressway with different time span.
 * 
 * This class is used to calcluate two things
 * 
 * NOV - Number Of Vehicles present on a segment during the minute prior to current minute
 * LAV - Latest Average Velocity (LAV). This is calculated for a given segment with a time span of
 *       the five minutes prior to the current minute. 
 *
 */

public class SegmentStatisticsListener implements MessageListener
{
    private static Log log = LogFactory.getLog(SegmentStatisticsListener.class);
    private EPRuntime engine;
    private int count;

    private long tupleCounter = 0;
    private int tupleCountingWindow = 5000;//This is in miliseconds
    private long previousTime = 0; //This is the time that the tuple measurement was taken previously
    private PrintWriter outLogger = null;
    private int dataRate = 0;
    private long currentTime = 0;	
    private long expStartTime = 0;    
    
    public SegmentStatisticsListener(EPRuntime engine)
    {
        this.engine = engine;
        
		currentTime = System.currentTimeMillis();
		try {
			outLogger = new PrintWriter(new BufferedWriter(new FileWriter("esper-segstat-rate.csv", true)));
            outLogger.println("-------- New Session ------<date-time>,<wall-clock-time(s)>,<physical-tuples-in-last-period>,<physical-tuples-data-rate>");
			outLogger.println("Date,Wall clock time (s),TTuples,Data rate (tuples/s)");
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    }

    public void onMessage(Message message)
    {
		tupleCounter += 1;
		currentTime = System.currentTimeMillis();
		
		if (previousTime == 0){
			previousTime = System.currentTimeMillis();
            expStartTime = previousTime;
		}
		
		if ((currentTime - previousTime) >= tupleCountingWindow){
			dataRate = Math.round((tupleCounter*1000)/(currentTime - previousTime));//need to multiply by thousand to compensate for ms time unit
			Date date = new Date(currentTime);
			DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
			outLogger.println(formatter.format(date) + "," + Math.round((currentTime - expStartTime)/1000) + "," + tupleCounter + "," + dataRate);
			outLogger.flush();
			tupleCounter = 0;
			previousTime = currentTime;
		} 
    	
        BytesMessage bytesMsg = (BytesMessage) message;
        String body = getBody(bytesMsg);
        
        String[] fields = body.split(" ");
        byte typeField = Byte.parseByte(fields[0]);
        
        switch(typeField){
        	case 0:
        		//posEvtList.add(new PositionReportEvent(fields));
        		engine.sendEvent(new PositionReportEvent(fields));
        		count++;
        		break;
        	case 2:
        		log.info("Account balance report");
        		break;
        	case 3:
        		log.info("Expenditure report");
        		break;
        	case 4:
        		log.info("Travel time report");
        		break;
        }
    }

    public int getCount()
    {
        return count;
    }
       
    private String getBody(BytesMessage bytesMsg)
    {
        try
        {
            long length = bytesMsg.getBodyLength();
            byte[] buf = new byte[(int)length];
            bytesMsg.readBytes(buf);
            return new String(buf);
        }
        catch (JMSException e)
        {
            String text = "Error getting message body";
            log.error(text, e);
            throw new RuntimeException(text, e);
        }
    }
}