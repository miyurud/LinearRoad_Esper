/**************************************************************************************
 * Copyright (C) 2008 EsperTech, Inc. All rights reserved.                            *
 * http://esper.codehaus.org                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package org.linear.esper.layer.accbalance;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Properties;

import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.example.servershell.jms.JMSContext;
import com.espertech.esper.example.servershell.jms.JMSContextFactory;

import javax.jms.DeliveryMode;
import javax.jms.MessageListener;
import javax.jms.Message;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.linear.db.LinearRoadDBComm;
import org.linear.esper.events.AccidentEvent;
import org.linear.esper.events.AccountBalanceEvent;
import org.linear.esper.events.LAVEvent;
import org.linear.esper.events.NOVEvent;
import org.linear.esper.events.PositionReportEvent;
import org.linear.esper.events.TollCalculationEvent;
import org.linear.esper.input.InputEventInjectorClient;
import org.linear.esper.util.Constants;


public class AccountBalanceListener implements MessageListener
{
    private static Log log = LogFactory.getLog(AccountBalanceListener.class);
    private EPRuntime engine;
    private int count;
    private LinkedList<AccountBalanceEvent> accEvtList;
    private HashMap<Integer, Integer> tollList;
    
	private JMSContext jmsCtx_output;
	private MessageProducer producer_output;
	
    private long tupleCounter = 0;
    private int tupleCountingWindow = 5000;//This is in miliseconds
    private long previousTime = 0; //This is the time that the tuple measurement was taken previously
    private PrintWriter outLogger = null;
    private int dataRate = 0;
    private long currentTime = 0;
    private long expStartTime = 0; 
	
    public AccountBalanceListener(EPRuntime engine)
    {
        this.engine = engine;
        this.accEvtList = new LinkedList<AccountBalanceEvent>();
        this.tollList = new HashMap<Integer, Integer>();
     
		currentTime = System.currentTimeMillis();
		try {
			outLogger = new PrintWriter(new BufferedWriter(new FileWriter("esper-accbalance-rate.csv", true)));
            outLogger.println("----- New Session -----<date-time>,<wall-clock-time(s)>,<physical-tuples-in-last-period>,<physical-tuples-data-rate>");
			outLogger.println("Date,Wall clock time (s),TTuples,Data rate (tuples/s)");
		} catch (IOException e1) {
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
       	   case Constants.ACC_BAL_EVENT_TYPE:
       		   AccountBalanceEvent et = new AccountBalanceEvent(Long.parseLong(fields[1]), Integer.parseInt(fields[2]), Integer.parseInt(fields[9]));
       		   engine.sendEvent(et);
       		   //process(et);
       		   //System.out.println("ACC BAL Evt : " + et.toString());
       		   break;
	       case Constants.TOLL_EVENT_TYPE:
	    	   int key = Integer.parseInt(fields[1]);
	    	   int value = Integer.parseInt(fields[2]);
	    	   
	    	   TollCalculationEvent tollEVt = new TollCalculationEvent();
	    	   tollEVt.vid = key;
	    	   tollEVt.toll = value;
	    	   engine.sendEvent(tollEVt);
//	    	   Integer kkey = (Integer)tollList.get(key);
//	    	   
//	    	   //System.out.println("key : " + key + " value : " + value);
//	    	   
//	    	   if(kkey != null){
//	    		   tollList.put(key, (kkey + value)); //If the car id is already in the hashmap we need to add the tool to the existing toll.
//	    	   }else{
//	    		   tollList.put(key, value);
//	    	   }
	    	   
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
