/**************************************************************************************
 * Copyright (C) 2008 EsperTech, Inc. All rights reserved.                            *
 * http://esper.codehaus.org                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package org.linear.esper.layer.toll;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
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

import java.io.PrintWriter;
import java.io.FileWriter;
import java.io.BufferedWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.linear.db.LinearRoadDBComm;
import org.linear.esper.events.AccidentEvent;
import org.linear.esper.events.LAVEvent;
import org.linear.esper.events.NOVEvent;
import org.linear.esper.events.PositionReportEvent;
import org.linear.esper.events.TollCalculationEvent;
import org.linear.esper.input.InputEventInjectorClient;
import org.linear.esper.util.Constants;

public class TollEventListener implements MessageListener
{
	private JMSContext jmsCtx_account_balance;
	private MessageProducer producer_account_balance;
	
	private static Log log = LogFactory.getLog(TollEventListener.class);
    private EPRuntime engine;
    private int count;
    
	LinkedList cars_list = new LinkedList();
	HashMap<Integer, Car> carMap = new HashMap<Integer, Car>(); 
	HashMap<Byte, AccNovLavTuple> segments = new HashMap<Byte, AccNovLavTuple>();
	byte NUM_SEG_DOWNSTREAM = 5; //Number of segments downstream to check whether an accident has happened or not
	int BASE_TOLL = 2; //This is a predefined constant (mentioned in Richard's thesis)
	private LinkedList<PositionReportEvent> posEvtList = new LinkedList<PositionReportEvent>();

	private JMSContext jmsCtx_output;
	private MessageProducer producer_output;

    private long tupleCounter = 0;
    private int tupleCountingWindow = 5000;//This is in miliseconds
    private long previousTime = 0; //This is the time that the tuple measurement was taken previously
    private PrintWriter outLogger = null;
    private int dataRate = 0;
    private long currentTime = 0;
    private long expStartTime = 0;	
	
    public TollEventListener(EPRuntime engine)
    {
        this.engine = engine;
        
		currentTime = System.currentTimeMillis();
		try {
			outLogger = new PrintWriter(new BufferedWriter(new FileWriter("esper-toll-rate.csv", true)));
            outLogger.println("----- New Session -----<date-time>,<wall-clock-time(s)>,<physical-tuples-in-last-period>,<physical-tuples-data-rate>");
			outLogger.println("Date,Wall clock time (s),TTuples,Data rate (tuples/s)");
		} catch (IOException e1) {
			e1.printStackTrace();
		}
        
        Properties properties = new Properties();
        InputStream propertiesIS = InputEventInjectorClient.class.getClassLoader().getResourceAsStream(org.linear.esper.util.Constants.CONFIG_FILENAME);
        if (propertiesIS == null)
        {
            throw new RuntimeException("Properties file '" + org.linear.esper.util.Constants.CONFIG_FILENAME + "' not found in classpath");
        }
        
        try {
			properties.load(propertiesIS);
	
			String destination_output = properties.getProperty(org.linear.esper.util.Constants.JMS_INCOMING_DESTINATION_OUTPUT_LAYER);
			String jmsurl_output = properties.getProperty(org.linear.esper.util.Constants.JMS_PROVIDER_URL_OUTPUT_LAYER);
			String jmsurl_account_balance = properties.getProperty(org.linear.esper.util.Constants.JMS_PROVIDER_URL_ACCBALANCE_LAYER);
			String destination_account_balance = properties.getProperty(org.linear.esper.util.Constants.JMS_INCOMING_DESTINATION_ACCBALANCE_LAYER);
			
			String connFactoryName = properties.getProperty(org.linear.esper.util.Constants.JMS_CONNECTION_FACTORY_NAME);
	        String user = properties.getProperty(org.linear.esper.util.Constants.JMS_USERNAME);
	        String password = properties.getProperty(org.linear.esper.util.Constants.JMS_PASSWORD);
	        boolean isTopic = Boolean.parseBoolean(properties.getProperty(org.linear.esper.util.Constants.JMS_IS_TOPIC));
	        String factory = properties.getProperty(org.linear.esper.util.Constants.JMS_CONTEXT_FACTORY);
			jmsCtx_output = JMSContextFactory.createContext(factory, jmsurl_output, connFactoryName, user, password, destination_output, isTopic);
			jmsCtx_output.getConnection().start();
			producer_output = jmsCtx_output.getSession().createProducer(jmsCtx_output.getDestination());

			jmsCtx_account_balance = JMSContextFactory.createContext(factory, jmsurl_account_balance, connFactoryName, user, password, destination_account_balance, isTopic);
	        jmsCtx_account_balance.getConnection().start();
	        producer_account_balance = jmsCtx_account_balance.getSession().createProducer(jmsCtx_account_balance.getDestination());
			

		} catch (IOException e) {
			e.printStackTrace();
		} catch (JMSException ec) {
			ec.printStackTrace();
		} catch (NamingException ex) {
			ex.printStackTrace();
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
        String tuple = getBody(bytesMsg);
        
       String[] fields = tuple.split(" ");
       byte typeField = Byte.parseByte(fields[0]);
       
       switch(typeField){
       	   case Constants.POS_EVENT_TYPE:
       		   //process(new PositionReportEvent(fields));
       		   engine.sendEvent(new PositionReportEvent(fields));
       		   break;
	       case Constants.LAV_EVENT_TYPE:
	    	   LAVEvent obj = new LAVEvent(Byte.parseByte(fields[1]), Float.parseFloat(fields[2]), Byte.parseByte(fields[3]));
	    	   //lavEventOcurred(obj);
	    	   engine.sendEvent(obj);
	    	   break;
	       case Constants.NOV_EVENT_TYPE:
	    	   NOVEvent obj2 = new NOVEvent(Integer.parseInt(fields[1]), Byte.parseByte(fields[2]), Integer.parseInt(fields[3]));
	    	   engine.sendEvent(obj2);
	    	   //novEventOccurred(obj2);
	    	   break;
	       case Constants.ACCIDENT_EVENT_TYPE:
	    	   //accidentEventOccurred(new AccidentEvent(Integer.parseInt(fields[1]), Integer.parseInt(fields[2]), Byte.parseByte(fields[3]), Byte.parseByte(fields[4]), Byte.parseByte(fields[5]), Long.parseLong(fields[6])));
	    	   AccidentEvent evt = new AccidentEvent(Integer.parseInt(fields[1]), Integer.parseInt(fields[2]), Byte.parseByte(fields[3]), Byte.parseByte(fields[4]), Byte.parseByte(fields[5]), Long.parseLong(fields[6]));
	    	   engine.sendEvent(evt);
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
