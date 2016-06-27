package org.linear.esper.layer.dailyexp;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;

import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.linear.esper.events.ExpenditureEvent;
import org.linear.esper.events.HistoryEvent;
import org.linear.esper.input.InputEventInjectorClient;
import org.linear.esper.util.Constants;
import org.linear.esper.util.Utilities;

import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.espertech.esper.example.servershell.jms.JMSContext;
import com.espertech.esper.example.servershell.jms.JMSContextFactory;

public class DailyExpensesStatement {
    private static Log log = LogFactory.getLog(DailyExpensesListener.class);
    private EPRuntime engine;
    private int count;
    private String host;
    private int port;
    
    private LinkedList<ExpenditureEvent> expEvtList;
    private LinkedList<HistoryEvent> historyEvtList;
    
	private JMSContext jmsCtx_output;
	private MessageProducer producer_output;
	
	public DailyExpensesStatement(){
        expEvtList = new LinkedList<ExpenditureEvent>();
        historyEvtList = new LinkedList<HistoryEvent>();
        
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
			String connFactoryName = properties.getProperty(org.linear.esper.util.Constants.JMS_CONNECTION_FACTORY_NAME);
	        String user = properties.getProperty(org.linear.esper.util.Constants.JMS_USERNAME);
	        String password = properties.getProperty(org.linear.esper.util.Constants.JMS_PASSWORD);
	        boolean isTopic = Boolean.parseBoolean(properties.getProperty(org.linear.esper.util.Constants.JMS_IS_TOPIC));
	        String factory = properties.getProperty(org.linear.esper.util.Constants.JMS_CONTEXT_FACTORY);
			jmsCtx_output = JMSContextFactory.createContext(factory, jmsurl_output, connFactoryName, user, password, destination_output, isTopic);
			jmsCtx_output.getConnection().start();
			producer_output = jmsCtx_output.getSession().createProducer(jmsCtx_output.getDestination());
			
			host = properties.getProperty(org.linear.esper.util.Constants.LINEAR_DB_HOST);
			port = Integer.parseInt(properties.getProperty(org.linear.esper.util.Constants.LINEAR_DB_PORT));
			String historyFile = properties.getProperty(org.linear.esper.util.Constants.LINEAR_HISTORY);
			
			HistoryLoadingNotifier notifierObj = new HistoryLoadingNotifier(false); 
			notifierObj.start();//The notification server starts at this point
			
			loadHistoricalInfo(historyFile);
			
			notifierObj.setStatus(true);//At this moment we notify all the listeners that we have done loading the history data			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JMSException ec) {
			ec.printStackTrace();
		} catch (NamingException ex) {
			ex.printStackTrace();
		}
    }
	
	public void createStatement(EPAdministrator admin){
		EPStatement statement = admin.createEPL("select time, vid, qid, xWay, dday from ExpenditureEvent");
		
		statement.addListener(new UpdateListener(){

			@Override
			public void update(EventBean[] arg0, EventBean[] arg1) {
				ExpenditureEvent evt = new ExpenditureEvent();
				evt.time = (Long)arg0[0].get("time");
				evt.vid = (Integer)arg0[0].get("vid");
				evt.qid = (Integer)arg0[0].get("qid");
				evt.xWay = (Byte)arg0[0].get("xWay");
				evt.dday = (Integer)arg0[0].get("dday");
				process(evt);
			}
		});
	}
	
    public void process(ExpenditureEvent evt){    	
		int len = 0;
		Statement stmt;
		BytesMessage bytesMessage = null;
		
		Iterator<HistoryEvent> itr = historyEvtList.iterator();
		int sum = 0;
		while(itr.hasNext()){
			HistoryEvent histEvt = (HistoryEvent)itr.next();
			
			if((histEvt.carid == evt.vid) && (histEvt.x == evt.xWay) && (histEvt.d == evt.dday)){
				sum += histEvt.daily_exp;
			}					
		}
		
		try{
		    bytesMessage = jmsCtx_output.getSession().createBytesMessage();

		    bytesMessage.writeBytes((Constants.DAILY_EXP_EVENT_TYPE + " " + evt.vid + " " + sum).getBytes());
		    bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
		    producer_output.send(bytesMessage);
		}catch(JMSException e){
			e.printStackTrace();
		}
    }
    
	public void loadHistoricalInfo(String inputFileHistory) {
			BufferedReader in;
			try {
				in = new BufferedReader(new FileReader(inputFileHistory));
			
			String line;
			int counter = 0;
			int batchCounter = 0;
			int BATCH_LEN = 10000;//A batch size of 1000 to 10000 is usually OK		
			Statement stmt;
			StringBuilder builder = new StringBuilder();
					
			log.info(Utilities.getTimeStamp() + " : Loading history data");
			while((line = in.readLine()) != null){	
				//#(1 8 0 55)
				/*
				0 - Car ID
				1 - day
				2 - x - Expressway number
				3 - daily expenditure
				*/
				
				String[] fields = line.split(" ");
				fields[0] = fields[0].substring(2);
				fields[3] = fields[3].substring(0, fields[3].length() - 1);
				
				historyEvtList.add(new HistoryEvent(Integer.parseInt(fields[0]), Integer.parseInt(fields[1]), Integer.parseInt(fields[2]), Integer.parseInt(fields[3])));
			}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (NumberFormatException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			log.info(Utilities.getTimeStamp() + " : Done Loading history data");
			//Just notfy this to the input event injector so that it can start the data emission process
			try {
				PrintWriter writer = new PrintWriter("done.txt", "UTF-8");
				writer.println("\n");
				writer.flush();
				writer.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
	}
}
