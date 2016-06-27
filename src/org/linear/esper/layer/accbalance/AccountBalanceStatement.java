/**
 * 
 */
package org.linear.esper.layer.accbalance;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Properties;

import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.linear.esper.events.AccountBalanceEvent;
import org.linear.esper.events.TollCalculationEvent;
import org.linear.esper.input.InputEventInjectorClient;
import org.linear.esper.util.Constants;

import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.espertech.esper.event.map.MapEventBean;
import com.espertech.esper.example.servershell.jms.JMSContext;
import com.espertech.esper.example.servershell.jms.JMSContextFactory;

/**
 * @author miyuru
 *
 */
public class AccountBalanceStatement {
    private static Log log = LogFactory.getLog(AccountBalanceListener.class);
    private EPRuntime engine;
    private int count;
    private LinkedList<AccountBalanceEvent> accEvtList;
    private HashMap<Integer, Integer> tollList;
    
	private JMSContext jmsCtx_output;
	private MessageProducer producer_output;
	
	public AccountBalanceStatement(){
        this.accEvtList = new LinkedList<AccountBalanceEvent>();
        this.tollList = new HashMap<Integer, Integer>();
        
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
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JMSException ec) {
			ec.printStackTrace();
		} catch (NamingException ex) {
			ex.printStackTrace();
		}
	}
	
	
	
    public void process(AccountBalanceEvent evt){
		BytesMessage bytesMessage = null;
			    		
		if(evt != null){
				try{
				    bytesMessage = jmsCtx_output.getSession().createBytesMessage();
				    bytesMessage.writeBytes((Constants.ACC_BAL_EVENT_TYPE + " " + evt.vid + " " + tollList.get(evt.vid)).getBytes());
				    bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
				    producer_output.send(bytesMessage);
				}catch(JMSException e){
					e.printStackTrace();
				}
		}
    }



	public void createStatement(EPAdministrator epAdministrator) {
		EPStatement statementToll = epAdministrator.createEPL("select vid, toll, segment from TollCalculationEvent");
		
		statementToll.addListener(new UpdateListener(){

			@Override
			public void update(EventBean[] newEvents, EventBean[] oldEvents) {
				if(oldEvents == null){
					MapEventBean evt = (MapEventBean)newEvents[0];
					TollCalculationEvent tevt = new TollCalculationEvent();
//					tevt.vid = (Integer)evt.get("vid");
//					tevt.toll = (Integer)evt.get("toll");
//					tevt.segment = (Byte)evt.get("segment");
					
			    	   int key = (Integer)evt.get("vid");
			    	   int value = (Integer)evt.get("toll");
			    	   
			    	   Integer kkey = (Integer)tollList.get(key);
			    	   
			    	   //System.out.println("key : " + key + " value : " + value);
			    	   
			    	   if(kkey != null){
			    		   tollList.put(key, (kkey + value)); //If the car id is already in the hashmap we need to add the tool to the existing toll.
			    	   }else{
			    		   tollList.put(key, value);
			    	   }
					
				}
			}
			
		});
		
		EPStatement statementAccBalance = epAdministrator.createEPL("select ttime, vid, qid from AccountBalanceEvent");
		statementAccBalance.addListener(new UpdateListener(){

			@Override
			public void update(EventBean[] newEvents, EventBean[] oldEvents) {
				if(oldEvents == null){
					MapEventBean evt = (MapEventBean)newEvents[0];
					AccountBalanceEvent accbal_evt = new AccountBalanceEvent();
					accbal_evt.ttime = (Long)evt.get("ttime");
					accbal_evt.vid = (Integer)evt.get("vid");
					accbal_evt.qid = (Integer)evt.get("qid");
					process(accbal_evt);
				}
			}
			
		});
	}
}
