package org.linear.esper.layer.toll;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
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
import org.linear.esper.events.AccidentEvent;
import org.linear.esper.events.LAVEvent;
import org.linear.esper.events.NOVEvent;
import org.linear.esper.events.PositionReportEvent;
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

public class TollStatement {
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
	
	public TollStatement(){
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
	
	public void createStatement(EPAdministrator admin){
		EPStatement statementPositionReports = admin.createEPL("select ttime, vid, speed, xway, mile, poffset, lane, dir from PositionReportEvent");
		
		statementPositionReports.addListener(new UpdateListener(){

			@Override
			public void update(EventBean[] newEvents, EventBean[] oldEvents) {
				if (oldEvents == null){
					MapEventBean evt = (MapEventBean) newEvents[0];
					PositionReportEvent pevt = new PositionReportEvent();
					pevt.ttime = (Long)evt.get("ttime");
					pevt.vid = (Integer)evt.get("vid");
					pevt.speed = (Byte)evt.get("speed");
					pevt.xway = (Byte)evt.get("xway");
					pevt.mile = (Byte)evt.get("mile");
					pevt.poffset = (Short)evt.get("poffset");
					pevt.lane = (Byte)evt.get("lane");
					pevt.dir = (Byte)evt.get("dir");
					
					process(pevt);
				}
			}
			
		});
		
		EPStatement statementLAVEvents = admin.createEPL("select segment, lav, dir from LAVEvent");
		
		statementLAVEvents.addListener(new UpdateListener(){
			@Override
			public void update(EventBean[] newEvents, EventBean[] oldEvents) {
				if (oldEvents == null){
					MapEventBean evt = (MapEventBean) newEvents[0];
					LAVEvent levt = new LAVEvent();
					levt.segment = (Byte)evt.get("segment");
					levt.lav = (Float)evt.get("lav");
					levt.dir = (Byte)evt.get("dir");
					
					lavEventOcurred(levt);
				}
			}
			
		});
		
		EPStatement statementNOVEvents = admin.createEPL("select mminute, segment, nov from NOVEvent");
		
		statementNOVEvents.addListener(new UpdateListener(){

			@Override
			public void update(EventBean[] newEvents, EventBean[] oldEvents) {
				if (oldEvents == null){
					MapEventBean evt = (MapEventBean) newEvents[0];
					NOVEvent nevt = new NOVEvent();
					nevt.mminute = (Integer)evt.get("mminute");
					
					try{
						nevt.segment = (Byte)evt.get("segment");
					}catch(NumberFormatException e){
						System.out.println("Not Number Format Exception for tuple : " + evt.toString());
					}
					nevt.nov = (Integer)evt.get("nov");
					novEventOccurred(nevt);
				}
			}
			
		});
	}
	
	public void accidentEventOccurred(AccidentEvent accEvent) {
		System.out.println("Accident Occurred :" + accEvent.toString());
		boolean flg = false;
		
		synchronized(this){
			flg = segments.containsKey(accEvent.mile);
		}
		
		if(!flg){
			AccNovLavTuple obj = new AccNovLavTuple();
			obj.isAcc = true;
			synchronized(this){
				segments.put(accEvent.mile, obj);
			}
		}else{
			synchronized(this){
				AccNovLavTuple obj = segments.get(accEvent.mile);
				obj.isAcc = true;
				segments.put(accEvent.mile, obj);
			}
		}
	}
    
    public void novEventOccurred(NOVEvent novEvent){
		boolean flg = false;

		flg = segments.containsKey(novEvent.segment);
	
		if(!flg){
			AccNovLavTuple obj = new AccNovLavTuple();
			obj.nov = novEvent.nov;
			segments.put(novEvent.segment, obj);
		}else{
			AccNovLavTuple obj = segments.get(novEvent.segment);
			obj.nov = novEvent.nov;

			segments.put(novEvent.segment, obj);
		}    	
    }
    
    public void lavEventOcurred(LAVEvent lavEvent){
		boolean flg = false;
		
		flg = segments.containsKey(lavEvent.segment); 
		
		if(!flg){
			AccNovLavTuple obj = new AccNovLavTuple();
			obj.lav = lavEvent.lav;
			segments.put(lavEvent.segment, obj);
		}else{
			AccNovLavTuple obj = segments.get(lavEvent.segment);
			obj.lav = lavEvent.lav;
			segments.put(lavEvent.segment, obj);
		}
    }

	public void process(PositionReportEvent evt){
		BytesMessage bytesMessage = null;
				
		Iterator<Car> itr = cars_list.iterator();
	
		if(!carMap.containsKey(evt.vid)){
			Car c = new Car();
			c.carid = evt.vid;
			c.mile = evt.mile;
			carMap.put(evt.vid, c);
		}else{
			Car c = carMap.get(evt.vid);

				if(c.mile != evt.mile){ //Car is entering a new mile/new segment
					c.mile = evt.mile;
					carMap.put(evt.vid, c);

					if((evt.lane != 0)&&(evt.lane != 7)){ //This is to make sure that the car is not on an exit ramp
						AccNovLavTuple obj = null;
						
						obj = segments.get(evt.mile);

						if(obj != null){									
							if(isInAccidentZone(evt)){
								System.out.println("Its In AccidentZone");
							}
							
							if(((obj.nov < 50)||(obj.lav > 40))||isInAccidentZone(evt)){
								TollCalculationEvent tollEvt = new TollCalculationEvent(); //In this case we set the toll to 0
								tollEvt.vid = evt.vid;
								tollEvt.segment = evt.mile;
																										
								String msg = (Constants.TOLL_EVENT_TYPE + " " + tollEvt.toCompressedString());
								
								try{
								    bytesMessage = jmsCtx_output.getSession().createBytesMessage();
								    bytesMessage.writeBytes(msg.getBytes());
								    bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
								    producer_output.send(bytesMessage);
								}catch(JMSException e){
									e.printStackTrace();
								}
								
								try{
							          bytesMessage = jmsCtx_account_balance.getSession().createBytesMessage();
							          bytesMessage.writeBytes(msg.getBytes());
							          bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
							          producer_account_balance.send(bytesMessage);
									} catch (JMSException e) {
										e.printStackTrace();
									}
							}else{
								TollCalculationEvent tollEvt = new TollCalculationEvent(); //In this case we need to calculate a toll
								tollEvt.vid = evt.vid;
								tollEvt.segment = evt.mile;
								
								if(segments.containsKey(evt.mile)){
									AccNovLavTuple tuple = null;
									
									synchronized(this){
										tuple = segments.get(evt.mile);
									}
																				
									tollEvt.toll = BASE_TOLL*(tuple.nov - 50)*(tuple.nov - 50);
																		
									String msg = (Constants.TOLL_EVENT_TYPE + " " + tollEvt.toCompressedString());
									
									try{
									    bytesMessage = jmsCtx_output.getSession().createBytesMessage();

									    bytesMessage.writeBytes(msg.getBytes());
									    bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
									    producer_output.send(bytesMessage);
									}catch(JMSException e){
										e.printStackTrace();
									}
									
									try{
								          bytesMessage = jmsCtx_account_balance.getSession().createBytesMessage();
								          bytesMessage.writeBytes(msg.getBytes());
								          bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
								          producer_account_balance.send(bytesMessage);
										} catch (JMSException e) {
											e.printStackTrace();
										}
								}
							}						
						}
					}
				}
		}
	}
    
	private boolean isInAccidentZone(PositionReportEvent evt) {
		byte mile = evt.mile;
		byte checkMile = (byte) (mile + NUM_SEG_DOWNSTREAM);
		
		while(mile < checkMile){
			if(segments.containsKey(mile)){
				AccNovLavTuple obj = segments.get(mile);
				
				if(Math.abs((evt.ttime - obj.time)) > 20){
					obj.isAcc = false;
					mile++;
					continue; //May be we remove the notification for a particular mile down the xway. But another mile still might have accident. Therefore, we cannot break here.
				}
				
				if(obj.isAcc){
					return true;
				}
			}
			mile++;
		}
		
		return false;
	}
}
