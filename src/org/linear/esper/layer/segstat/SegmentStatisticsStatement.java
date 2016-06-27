package org.linear.esper.layer.segstat;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Set;

import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.naming.NamingException;

import org.linear.esper.input.InputEventInjectorClient;
import org.linear.esper.util.Constants;

import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.espertech.esper.event.map.MapEventBean;
import com.espertech.esper.example.servershell.jms.JMSContext;
import com.espertech.esper.example.servershell.jms.JMSContextFactory;

public class SegmentStatisticsStatement {
	private long currentSecond;
    private ArrayList<MapEventBean> evtListNOV;
	private ArrayList<MapEventBean> evtListLAV;
	private byte minuteCounter;
	private int lavWindow = 5; //This is LAV window in minutes
	
	private JMSContext jmsCtx_toll;
	private MessageProducer producer_toll;
		
	public SegmentStatisticsStatement(){
		this.currentSecond = -1;
        new LinkedList<MapEventBean>();
        evtListNOV = new ArrayList<MapEventBean>();
        evtListLAV = new ArrayList<MapEventBean>();
        
        Properties properties = new Properties();
        InputStream propertiesIS = InputEventInjectorClient.class.getClassLoader().getResourceAsStream(org.linear.esper.util.Constants.CONFIG_FILENAME);
        if (propertiesIS == null)
        {
            throw new RuntimeException("Properties file '" + org.linear.esper.util.Constants.CONFIG_FILENAME + "' not found in classpath");
        }

        try {
			properties.load(propertiesIS);
	
			String destination_toll = properties.getProperty(org.linear.esper.util.Constants.JMS_INCOMING_DESTINATION_TOLL_LAYER);
			String jmsurl_toll = properties.getProperty(org.linear.esper.util.Constants.JMS_PROVIDER_URL_TOLL_LAYER);
			String connFactoryName = properties.getProperty(org.linear.esper.util.Constants.JMS_CONNECTION_FACTORY_NAME);
	        String user = properties.getProperty(org.linear.esper.util.Constants.JMS_USERNAME);
	        String password = properties.getProperty(org.linear.esper.util.Constants.JMS_PASSWORD);
	        boolean isTopic = Boolean.parseBoolean(properties.getProperty(org.linear.esper.util.Constants.JMS_IS_TOPIC));
	        String factory = properties.getProperty(org.linear.esper.util.Constants.JMS_CONTEXT_FACTORY);
			jmsCtx_toll = JMSContextFactory.createContext(factory, jmsurl_toll, connFactoryName, user, password, destination_toll, isTopic);
			jmsCtx_toll.getConnection().start();
			producer_toll = jmsCtx_toll.getSession().createProducer(jmsCtx_toll.getDestination());
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JMSException ec) {
			ec.printStackTrace();
		} catch (NamingException ex) {
			ex.printStackTrace();
		}        
	}

	public void createStatement(EPAdministrator admin){
		EPStatement statement = admin.createEPL("select ttime, vid, speed, xway, mile, poffset, lane, dir from PositionReportEvent");
		
		statement.addListener(new UpdateListener(){
			@Override
			public void update(EventBean[] newEvents, EventBean[] oldEvents) {
				//The update() method is called with either of the parameters being null (but not both simultaneously). Therefore, we have to
				//eliminate the scenario of calling the update() with oldEvents.
				if(oldEvents == null){
					MapEventBean evt = (MapEventBean) newEvents[0];
					long evttime = (Long) evt.get("ttime");
					
	        		if(currentSecond == -1){
	        			currentSecond = evttime;
	        		}else{
	        			if((evttime - currentSecond) > 60){
	        				calculateNOV();
	        				
	    					evtListNOV.clear();
	    					
	    					currentSecond = evttime;
	    					minuteCounter++;
	    					
	    					if(minuteCounter >= lavWindow){
	    						calculateLAV(currentSecond);
	    							
	    						//LAV list cannot be cleared because it need to keep the data for 5 minutes prior to any time t
	    						//evtListLAV.clear();
	    						
	    						minuteCounter = 0;
	    					}
	        			}
	        		}
	    			evtListNOV.add(evt);
	    			evtListLAV.add(evt);
				}
			}
		});
	}
	
	private void calculateLAV(long currentTime) {
		ArrayList<Byte> segList = new ArrayList<Byte>();
		//First identify the number of segments
		Iterator<MapEventBean> itr = evtListLAV.iterator();
		byte curID = -1;
		while(itr.hasNext()){
			curID = (Byte)itr.next().get("mile");
			
			if(!segList.contains(curID)){
				segList.add(curID);
			}
		}
		
		ArrayList<MapEventBean> tmpEvtListLAV = new ArrayList<MapEventBean>(); 
		float lav = -1;
		
		for(byte i =0; i < 2; i++){ //We need to do this calculation for both directions (west = 0; East = 1)
			Iterator<Byte> segItr = segList.iterator();
			byte mile = -1;
			MapEventBean evt = null;
			long totalSegmentVelocity = 0;
			long totalSegmentVehicles = 0;
			BytesMessage bytesMessage = null;
			//We calculate LAV per segment
			while(segItr.hasNext()){
				mile = segItr.next();
				itr = evtListLAV.iterator();
				
				while(itr.hasNext()){
					evt = itr.next();
					
					if((Math.abs((((Long)evt.get("ttime")) - currentTime)) < 300)){
						if((((Byte)evt.get("mile")) == mile) && (i == ((Byte)evt.get("dir")))){ //Need only last 5 minutes data only
							totalSegmentVelocity += (Byte)evt.get("speed");
							totalSegmentVehicles++;
						}
						
						if(i == 1){//We need to add the events to the temp list only once. Because we iterate twice through the list
							tmpEvtListLAV.add(evt);//Because of the filtering in the previous if statement we do not accumulate events that are older than 5 minutes
						}
					}
				}
				
				lav = ((float)totalSegmentVelocity/totalSegmentVehicles);
				if(!Float.isNaN(lav)){
					try{
					    bytesMessage = jmsCtx_toll.getSession().createBytesMessage();
					    //lavEventOccurred
					    bytesMessage.writeBytes((Constants.LAV_EVENT_TYPE + " " + mile + " " + lav + " " + i).getBytes());
					    bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
					    producer_toll.send(bytesMessage);
					}catch(JMSException e){
						e.printStackTrace();
					}
					//Also need to add to DB
					totalSegmentVelocity = 0;
					totalSegmentVehicles = 0;
				}
			}
		}
		
		//We assign the updated list here. We have discarded the events that are more than 5 minutes duration
		evtListLAV = tmpEvtListLAV;	
	}
	
	private void calculateNOV() {
		BytesMessage bytesMessage = null;
		ArrayList<Byte> segList = new ArrayList<Byte>();
		Hashtable<Byte, ArrayList<Integer> > htResult = new Hashtable<Byte, ArrayList<Integer> >();  
		
		//Get the list of segments first
		Iterator<MapEventBean> itr = evtListNOV.iterator();
		byte curID = -1;
		while(itr.hasNext()){
			curID = (Byte) itr.next().get("mile");
			
			if(!segList.contains(curID)){
				segList.add(curID);
			}
		}
		
		Iterator<Byte> segItr = segList.iterator();
		int vid = -1;
		byte mile = -1;
		ArrayList<Integer> tempList = null;
		MapEventBean evt = null;

		//For each segment		
		while(segItr.hasNext()){
			mile = segItr.next();
			itr = evtListNOV.iterator();
			while(itr.hasNext()){
				evt = itr.next();
				
				if(((Byte)evt.get("mile")) == mile){
					vid = (Integer) evt.get("vid");
					
					if(!htResult.containsKey(mile)){
						tempList = new ArrayList<Integer>();
						tempList.add(vid);
						htResult.put(mile, tempList);
					}else{
						tempList = htResult.get(mile);
						tempList.add(vid);
						
						htResult.put(mile, tempList);
					}
				}
			}
		}
		
		Set<Byte> keys = htResult.keySet();
		
		Iterator<Byte> itrKeys = keys.iterator();
		int numVehicles = -1;
		mile = -1;
		
		while(itrKeys.hasNext()){
			mile = itrKeys.next();
			numVehicles = htResult.get(mile).size();
			
			try{
			    bytesMessage = jmsCtx_toll.getSession().createBytesMessage();

			    bytesMessage.writeBytes((Constants.NOV_EVENT_TYPE + " " + ((int)Math.floor(currentSecond/60)) + " " + mile + " " + numVehicles).getBytes());
			    bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
			    producer_toll.send(bytesMessage);
			}catch(JMSException e){
				e.printStackTrace();
			}
		}
	}
}