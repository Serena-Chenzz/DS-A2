package activitystreamer.server.commands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;

import activitystreamer.server.Connection;
import activitystreamer.server.Control;
import activitystreamer.server.Message;
import activitystreamer.util.Settings;
import activitystreamer.models.*;
import activitystreamer.server.Load;

public class ActivityServerBroadcastThread extends Thread{
    
    private static boolean closeConnection=false;
    private final static Logger log = LogManager.getLogger();
    
    public ActivityServerBroadcastThread() {
        //start();
    }
    
    
    @Override
    public void run() {
        log.info("ActivityServerBroadcastThread is running");
        while(!Control.getInstance().getTerm()){
            //Fetch the latest serverMsgBufferQueue
            HashMap<Connection, ArrayList<Message>> serverMsgBuffQueue = Control.getServerMsgBuffQueue();
            HashMap<Connection, Boolean> serverMsgBuffActivator = Control.getServerMsgBuffActivator();
            if (!serverMsgBuffQueue.isEmpty()){
                //Use iterator to avoid concurrency issues
                Iterator<Entry<Connection, ArrayList<Message>>> it = serverMsgBuffQueue.entrySet().iterator();
                while(it.hasNext()){
                    Entry<Connection, ArrayList<Message>> newEntry = it.next();
                    Connection con = newEntry.getKey();
                    if (!(serverMsgBuffActivator.get(con)==null)&&serverMsgBuffActivator.get(con)){
                        ArrayList<Message> targetList = serverMsgBuffQueue.get(con);
                        if((!(targetList == null))&&(!targetList.isEmpty())){
                            //Waiting for acknowledgment, deactivate sending messages
                            Control.getInstance().deactivateMessageQueue(con);
                            //Broadcast the first message
                            Message msg = targetList.get(0);
                            String broadMsg = Command.createActivityServerBroadcast(msg);
                            log.info("Sending Activity_broadcast message" + msg);
                            con.writeMsg(broadMsg);
                        }
                        
                    }
                }
            }
        }
        
        log.info("closing Activity Server Broadcast thread....");
        closeConnection = true;
      
    }
    
    public static boolean getResponse() {
        return closeConnection;
    }
}

