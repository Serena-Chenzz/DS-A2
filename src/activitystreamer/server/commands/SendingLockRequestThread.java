package activitystreamer.server.commands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.ConcurrentModificationException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;

import activitystreamer.server.Connection;
import activitystreamer.server.Control;
import activitystreamer.server.Message;
import activitystreamer.util.Settings;
import activitystreamer.models.*;
import activitystreamer.server.Load;

public class SendingLockRequestThread extends Thread {
    private static boolean closeConnection=false;
    private final static Logger log = LogManager.getLogger();
    
    public void run(){
       //Monitor the lockAckQueue every 5 seconds
        while(!Control.getInstance().getTerm()){
            try{
                HashMap<Connection, HashMap<Long, String>> lockAckQueue = Control.getLockAckQueue();
                if (!lockAckQueue.isEmpty()){
                    for (Connection con: lockAckQueue.keySet()){
                        HashMap<Long, String> targetMap = lockAckQueue.get(con);
                        if (!targetMap.isEmpty()){
                            for(long sendingTime:targetMap.keySet()){
                                long currentTime = System.currentTimeMillis();
                                String message = targetMap.get(sendingTime);
                                if ((!message.equals("Received Ack"))&&(!message.equals("Suspend"))&&(currentTime - sendingTime >= 2000)){
                                    String username = message.split(" ")[0];
                                    String secret = message.split(" ")[1];
                                    //JSONObject lockRequest = Command.createLockRequest(username, secret);
                                    //con.writeMsg(lockRequest.toJSONString());
                                    System.out.println("Changing lock status");
                                    Control.getInstance().changeLockStatus(con, username);
                                    Control.getInstance().setSuspendLockAck(con, username + " " +secret);
                                    if (Control.getInstance().checkAllLocks(username)){
                                        //Writing the user info in local storage
                                          Control.getInstance().addLocalUser(username, secret);
                                          if (Control.getInstance().changeInPendingList(username, secret)){
                                              //If the client is registered in this server, it will return back the message
                                              //Also, broadcast register success message to all other servers
                                              String registerSucMsg = Command.createRegisterSuccessBroadcast(username, secret).toJSONString();
                                              Control.getInstance().broadcast(registerSucMsg);
                                              closeConnection = false;
                                          }
                                      }
                                 }
                             }
                        }
                }
                Thread.sleep(Settings.getActivityInterval());
               }
            }catch(ConcurrentModificationException e){
                log.info("Block iterating arrays when modifying it");
            }catch (InterruptedException e){
                log.info("This thread is interrupted forcefully.");
            }
        }
        log.info("closing sending lockRequest thread....");
        closeConnection=true;
  }
    
    public static boolean getResponse() {
        return closeConnection;
    }
}
