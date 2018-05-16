package activitystreamer.server;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import activitystreamer.server.commands.*;
import activitystreamer.util.Settings;
import activitystreamer.models.*;

public class Control extends Thread {

    protected static final Logger log = LogManager.getLogger();
    //The connections list will record all connections to this server
    protected static ArrayList<Connection> connections;
    private static ArrayList<Connection> connectionClients;
    //This hashmap is to record status of each connection
    protected static HashMap<String, ArrayList<String>> connectionServers;
    private static ArrayList<Connection> neighbors;
    private static ArrayList<String> pendingNeighbors;
    //This userlist is to store all user information locally in memory
    private static ArrayList<User> localUserList;
    //Save the list of connected users plus timestamp and its connections
    private static HashMap<Connection, String> userConnections = new HashMap<Connection, String>();
    //This registerList is a pending list for all registration applications
    private static HashMap<Connection, User> registerPendingList;
    private static String uniqueId; // unique id for a server
    protected static boolean term = false;
    private static Listener listener;
    //Create a pending queue for each neighbor to serve as message buffer
    private static HashMap<Connection, ArrayList<Message>> serverMsgBuffQueue;
    //String will be like "timestamp,senderId,senderPort"
    private static HashMap<Connection, String> serverMsgAckQueue;

    protected static Control control = null;
    protected static Load serverLoad;
    
    
    public static Load getServerLoad() {
        return serverLoad;
    }

    public static Control getInstance() {
        if (control == null) {
            control = new Control();
        }
        return control;
    }

    public Control() {
        // initialize the connections array
        connections = new ArrayList<Connection>();
        connectionClients = new ArrayList<Connection>();
        //connectionServers is to record all replies from its neighbors and record all connection status.
        connectionServers = new HashMap<String, ArrayList<String>>();
        neighbors = new ArrayList<Connection>();
        pendingNeighbors = new ArrayList<String>();
        localUserList = new ArrayList<User>();
        registerPendingList = new HashMap<Connection, User>();
        serverLoad = new Load();
        uniqueId = Settings.getLocalHostname() + " " + Settings.getLocalPort();
        serverMsgBuffQueue = new HashMap<Connection, ArrayList<Message>>();
        serverMsgAckQueue = new HashMap<Connection, String>();
        // start a listener
        try {
            listener = new Listener();
            initiateConnection();
        } catch (IOException e1) {
            log.fatal("failed to startup a listening thread: " + e1);
            System.exit(-1);
        }
    }

    public void initiateConnection() {
        // make a connection to another server if remote hostname is supplied
        if (Settings.getRemoteHostname() != null) {
            createServerConnection(Settings.getRemoteHostname(), Settings.getRemotePort());
        }
        // start server announce
        new ServerAnnounce();

    }
    public synchronized HashMap<Connection, String> getUserConnections(){
        return userConnections;
    }
    
    public synchronized void updateAckQueue(long timestamp,String senderIp, int senderPort, Connection con){
        serverMsgAckQueue.put(con, timestamp + " " +senderIp + " " + senderPort);
    }
    
    public synchronized boolean checkAckQueue(long timestamp,String senderIp, int senderPort, Connection con){
        String latestMsg = serverMsgAckQueue.get(con);
        String currMsg = timestamp + " " +senderIp + " " + senderPort;
        if (latestMsg.equals(currMsg)){
            return false;
        }
        return true;
    }
    
    //Add the message into the buffer queue
    public synchronized boolean addMessageToBufferQueue(Message ackMsg, Connection con){
        ArrayList<Message> targetList = serverMsgBuffQueue.get(con);
        if (targetList != null){
            targetList.add(ackMsg);
            return true;
        } 
        return false;
    }
    
    
    public synchronized boolean removeMessageFromBufferQueue(long timestamp, String senderIp, int senderPort, Connection con){
        ArrayList<Message> targetList = serverMsgBuffQueue.get(con);
        try{
            if (targetList != null){
                for (Message msg:targetList ){
                    if ((msg.getTimeStamp()== timestamp)&&(msg.getSenderIp().equals(senderIp))
                         && (msg.getPortNum()==senderPort)){
                        targetList.remove(msg);
                        return true;
                    }
                }
                return false;
            } 
        }
        catch (NoSuchElementException e){
            log.error("Fail to remove the message. " + e.toString());
        }
        return false;
    }
    
    public synchronized void sendBufferedMsg(Connection con){
        ArrayList<Message> messageList = serverMsgBuffQueue.get(con);
        for(Message bufferedMsg: messageList){
            String actMsg = Command.createActivityServerBroadcast(bufferedMsg);
            con.writeMsg(actMsg);
        }
    }
    
    public synchronized void createServerConnection(String hostname, int port) {
        try {
                Connection con = outgoingConnection(new Socket(hostname, port));
                JSONObject authenticate = Command.createAuthenticate(Settings.getSecret(), uniqueId);
                String remoteId;
                if(hostname == null){
                    remoteId = "localhost" + " " + port;
                }else{
                    remoteId = hostname + " " + port;
                }
                pendingNeighbors.add(remoteId);
                con.writeMsg(authenticate.toJSONString());
                
            } catch (IOException e) {
                log.error("failed to make connection to " + Settings.getRemoteHostname() + ":" + Settings.getRemotePort() + " :" + e);
                System.exit(-1);
            }
    }

    /*
	 * Processing incoming messages from the connection.
	 * Return true if the connection should close.
     */
    public synchronized boolean process(Connection con, String msg) {
        System.out.println("\n**Receiving: " + msg);
        try {
            JSONParser parser = new JSONParser();
            JSONObject userInput = (JSONObject) parser.parse(msg);
            
            //If the userInput does not have a field for command, it will invoke an invalid message:
            if(!userInput.containsKey("command")){
                String invalidFieldMsg = Command.createInvalidMessage("the received message did not contain a command");
                con.writeMsg(invalidFieldMsg);
                return true;
            }
            else{
                String targetCommand = userInput.get("command").toString();
                if(!Command.contains(targetCommand)){
                    String invalidCommandMsg = Command.createInvalidMessage("the received message did not contain a valid command");
                    con.writeMsg(invalidCommandMsg);
                    return true;
                }
                else{
                    Command userCommand = Command.valueOf(targetCommand);
                    switch (userCommand) {
                        //In any case, if it returns true, it closes the connection.
                        //In any case, we should first check whether it is a valid message format
                        case AUTHENTICATE:
                            if (!Command.checkValidAuthenticate(userInput)){
                                String invalidAuth = Command.createInvalidMessage("Invalid Authenticate Message Format");
                                con.writeMsg(invalidAuth);
                                return true;
                            }
                            else{
                                log.debug(connectionServers.toString());
                                Authenticate auth = new Authenticate(msg, con);
                                if (!auth.getResponse()) {
                                    // Set remoteId for this connection
                                    String remoteId = (String)userInput.get("remoteId");
                                    con.setRemoteId(remoteId);
                                    // Send all neighbors information to the new server
                                    ArrayList<String> neighborInfo = new ArrayList<String>();
                                    
                                    for(Connection nei : neighbors){
                                        neighborInfo.add( nei.getRemoteId());                                            
                                    }
                                    String respondMsg = Command.createAuthenticateSuccess(neighborInfo, uniqueId);
                                    log.debug("Respond to authentication with message " + respondMsg);
                                    con.writeMsg(respondMsg);
                                    
                                    connectionServers.put(remoteId, new ArrayList<String>());
                                    neighbors.add(con);
                                    if (serverMsgBuffQueue.containsKey(con)){
                                        //We need to send all the buffered messages
                                        sendBufferedMsg(con);  
                                    }
                                    else{
                                        //Initialize message queue
                                        serverMsgBuffQueue.put(con, new ArrayList<Message>());
                                        serverMsgAckQueue.put(con, null);
                                    }
                                    log.debug("Add neighbor: " + con.getRemoteId());
                                }
                                return auth.getResponse();
                            }
                        case AUTHENTICATION_SUCCESS:
                            log.debug("Receive authentication sucess");
                            if (!Command.checkValidCommandFormat2(userInput)){
                                String invalidAuth = Command.createInvalidMessage("Invalid Authenticate Success Message Format");
                                con.writeMsg(invalidAuth);                                
                            }                                        
                            else{
                                // Set remoteId for this connection
                                String remoteId = (String)userInput.get("remoteId");
                                con.setRemoteId(remoteId);
                                // Add connection into neighbor list
                                connectionServers.put(remoteId, new ArrayList<String>());
                                neighbors.add(con);
                                //First, we need to check whether this connection is an old connection or not
                                if (serverMsgBuffQueue.containsKey(con)){
                                    //We need to send all the buffered messages
                                    sendBufferedMsg(con);  
                                }
                                else{
                                    //Initialize message queue
                                    serverMsgBuffQueue.put(con, new ArrayList<Message>());
                                    serverMsgAckQueue.put(con, null);
                                }
                                pendingNeighbors.remove(remoteId);
                                log.debug("Add neighbor: " + con.getRemoteId());
                                
                                
                                // creat full connection
                                ArrayList<String> neighborInfo = (ArrayList<String>)userInput.get("info");
                                for(String neiId : neighborInfo){
                                    String[] neiDetail = neiId.split(" ");
                                    if (!(this.containsServer(neiId) || pendingNeighbors.contains(neiId))){
                                        String hostname = neiDetail[0];
                                        Integer port = Integer.parseInt(neiDetail[1]);
                                        log.debug("Send connection request to " + hostname + ", port " + port); 
                                        createServerConnection(hostname, port);
                                            
                                    }
                                }
                                
                                return false;
                            }
                            return true;
                        case AUTHENTICATION_FAIL:
                            String remoteId = (String)userInput.get("remoteId");
                            if (!Command.checkValidCommandFormat2(userInput)){
                                String invalidAuth = Command.createInvalidMessage("Invalid Authenticate Fail Message Format");
                                con.writeMsg(invalidAuth);
                            }
                            if (!remoteId.isEmpty()){
                                if (!pendingNeighbors.contains(remoteId)) {
                                    String invalidServer = Command.createInvalidMessage("Authentication Fail Message "
                                            + "From Invalid Server");
                                    con.writeMsg(invalidServer);                                
                                }
                                else{
                                    pendingNeighbors.remove(remoteId);
                                    log.debug("Authentication failed and server is removed.");
                                }
                            }                            
                            return true;
        
                        case SERVER_ANNOUNCE:
                            if (!Command.checkValidServerAnnounce(userInput)){
                                String invalidServer = Command.createInvalidMessage("Invalid ServerAnnounce Message Format");
                                con.writeMsg(invalidServer);
                                return true;
                            }
                            else if (!this.containsServer(con.getRemoteId())) {
                                String invalidServer = Command.createInvalidMessage("The server has not"
                                        + "been authenticated.");
                                con.writeMsg(invalidServer);
                                return true;
                            }
                            // Record the load infomation
                            serverLoad.updateLoad(userInput);
                            return false;
        
                        case REGISTER:
                            if (!Command.checkValidCommandFormat1(userInput)){
                                String invalidReg = Command.createInvalidMessage("Invalid Register Message Format");
                                con.writeMsg(invalidReg);
                                return true;
                            }
                            else{
                                Register reg = new Register(msg, con);
                                return reg.getCloseCon();
                            }
                            
                        case REGISTER_SUCCESS_BROADCAST:
                            if (!Command.checkValidCommandFormat1(userInput)){
                                String invalidReg = Command.createInvalidMessage("Invalid Register_Success_Broadcast Message Format");
                                con.writeMsg(invalidReg);
                                return true;
                            }
                            else{
                                RegisterSuccessBroadcast reg = new RegisterSuccessBroadcast(msg, con);
                                return reg.getCloseCon();
                            }
        
                        case LOCK_REQUEST:
                            if (!Command.checkValidCommandFormat1(userInput)){
                                String invalidLoc = Command.createInvalidMessage("Invalid LockRequest Message Format");
                                con.writeMsg(invalidLoc);
                                return true;
                            }
                            else{
                                Lock lock = new Lock(msg, con);
                                return lock.getCloseCon();
                            }
        
                        case LOCK_DENIED:
                            if (!Command.checkValidCommandFormat1(userInput)){
                                String invalidLocD = Command.createInvalidMessage("Invalid LockDenied Message Format");
                                con.writeMsg(invalidLocD);
                                return true;
                            }
                            else{
                                LockDenied lockDenied = new LockDenied(msg, con);
                                return lockDenied.getCloseCon();
                            }
        
                        case LOCK_ALLOWED:
                            if (!Command.checkValidCommandFormat1(userInput)){
                                String invalidLocA = Command.createInvalidMessage("Invalid LockAllowed Message Format");
                                con.writeMsg(invalidLocA);
                                return true;
                            }
                            else{
                                LockAllowed lockAllowed = new LockAllowed(msg, con);
                                return lockAllowed.getCloseCon();
                            }
        
                        case LOGIN:
                            if (!Command.checkValidCommandFormat1(userInput)&&!(Command.checkValidAnonyLogin(userInput))){
                                String invalidLogin = Command.createInvalidMessage("Invalid Login Message Format");
                                con.writeMsg(invalidLogin);
                                return true;
                            }
                            else{
                                Login login = new Login(con, msg);
                                // If login success, check if client need redirecting
                                if(!login.getResponse()){
                                    return serverLoad.checkRedirect(con);                                   
                                }
                                return true;
                            }
                        case LOGOUT:
                            if (!Command.checkValidLogout(userInput)){
                                String invalidLogout = Command.createInvalidMessage("Invalid Login Message Format");
                                con.writeMsg(invalidLogout);
                                return true;
                            }
                            else{
                                Logout logout = new Logout(con, msg);
                                return logout.getResponse();
                            }
                            
                        case ACTIVITY_MESSAGE:
                            if (!Command.checkValidAcitivityMessage(userInput)){
                                String invalidAc = Command.createInvalidMessage("Invalid ActivityMessage Message Format");
                                con.writeMsg(invalidAc);
                                return true;
                            }
                            else{
                                ActivityMessage actMess = new ActivityMessage(con, msg);
                                return actMess.getResponse();
                            }
                            
                        case ACTIVITY_BROADCAST:
                            if (!Command.checkValidActivityBroadcast(userInput)){
                                String invalidAc = Command.createInvalidMessage("Invalid ActivityBroadcast Message Format");
                                con.writeMsg(invalidAc);
                                return true;
                            }
                            else{
                                ActivityBroadcast actBroad = new ActivityBroadcast(con, msg);
                                return actBroad.getResponse();
                            }
                        case ACTIVITY_ACKNOWLEDGEMENT:
                            if (!Command.checkValidActivityAcknowledgment(userInput)){
                                String invalidAc = Command.createInvalidMessage("Invalid ActivityAcknowledgement Message Format");
                                con.writeMsg(invalidAc);
                                return true;
                            }
                            else{
                                ActivityAcknowledgement actAck = new ActivityAcknowledgement(con, msg);
                                return actAck.getResponse();
                            }
                            
                        case INVALID_MESSAGE:
                            //First, check its informarion format
                            if (!Command.checkValidCommandFormat2(userInput)){
                                String invalidMsg = Command.createInvalidMessage("Invalid InvalidMessage Message Format");
                                con.writeMsg(invalidMsg);
                                return true;
                            }
                            log.info("Receive response: " + Command.getInvalidMessage(userInput));
                            return true;
                            
                        //If it receives the message other than the command above, it will return an invalid message and close the connection
                        default:
                            String invalidMsg = Command.createInvalidMessage("the received message did not contain a correct command");
                            con.writeMsg(invalidMsg);
                            return true;
                        
                        }
                    }
                }

        }  
        catch (ParseException e) {
            //If parseError occurs, the server should return an invalid message and close the connection
            String invalidParseMsg = Command.createInvalidMessage("JSON parse error while parsing message");
            con.writeMsg(invalidParseMsg);
            log.error("msg: " + msg + " has error: " + e);
        }
        return true;
    }

    
    public synchronized boolean containsServer(String remoteId) {
        return connectionServers.containsKey(remoteId);
    }
    
  //Given a username and secret, check whether it is correct in this server's local userlist.
    public synchronized boolean checkLocalUserAndSecret(String username,String secret){
        for(User user:localUserList ){
            if (user.getUsername().equals(username)&&user.getSecret().equals(secret)){
                return true;
            }
        } 
        return false;
    }

    //Add a user to the pending list
    public synchronized void addUserToRegistePendingList(String username, String secret, Connection con) {
        User pendingUser = new User(username, secret);
        registerPendingList.put(con, pendingUser);

    }

    //Check whether this user is in this pending list. If so, write messages
    public synchronized boolean changeInPendingList(String username, String secret) {
        Iterator<Map.Entry<Connection, User>> it = registerPendingList.entrySet().iterator();
        User targetUser = new User(username, secret);
        while (it.hasNext()) {
            Map.Entry<Connection, User> mentry = it.next();
            Connection con = mentry.getKey();
            User user = mentry.getValue();
            if (user.equals(targetUser)) {
                JSONObject response = Command.createRegisterSuccess(username);
                registerPendingList.remove(con);
                con.writeMsg(response.toJSONString());
                // check if it needs redirect
                if(serverLoad.checkRedirect(con)){
                    // Close connection
                    connectionClosed(con);
                    con.closeCon();
                };              
                return true;
            }
        }
        return false;
    }

    //Check whether this user is in this pending list, if so, delete it and write messages
    public synchronized boolean deleteFromPendingList(String username, String secret) {
        Iterator<Map.Entry<Connection, User>> it = registerPendingList.entrySet().iterator();
        User targetUser = new User(username, secret);
        while (it.hasNext()) {
            Map.Entry<Connection, User> mentry = it.next();
            Connection con = mentry.getKey();
            User user = mentry.getValue();
            if (user.equals(targetUser)) {
                JSONObject response = Command.createRegisterFailed(username);
                con.writeMsg(response.toJSONString());
                registerPendingList.remove(con);
                con.closeCon();
                return true;
            }
        }
        return false;
    }

    //Given a username, check whether it is in this server's local userlist.
    public synchronized boolean checkLocalUser(String username) {
        for (User user : localUserList) {
            if (user.getUsername().equals(username)) {
                return true;
            }
        }
        return false;
    }

    //This function is to add a user to the local userList
    public synchronized void addLocalUser(String username, String secret) {
        User userToAdd = new User(username, secret);
        log.info(username + " " + secret);
        localUserList.add(userToAdd);
    }

    //This function is to delete a user to the local userList
    public synchronized void deleteLocalUser(String username, String secret) {
        for(User user:localUserList){
            if (user.getUsername().equals(username)){
                localUserList.remove(user);
            }
        }
    }

    public synchronized boolean checkAllLocks(Connection con, String msg) {
        try {
            JSONParser parser = new JSONParser();
            JSONObject message = (JSONObject) parser.parse(msg);
            String command = message.get("command").toString();
            String username = message.get("username").toString();
            //Step 1, add this string to the connectionservers list
            for (String remoteId : connectionServers.keySet()) {
                if (remoteId.equals(con.getRemoteId())) {
                    connectionServers.get(remoteId).add(command + " " + username);
                }
                log.debug("Updated hasmap, Con:" + con.getSocket() + " Value:" + connectionServers.get(remoteId));
                
            }
            //Step 2, check whether all the connection return a lock allowed/lock_request regarding to this user
            for (String remoteId : connectionServers.keySet()){
                if (!(connectionServers.get(remoteId).contains("LOCK_ALLOWED " + username) || 
                        connectionServers.get(remoteId).contains("LOCK_REQUEST " + username))){
                    return false;
                }
            }
            return true;
            
        } catch (ParseException e) {
            log.error(e);
        }
        return false;
    }

    public synchronized boolean broadcast(String msg) {     
        for (Connection nei : neighbors) {
            nei.writeMsg(msg);               
        }    
        // need failure model
        return true;
    }

    /*
	 * The connection has been closed by the other party.
     */
    public synchronized void connectionClosed(Connection con) {
        if (!term) {
            connections.remove(con);
            getConnectionClients().remove(con);
            neighbors.remove(con);
            connectionServers.remove(con.getRemoteId());
            registerPendingList.remove(con);
        }
    }

    /*
	 * A new incoming connection has been established, and a reference is returned to it
     */
    public synchronized Connection incomingConnection(Socket s) throws IOException {
        //log.debug("incomming connection: " + Settings.socketAddress(s));
        Connection c = new Connection(s);
        connections.add(c);
        return c;
    }

    /*
	 * A new outgoing connection has been established, and a reference is returned to it
     */
    public synchronized Connection outgoingConnection(Socket s) throws IOException {
        //log.debug("outgoing connection: " + s.toString());
        Connection c = new Connection(s);
        connections.add(c);
        return c;

    }

    public final void setTerm(boolean t) {
        term = t;
    }
    public final boolean getTerm() {
    	return term;
    }

    public synchronized final ArrayList<Connection> getConnections() {
        return connections;
    }

    public HashMap<String, ArrayList<String>> getConnectionServers() {
        return connectionServers;
    }

    public synchronized static String getUniqueId() {
        return uniqueId;
    }
    
    public void listenAgain() {
    	listener.setTerm(true);
    }

	public static ArrayList<Connection> getConnectionClients() {
		return connectionClients;
	}

	public static void setConnectionClients(Connection con) {
		log.debug("adding connection client: "+con);
		connectionClients.add(con);
	}

}
