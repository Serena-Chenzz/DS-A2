package activitystreamer.server;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;
import java.util.Iterator;
import java.util.Map;

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
    //This userlist is to store all user information locally in memory
    private static ArrayList<User> localUserList;
    //This registerList is a pending list for all registration applications
    private static HashMap<Connection, User> registerPendingList;
    private final String uniqueId; // unique id for a server
    protected static boolean term = false;
    private static Listener listener;

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
        uniqueId = UUID.randomUUID().toString();
        //connectionServers is to record all replies from its neighbors and record all connection status.
        connectionServers = new HashMap<String, ArrayList<String>>();
        neighbors = new ArrayList<Connection>();
        localUserList = new ArrayList<User>();
        registerPendingList = new HashMap<Connection, User>();
        serverLoad = new Load();
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
            try {
                Connection con = outgoingConnection(new Socket(Settings.getRemoteHostname(), Settings.getRemotePort()));
                JSONObject authenticate = Command.createAuthenticate(Settings.getSecret());
                con.writeMsg(authenticate.toJSONString());
                connectionServers.put(con.getRemoteId(), new ArrayList<String>());
                neighbors.add(con);
                log.debug("Add neighbor: " + con.getRemoteId());
            } catch (IOException e) {
                log.error("failed to make connection to " + Settings.getRemoteHostname() + ":" + Settings.getRemotePort() + " :" + e);
                System.exit(-1);
            }
        }
        // start server announce
        new ServerAnnounce();

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
                                    connectionServers.put(con.getRemoteId(), new ArrayList<String>());
                                    neighbors.add(con);
                                    log.debug("Add neighbor: " + con.getRemoteId());
                                }
                                return auth.getResponse();
                            }
        
                        case AUTHENTICATION_FAIL:
                            if (!Command.checkValidCommandFormat2(userInput)){
                                String invalidAuth = Command.createInvalidMessage("Invalid AuthenticateFail Message Format");
                                con.writeMsg(invalidAuth);                                
                            }
                            else if (!this.containsServer(con.getRemoteId())) {
                                String invalidServer = Command.createInvalidMessage("The server has not"
                                        + "been authenticated.");
                                con.writeMsg(invalidServer);                                
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
                            // continue to broadcast the receiving server announce
                            broadcast(msg, con.getRemoteId());
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

    public synchronized boolean broadcast(String msg, String excludeConId) {
        // If the message originate from one neighbor, when broadcast,
        // exclude this neighbor from being broadcast
        if (excludeConId.isEmpty()) {
            for (Connection nei : neighbors) {
                nei.writeMsg(msg);
                //log.debug("Broadcast with no exclusion of connection");
                //log.debug("Broadcast message to " + nei.getRemoteId() + " : " + msg);
            }
        } else {
        	
            for (Connection nei : neighbors) {
                if (!(nei.getRemoteId().equals(excludeConId))) {
                    nei.writeMsg(msg);
                    //log.debug("Broadcast exclude: " + excludeConId);
                    //log.debug("Broadcast message to " + nei.getRemoteId() + " : " + msg);
                }
            }
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

    public String getUniqueId() {
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
