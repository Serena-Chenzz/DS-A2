package activitystreamer.server.commands;

import java.util.ArrayList;

import activitystreamer.models.User;
import activitystreamer.server.Control;

public class UserListReview {
	private ArrayList<User> newLocalUserList;
	
	public UserListReview(ArrayList<User> localUserList) {
		try{
			newLocalUserList.clear();
		}catch(Exception e){
			
		}
		
		
		
		//Check for every user coming from the userList message if:
		for(User user : localUserList) {
			//If user exists in current localUserList, then:
			if(Control.getInstance().checkLocalUser(user.getUsername())) {
				//Check if has same secret, add it to newLocalUserList
				if(Control.getInstance().checkLocalUserAndSecret(user.getUsername(), user.getSecret())) {
					newLocalUserList.add(user);
				//If user has different secret, then delete it from localUserList
				}else {
					Control.getInstance().deleteLocalUser(user.getUsername(), user.getSecret());
				}
			//If user does not exist, or user was previously created, then add it to new userList	
			}else {
				newLocalUserList.add(user);
			}
		}
		
		//If there was a new user registered in the network before network fail, add it to the new list too
		//even if it is not in the userList message
		for(User user : Control.getLocalUserList()) {
			if(!newLocalUserList.contains(user)) {
				newLocalUserList.add(user);
			}
		}
		
		//Finally, replace currentLocalUserList for a new one
		try {
			if(newLocalUserList.size()>0) {
				Control.setLocalUserList(newLocalUserList);
			}
		}catch(Exception e) {
			
		}
	}

}
