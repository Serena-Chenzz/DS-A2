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
		for(User user : localUserList) {
			//If user exists in current localUserList 
			if(Control.getInstance().checkLocalUser(user.getUsername())) {
				//Check if user has a different secret than current user, if false then add it to the new list
				if(!Control.getInstance().checkLocalUserAndSecret(user.getUsername(), user.getSecret())) {
					newLocalUserList.add(user);
				}
			//If user does not exist, add it	
			}else {
				newLocalUserList.add(user);
			}
			
		}
		
		try {
			if(newLocalUserList.size()>0) {
				Control.setLocalUserList(newLocalUserList);
			}
		}catch(Exception e) {
			
		}
	}

}
