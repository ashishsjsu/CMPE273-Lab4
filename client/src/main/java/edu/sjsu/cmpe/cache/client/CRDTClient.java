package edu.sjsu.cmpe.cache.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class CRDTClient {
	
	public ConcurrentHashMap<String, String> putReqStatus = new ConcurrentHashMap<String, String>();
	public ConcurrentHashMap<String, String> getReqStatus = new ConcurrentHashMap<String, String>();
	private ArrayList<DistributedCacheService> serverslist = new ArrayList<DistributedCacheService>();
	
	public void addServer(String serverURL) {
		serverslist.add(new DistributedCacheService(serverURL,this));
	}
	
	
	public void put(long key, String value) {
		for(DistributedCacheService service: serverslist) {
			service.put(key, value);
		}
		
		while(true) {
        	if(putReqStatus.size() < 3) {
        		try {
        			System.out.println("Waiting for all put request to get processed...");
					Thread.sleep(500);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
        	} else{
        		int failed = 0;
        		int success = 0;
        		for(DistributedCacheService service: serverslist) {
        			System.out.println("put status for : "+service.getCacheServerURL()+": "+putReqStatus.get(service.getCacheServerURL()));
        			if(putReqStatus.get(service.getCacheServerURL()).equalsIgnoreCase("fail")) 
            			failed++;
            		else
            			success++;
        		}
        		
        		if(failed > 1) {
        			System.out.println("Rolling back put operations on all servers");
        			for(DistributedCacheService service: serverslist) {
        				service.delete(key);
        			}
        		} else {
        			System.out.println("Successfully updated servers");
        		}
        		putReqStatus.clear();
        		break;
        	}
        }
	}
	
	public String get(long key){
		for(DistributedCacheService service: serverslist) {
			service.get(key);
		}
		
		while(true) {
        	if(getReqStatus.size() < 3) {
        		try {
        			System.out.println("Waiting for all get requests to process...");
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
        	} else{
        		HashMap<String, List<String>> valuesMap = new HashMap<String, List<String>>();
        		for(DistributedCacheService service: serverslist) {
        			if(getReqStatus.get(serviceservice.getCacheServerURL()).equalsIgnoreCase("fail")) 
            			System.out.println("Cannot get value from server: "+service.getCacheServerURL());
            		else {
            			if(valuesMap.containsKey(getReqStatus.get(service.getCacheServerURL()))) {
            				valuesMap.get(getReqStatus.get(service.getCacheServerURL())).add(service.getCacheServerURL());
            			} else {
            				List<String> tempList = new ArrayList<String>();
            				tempList.add(service.getCacheServerURL());
            				valuesMap.put(getReqStatus.get(service.getCacheServerURL()),tempList);
            			}
            		}
        		}
        		
        		if(valuesMap.size() != 1) {
        			System.out.println("Values are not consistent on each server");
        			Iterator<Entry<String, List<String>>> iterator = valuesMap.entrySet().iterator();
        			int majority = 0;
        			String finVal = null;
        			ArrayList <String> updateServer = new ArrayList<String>();
        		    while (iterator.hasNext()) {
        		        Map.Entry<String, List<String>> map = (Map.Entry<String, List<String>>)iterator.next();
        		        if(map.getValue().size() > majority) {
        		        	majority = map.getValue().size();
        		        	finVal = map.getKey();
        		        } else {
        		        	for (String str: map.getValue()){
        		        		updateServer.add(str);
        		        	}
        		        }
        		    }
        		    
        			System.out.println("Updating values to make the servers consistent.");
        			for(String str: updateServer){
        				for(DistributedCacheService service: serverslist) {
            				if(serviceservice.getCacheServerURL().equalsIgnoreCase(str)){
            					System.out.println("Correcting value for server: "+service.getCacheServerURL()+" as: "+ finVal);
            					service.put(key, finVal);
            				}
            			}
        			}
        			getReqStatus.clear();
        			return finVal;
        		} else {
        			System.out.println("Successfully performed get function all serverslist");
        			getReqStatus.clear();
        			return valuesMap.keySet().toArray()[0].toString();
        		}
        	}
        }
		
	}

}
