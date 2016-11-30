package com.ibm.streams.resourcemgr.mesos.command;

import java.util.Locale;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.ibm.streams.resourcemgr.ClientConnection;
import com.ibm.streams.resourcemgr.ClientConnectionFactory;
import com.ibm.streams.resourcemgr.mesos.StreamsMesosConstants;

public class GetResourceState {
	static public void main(String[] args) {
		String zkConnect = null;
		for (int index = 0; index < args.length; ++index) {
			String arg = args[index];
			if (arg.equals(StreamsMesosConstants.ZK_ARG) && index + 1 < args.length) {
				zkConnect = args[++index];
			}
		}
		
		if (zkConnect == null) {
			zkConnect = System.getenv("STREAMS_ZKCONNECT");
		}
		
		if (zkConnect == null) {
			System.out.println("usage:  --zkconnect <host:port,...>");
		}
		
		ClientConnection client = null;
		try {
			client = ClientConnectionFactory.createConnection("GetResourceStateCommand", StreamsMesosConstants.RESOURCE_TYPE, zkConnect, "StreamsDomain");
			client.connect();
			JSONObject data = new JSONObject();
			data.put(StreamsMesosConstants.CUSTOM_COMMAND, StreamsMesosConstants.CUSTOM_COMMAND_GET_RESOURCE_STATE);
			String response = client.customCommand(data.toString(), Locale.getDefault());
			JSONParser parser = new JSONParser();
			JSONObject responseObj = (JSONObject) parser.parse(response);
			JSONArray resources = (JSONArray)responseObj.get(StreamsMesosConstants.CUSTOM_RESULT_RESOURCES);
			
			String columnHeadings = "\nNative Resource Name\tDisplay Name\tMesos Task ID\tResource State\tRequest State\tTask Completion Status\tHost Name\tMaster";
			System.out.println(columnHeadings);
			for (Object obj : resources.toArray()) {
				JSONObject resource = (JSONObject)obj;
				System.out.println(resource.get(StreamsMesosConstants.CUSTOM_RESULT_RESOURCE_ID) + "\t" +
						resource.get(StreamsMesosConstants.CUSTOM_RESULT_RESOURCE_STREAMS_ID) + "\t" +
						resource.get(StreamsMesosConstants.CUSTOM_RESULT_RESOURCE_TASK_ID) + "\t" +
						resource.get(StreamsMesosConstants.CUSTOM_RESULT_RESOURCE_RESOURCE_STATE) + "\t" +
						resource.get(StreamsMesosConstants.CUSTOM_RESULT_RESOURCE_REQUEST_STATE) + "\t" +
						resource.get(StreamsMesosConstants.CUSTOM_RESULT_RESOURCE_COMPLETION_STATUS) + "\t" +
						resource.get(StreamsMesosConstants.CUSTOM_RESULT_RESOURCE_HOST_NAME) + "\t" +
						resource.get(StreamsMesosConstants.CUSTOM_RESULT_RESOURCE_IS_MASTER) + "\t" 
						);
			}
		} catch (Throwable t) { t.printStackTrace(); }
		finally {
			if (client != null) {
				try {client.close(); } catch (Throwable t) {}
			}
		}
	}

}
