package com.ibm.streams.resourcemgr.mesos.command;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
			// Connect to the Resource Manager
			client = ClientConnectionFactory.createConnection("GetResourceStateCommand", StreamsMesosConstants.RESOURCE_TYPE, zkConnect, "StreamsDomain");
			client.connect();
			
			// Create the command json
			JSONObject data = new JSONObject();
			data.put(StreamsMesosConstants.CUSTOM_COMMAND, StreamsMesosConstants.CUSTOM_COMMAND_GET_RESOURCE_STATE);
			
			// Call the command on the client
			String response = client.customCommand(data.toString(), Locale.getDefault());
			
			// Parse the JSON
			JSONParser parser = new JSONParser();
			JSONObject responseObj = (JSONObject) parser.parse(response);
			JSONArray resources = (JSONArray)responseObj.get(StreamsMesosConstants.CUSTOM_RESULT_RESOURCES);
			
			// Create output table headings
			String colHeadings[] = {"Native Name", "Display Name", "Mesos Task ID", "Resource State", "Request State", "Completion Status", "Host Name", "Is Master"};

			// Get the table body
			List<String[]> tableBody = new ArrayList<String[]>();
			for (Object obj : resources.toArray()) {
				String row[] = new String[colHeadings.length];
				JSONObject resource = (JSONObject)obj;
				row[0] = resource.get(StreamsMesosConstants.CUSTOM_RESULT_RESOURCE_ID).toString();
				row[1] = resource.get(StreamsMesosConstants.CUSTOM_RESULT_RESOURCE_STREAMS_ID).toString();
				row[2] = resource.get(StreamsMesosConstants.CUSTOM_RESULT_RESOURCE_TASK_ID).toString();
				row[3] = resource.get(StreamsMesosConstants.CUSTOM_RESULT_RESOURCE_RESOURCE_STATE).toString();
				row[4] = resource.get(StreamsMesosConstants.CUSTOM_RESULT_RESOURCE_REQUEST_STATE).toString();
				row[5] = resource.get(StreamsMesosConstants.CUSTOM_RESULT_RESOURCE_COMPLETION_STATUS).toString();
				row[6] = resource.get(StreamsMesosConstants.CUSTOM_RESULT_RESOURCE_HOST_NAME).toString();
				row[7] = resource.get(StreamsMesosConstants.CUSTOM_RESULT_RESOURCE_IS_MASTER).toString();
				tableBody.add(row);
			}
			
			// Get width of headings 
			int colWidths[] = new int[colHeadings.length];
			for (int i = 0; i < colHeadings.length; i++) {
				colWidths[i] = colHeadings[i].length();
			}
			
			// Get max widths including body with 
			for (String[] row : tableBody) {
				for (int i = 0; i < colHeadings.length; i++) {
					colWidths[i] = Math.max(colWidths[i], row[i].length());
				}
			}
			
			// Create output format string
			int colGap = 2;
			StringBuffer fmt = new StringBuffer();
			for (int i = 0; i < colHeadings.length; i++) {
				fmt.append("%-" + String.valueOf(colWidths[i] + colGap) + "s");
			}
			fmt.append("\n");
			
			// Output the Table
			System.out.format(fmt.toString(), colHeadings[0], colHeadings[1], colHeadings[2], colHeadings[3], colHeadings[4], colHeadings[5], colHeadings[6], colHeadings[7]);
			
			for (String[] row: tableBody) {
				System.out.format(fmt.toString(), row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]);
			}
			
		
		} catch (Throwable t) { t.printStackTrace(); }
		finally {
			if (client != null) {
				try {client.close(); } catch (Throwable t) {}
			}
		}
	}

}
