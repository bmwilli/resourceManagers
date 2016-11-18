// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.ibm.streams.resourcemgr.mesos;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.streams.resourcemgr.ClientInfo;
import com.ibm.streams.resourcemgr.ResourceManagerException;
import com.ibm.streams.resourcemgr.ResourceTags;

/**
 * Contains the state of the Resource manager
 * 
 * Future: Primary interface with ResourcePersistenceManager
 *
 */

public class StreamsMesosState {

	private static final Logger LOG = LoggerFactory.getLogger(StreamsMesosState.class);
	
	private StreamsMesosResourceManager _manager;

	/* StreamsMesosResource containers */

	// allResources: Tracks all resources no matter what state (e.g. requested,
	// running, etc.)
	// indexed by id that we generate
	// Using concurrentHashMap.  simplifies concurrency between the threads of the this class and the mesos scheduler
	private Map<String, StreamsMesosResource> _allResources;
	
	// newRequests: Tracks new requests from Streams and checked by scheduler
	// when new offers arrive
	// Using concurrent CopyOnWriteArrayList.  It is slower than ArrayList but at the rate that 
	// we add/remove resources and requests it is more than fast enough and simplifies concurrency
	private List<StreamsMesosResource> _newRequests;
	
	// pendingRequests: Tracks requests that have been reported back to Streams as PENDING
	private List<StreamsMesosResource> _pendingRequests;

	// Streams Client Information
	// NOTE: We only handle a single Controller client at this time
	private ClientInfo _clientInfo;
	
	/**
	 * Constructor
	 */
	public StreamsMesosState(StreamsMesosResourceManager manager) {
		_manager = manager;
		_allResources = new ConcurrentHashMap<String, StreamsMesosResource>();
		_newRequests = new CopyOnWriteArrayList<StreamsMesosResource>();
		_pendingRequests = new CopyOnWriteArrayList<StreamsMesosResource>();
		_clientInfo = null;

	}
	
	
	
	// Create a new SMR and put it proper containers
	synchronized public StreamsMesosResource createNewResource(String domainId, String zk, ResourceTags tags, boolean isMaster, List<Protos.CommandInfo.URI> uriList) throws ResourceManagerException {
		// Create the Resource object (default state is NEW)
		StreamsMesosResource smr = new StreamsMesosResource(Utils.generateNextId("smr"), domainId, zk, _manager.getArgsMap(),
				uriList);

		smr.setMaster(isMaster);
		
		// Set resource needs (Need to integrate with tags soon)
		double memory = StreamsMesosConstants.RM_MEMORY_DEFAULT;
		double cores = StreamsMesosConstants.RM_CORES_DEFAULT;

		if (Utils.hasProperty(_manager.getProps(), StreamsMesosConstants.PROPS_DC_MEMORY))
			memory = Utils.getDoubleProperty(_manager.getProps(), StreamsMesosConstants.PROPS_DC_MEMORY);

		if (Utils.hasProperty(_manager.getProps(), StreamsMesosConstants.PROPS_DC_CORES))
			cores = Utils.getDoubleProperty(_manager.getProps(), StreamsMesosConstants.PROPS_DC_CORES);
		smr.setMemory(memory);
		smr.setCpu(cores);

		// Set the resource tags
		if (tags != null) {
			_manager.convertTags(tags, smr); // may set mem/cpu from the tags if they are specified
			smr.getTags().addAll(tags.getNames());
		}
		
		
		// Add to collections to track
		LOG.info("Queuing new Resource Request: " + smr.toString());
		_newRequests.add(smr);
		_allResources.put(smr.getId(), smr);

		return smr;
	}
	
	public Map<String, StreamsMesosResource> getAllResources() {
		return _allResources;
	}
	
	// Return list of new Reqeusts as an immutable list
	public List<StreamsMesosResource> getNewRequestList() {
		return _newRequests;
	}
	
	public StreamsMesosResource getResource(String resourceId) {
		if (_allResources.containsKey(resourceId)) {
			return _allResources.get(resourceId);
		} else {
			LOG.warn("getResource from state failed: Resource Not found (id: " + resourceId  + ")");
			return null;
		}
	}
	
	public StreamsMesosResource getResourceByTaskId(String taskId) {
		for (StreamsMesosResource smr : _allResources.values()) {
			if (smr.isAllocated()) {
				if (smr.getTaskId().equals(taskId)) {
					return smr;
				} 
			}
		}
		// Not found
		LOG.warn("getREsourceByTaskId from state failed: Resource Not found.  May not be Launched yet. (TaskID: " + taskId + ")");
		return null;
	}
	
	
	public void taskLaunched(String resourceId) {
		StreamsMesosResource smr = getResource(resourceId);
		if (smr != null) {
			smr.setResourceState(StreamsMesosResource.ResourceState.LAUNCHED);
			smr.setTaskCompletionStatus(StreamsMesosResource.TaskCompletionStatus.NONE);
		} else {
			LOG.warn("taskLaunched from state failed to find resource (id: " + resourceId + ")");
		}

	}
	
	private void mapAndUpdateMesosTaskStateToResourceState(StreamsMesosResource smr, Protos.TaskState taskState) {
		// Handle the mapping and interpretation of mesos status update
		
		StreamsMesosResource.ResourceState newResourceState = null;
		StreamsMesosResource.TaskCompletionStatus newTaskCompletionStatus = null;
		
		
		switch (taskState) {
		case TASK_STAGING:
		case TASK_STARTING:
			newResourceState = StreamsMesosResource.ResourceState.LAUNCHED;
			newTaskCompletionStatus = StreamsMesosResource.TaskCompletionStatus.NONE;
			break;
		case TASK_RUNNING:
			newResourceState = StreamsMesosResource.ResourceState.RUNNING;
			newTaskCompletionStatus = StreamsMesosResource.TaskCompletionStatus.NONE;
			break;
		case TASK_FINISHED:
			newResourceState = StreamsMesosResource.ResourceState.STOPPED;
			newTaskCompletionStatus = StreamsMesosResource.TaskCompletionStatus.FINISHED;
			break;
		case TASK_ERROR:
			newResourceState = StreamsMesosResource.ResourceState.FAILED;
			newTaskCompletionStatus = StreamsMesosResource.TaskCompletionStatus.ERROR;
			break;	
		case TASK_KILLED:
			newResourceState = StreamsMesosResource.ResourceState.FAILED;
			newTaskCompletionStatus = StreamsMesosResource.TaskCompletionStatus.KILLED;
			break;
		case TASK_LOST:
			newResourceState = StreamsMesosResource.ResourceState.FAILED;
			newTaskCompletionStatus = StreamsMesosResource.TaskCompletionStatus.LOST;
			break;
		case TASK_FAILED:
			newResourceState = StreamsMesosResource.ResourceState.FAILED;
			newTaskCompletionStatus = StreamsMesosResource.TaskCompletionStatus.FAILED;
			break;
		default:
			newResourceState = null;
			newTaskCompletionStatus = null;
			break;
		}
		
		
		if (newResourceState != null) {
			LOG.info("Mesos Task Status Update Mapped: Resource ID: " + smr.getId() + ", Resource State = " + newResourceState.toString() +
					", Task Completion Status = " + newTaskCompletionStatus.toString());
			smr.setResourceState(newResourceState);
			smr.setTaskCompletionStatus(newTaskCompletionStatus);
		} else
			LOG.info("Mesos Task Status Update was not mapped to a Resource State, no action");	
	}
	
	
	
	// Handle the Status updates from Mesos
	public void taskStatusUpdate(TaskStatus taskStatus) {
		LOG.info("_state.taskStatusUpdate()");
		
		StreamsMesosResource.ResourceState oldResourceState, newResourceState;
		StreamsMesosResource.RequestState oldRequestState;
		
		String taskId = taskStatus.getTaskId().getValue();
		
		StreamsMesosResource smr = getResourceByTaskId(taskId);

		if (smr != null) {
			oldResourceState = smr.getResourceState();
			mapAndUpdateMesosTaskStateToResourceState(smr, taskStatus.getState());
			newResourceState = smr.getResourceState();
			oldRequestState = smr.getRequestState();
			
			// What does the update mean for us with regards to:
			// * PENDING vs ALLOCATED
			// * Lists it is on
			// * Notifications to be sent
			// ****** WORK IS HERE!!!
			//if (oldResourceState = )
			
			
			


		} else {
			LOG.warn("taskStatusUpdate from state failed to find resource (TaskID: " + taskId + ")");
			return;
		}
	}
	
	
	
	
	
    /**
     * Returns list of all known client identifiers
     * 
     * @return Collection<String>
     */
    public Collection<String> getClientIds() {
        //return getChildren(getClientsPath());
    	List<String> clientIds = new ArrayList<String>();
    	clientIds.add(_clientInfo.getClientId());
    	
    	return clientIds;
    }

    /**
     * Returns client information
     * 
     * @param clientId - String
     * @return ClientInfo
     */
    public ClientInfo getClientInfo(String clientId) {
        ClientInfo client = null;
        /*
        try {
            // ../clients/<clientId>
            String clientJson = getPathProperty(getClientPath(clientId), "client");
            if (clientJson != null) {
                JSONObject json = JSONObject.parse(clientJson);
                client = new ClientInformation(json);
            }
        } catch (Throwable t) {
            Trace.logError(_manager.fgError(t.getMessage(), t), t);
        }
        */
        
        // Just ensure we are on the same page with the clientId requested
        if (_clientInfo != null) {
        	if (_clientInfo.getClientId().equals(clientId)) {
        		client = _clientInfo;
        	} else {
        		LOG.error("getClientInfo from state failed because clientId(" + clientId + ") does not match the client id we are working with (" + _clientInfo.getClientId() + ").");
        	}
        }

        return client;
    }
    

    /**
     * Set client information to persistence
     * 
     * @param client - ClientInfo
     */
    public void setClientInfo(ClientInfo client) {
    	if (client != null) {
	        if (_clientInfo == null) {
	        	_clientInfo = client;
	        } else if (!(_clientInfo.getClientId().equals(client.getClientId()))) {
	        	LOG.error("setClientInfo on state failed because clientId(" + client.getClientId() + ") does not match the client id already set (" + _clientInfo.getClientId() + ").");
	        } else {
	        	// Setting to the same value so no need to do anything
	        }
    	} else {
    		LOG.warn("setClientInfo on state failed because it was passed null client object");
    	}
	    

    	
    	/*
        try {
            String clientId = client.getClientId();

            // ../clients/<clientId>
            String clientPath = getClientPath(clientId);
            if (_persistence.exists(clientPath)) {
                JSONObject json = ((ClientInformation)client).toJson();
                Properties props = new Properties();
                props.setProperty("client", json.serialize());
                setPath(clientPath, props);
                
                // update requests with new client
                for (SymphonyRequest request : _requests.values()) {
                    request.setClient(client);
                }
            }
        } catch (Throwable t) {
            Trace.logError(_manager.fgError(t.getMessage(), t), t);
        }
        */
    }
	
	
	
}
