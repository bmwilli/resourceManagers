package com.ibm.streams.resourcemgr.mesos;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	
	/**
	 * Constructor
	 */
	public StreamsMesosState(StreamsMesosResourceManager manager) {
		_manager = manager;
		_allResources = new ConcurrentHashMap<String, StreamsMesosResource>();
		_newRequests = new CopyOnWriteArrayList<StreamsMesosResource>();
		_pendingRequests = new CopyOnWriteArrayList<StreamsMesosResource>();

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
	
	// Update SMR and maintain lists
	// Eventually pass what to update it with
	synchronized public void updateResourceByTaskId(String taskId, StreamsMesosResource.StreamsMesosResourceState newState) {
		LOG.info("updateResourceByTaskId(" + taskId + ", " + newState.toString());
		LOG.debug("updateResourceByTaskId.allResources: " + _allResources.toString());
		boolean foundMatch = false;
		for (StreamsMesosResource smr : _allResources.values()) {
			if (smr.isAllocated()) {
				if (smr.getTaskId().equals(taskId)) {
					foundMatch = true;
					updateResource(smr.getId(),newState);
				} 
			}
		}
		if (!foundMatch) {
			LOG.warn("Update of Resource by TaskId Failed because SMR Not found (TaskID: " + taskId + ", newstate: " + newState.toString() + ")");
		}
	}
	
	// Update the SMR status 
	// Need to handle these appropriately because we have concurrency issues of iterating in the scheduler
	// and calling this to update the list
	synchronized public void updateResource(String id, StreamsMesosResource.StreamsMesosResourceState newState) {
		LOG.info("updateResource(" + id + ", " + newState.toString());
		LOG.debug("updateResource.allResources: " + _allResources.toString());
		StreamsMesosResource smr = null;
		//StreamsMesosResource.StreamsMesosResourceState oldState;
		// Find it in the list of all StreamsMesosResources
		if (_allResources.containsKey(id)) {
			smr = _allResources.get(id);
			//oldState = smr.getState();
			
			// Update state
			smr.setState(newState);
		} else {
			LOG.warn("Update of Resource Failed because SMR Not found ((id: " + id + ", newstate: " + newState.toString() + ")");
		}

	}
	
}
