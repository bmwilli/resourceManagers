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

import java.io.FileReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Locale;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.FrameworkInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.streams.resourcemgr.AllocateInfo;
import com.ibm.streams.resourcemgr.AllocateInfo.AllocateType;
import com.ibm.streams.resourcemgr.AllocateMasterInfo;
import com.ibm.streams.resourcemgr.ClientInfo;
import com.ibm.streams.resourcemgr.ResourceDescriptor;
import com.ibm.streams.resourcemgr.ResourceDescriptor.ResourceKind;
import com.ibm.streams.resourcemgr.ResourceDescriptorState;
import com.ibm.streams.resourcemgr.ResourceDescriptorState.AllocateState;
import com.ibm.streams.resourcemgr.ResourceException;
import com.ibm.streams.resourcemgr.ResourceManagerAdapter;
import com.ibm.streams.resourcemgr.ResourceManagerException;
import com.ibm.streams.resourcemgr.ResourceManagerUtilities;
import com.ibm.streams.resourcemgr.ResourceManagerUtilities.ResourceManagerPackageType;
import com.ibm.streams.resourcemgr.ResourceTagException;
import com.ibm.streams.resourcemgr.ResourceTags;
import com.ibm.streams.resourcemgr.ResourceTags.TagDefinitionType;

/**
 * @author bmwilli
 *
 */
public class StreamsMesosResourceManager extends ResourceManagerAdapter {

	private static final Logger LOG = LoggerFactory.getLogger(StreamsMesosResourceManager.class);

	private StreamsMesosState _state;
	private Map<String, String> _envs = System.getenv();
	private Map<String, String> _argsMap = new HashMap<String, String>();
	private Properties _props = null;
	private StreamsMesosScheduler _scheduler;
	private MesosSchedulerDriver _driver;
	private List<Protos.CommandInfo.URI> _uriList = new ArrayList<Protos.CommandInfo.URI>();
	private boolean _deployStreams = false;



	/*
	 * Constructor NOTE: Arguments passed are not in a reliable order need to
	 * identify flags you expect and others should be read as key,value
	 * sequential arguments. NOTE: These arguments come from the
	 * streams-on-mesos script and are passed through the StreamsResourceServer
	 * which it executes and which in turn constructs this class
	 */
	public StreamsMesosResourceManager(String[] args) throws StreamsMesosException {
		LOG.debug("Constructing ResourceManagerAdapter: StreamsMesosResourceFramework");
		LOG.debug("args: " + Arrays.toString(args));
		LOG.trace("Enironment: " + _envs);

		_argsMap.clear();
		for (int i = 0; i < args.length; i++) {
			switch (args[i]) {
			case StreamsMesosConstants.DEPLOY_ARG:
				_deployStreams = true;
				_argsMap.put(args[i], "true");
				break;
			case StreamsMesosConstants.MESOS_MASTER_ARG:
			case StreamsMesosConstants.ZK_ARG:
			case StreamsMesosConstants.TYPE_ARG:
			case StreamsMesosConstants.MANAGER_ARG:
			case StreamsMesosConstants.INSTALL_PATH_ARG:
			case StreamsMesosConstants.HOME_DIR_ARG:
			case StreamsMesosConstants.PROP_FILE_ARG:
			case StreamsMesosConstants.DOMAIN_ID_ARG:
				_argsMap.put(args[i], args[++i]);
				break;
			}
		}
		LOG.debug("ArgsMap: " + _argsMap);

		String propsFile = null;
		// Process Properties FileReader
		if (_argsMap.containsKey(StreamsMesosConstants.PROP_FILE_ARG))
			propsFile = _argsMap.get(StreamsMesosConstants.PROP_FILE_ARG);
		else
			propsFile = StreamsMesosConstants.RM_PROPERTIES_FILE;
		LOG.debug("Reading Properties file from: " + propsFile);
		_props = new Properties();
		try {
			_props.load(new FileReader(propsFile));
		} catch (FileNotFoundException e) {
			LOG.error("Could not find properties file: " + propsFile);
			throw new StreamsMesosException("Could not find properties file: " + propsFile, e);
		} catch (IOException e) {
			LOG.error("IO Error reading the properties file: " + propsFile);
			throw new StreamsMesosException("IO Error reading the properties file: " + propsFile, e);
		}

		LOG.debug("Properties file properties: " + _props.toString());

	}

	/**
	 * @return the _state
	 */
	public StreamsMesosState getState() {
		return _state;
	}

	/**
	 * @return the argsMap
	 */
	public Map<String, String> getArgsMap() {
		return _argsMap;
	}

	/**
	 * @param argsMap the argsMap to set
	 */
	public void setArgsMap(Map<String, String> argsMap) {
		this._argsMap = argsMap;
	}

	/**
	 * @return the props
	 */
	public Properties getProps() {
		return _props;
	}

	/**
	 * @param props the props to set
	 */
	public void setProps(Properties props) {
		this._props = props;
	}


	//////////////////////////////////////////////////
	// Streams ResourceManager Implementation
	//////////////////////////////////////////////////

	/*
	 * (non-Javadoc) Initialize Resource Manager when resource server starts
	 *
	 * @see com.ibm.streams.resourcemgr.ResourceManagerAdapter#initialize()
	 */
	@Override
	public void initialize() throws ResourceManagerException {
		LOG.debug("Initialize();");

		_state = new StreamsMesosState(this);
		
		// Provision Streams if necessary
		// Caution, this can take some time and cause timeouts on slow machines
		// or workstations that are overloaded
		// In testing, saw issues where Streams Resource Manager Server would
		// disconnect client.
		if (_argsMap.containsKey(StreamsMesosConstants.DEPLOY_ARG)) {
			try {
				LOG.info("Deploy flag set.  Calling provisionStreams...");
				provisionStreams(_argsMap, _uriList, StreamsMesosConstants.PROVISIONING_SHARED_URI);
			} catch (StreamsMesosException e) {
				LOG.info("Caught error from provisionStreams");
				throw new ResourceManagerException("Initialization of Streams Mesos Failed to provision Streams", e);
			}
		}

		// Temporary, not sure why constant would ever have the master
		String master = StreamsMesosConstants.MESOS_MASTER;
		if (_argsMap.containsKey(StreamsMesosConstants.MESOS_MASTER_ARG))
			master = _argsMap.get(StreamsMesosConstants.MESOS_MASTER_ARG);

		// Setup and register the Mesos Scheduler
		LOG.info("About to call runMesosScheduler...");

		runMesosScheduler(master);

		LOG.info("StreamsMesosResourceFramework.initialize() complete");

	}

	/*
	 * (non-Javadoc) Close/Shutdown/Cleanup Resource Manager when resource
	 * server stops
	 * 
	 * @see com.ibm.streams.resourcemgr.ResourceManagerAdapter#close()
	 */
	@Override
	public void close() {
		LOG.info("StreamsResourceServer called close()");
		LOG.info("stopping the mesos driver...");
		Protos.Status driverStatus = _driver.stop();
		LOG.info("...driver stopped, status: " + driverStatus.toString());
		super.close();
	}
	
    @Override
    public void validateTags(ClientInfo client, ResourceTags tags, Locale locale) throws ResourceTagException,
            ResourceManagerException {
		LOG.info("StreamsResourceServer called validateTags()");
        for (String tag : tags.getNames()) {
            TagDefinitionType definitionType = tags.getTagDefinitionType(tag);
            switch (definitionType) {
                case NONE:
                    //no definition means use defaults
                    break;
                case PROPERTIES:
                    Properties propsDef = tags.getDefinitionAsProperties(tag);
                    for (Object key : propsDef.keySet()) {
                        validateTagAttribute(tag, (String)key, propsDef.get(key));
                    }
                    break;
                default:
                    throw new ResourceTagException("Unexpected properties in definition for tag: " + tag);
            }
        }
    }



	/*
	 * Create master resource. This resource is the first resource requested by
	 * Streams and is requested when the streams domain starts
	 * 
	 * @see
	 * com.ibm.streams.resourcemgr.ResourceManagerAdapter#allocateMasterResource
	 * (com.ibm.streams.resourcemgr.ClientInfo,
	 * com.ibm.streams.resourcemgr.AllocateMasterInfo)
	 */
	@Override
	public ResourceDescriptor allocateMasterResource(ClientInfo clientInfo, AllocateMasterInfo request)
			throws ResourceTagException, ResourceManagerException {
		LOG.info("#################### allocate master start ####################");
		LOG.info("Request: " + request);
		LOG.info("ClientInfo: " + clientInfo);
		
		ResourceDescriptor descriptor = null;
		
		List<ResourceDescriptorState> lst = allocateResources(clientInfo, true, 1, request.getTags(),
				AllocateType.SYNCHRONOUS);
		if (lst.size() == 0)
			throw new ResourceManagerException("Streams Mesos Resource Manager could not allocate master resource");
		
		descriptor =  lst.get(0).getDescriptor();

		LOG.info("#################### allocate master end ####################");

		return descriptor;
	}

	/*
	 * Create non-master resources. These resources are used for Streams
	 * instances
	 * 
	 * @see
	 * com.ibm.streams.resourcemgr.ResourceManagerAdapter#allocateResources(com.
	 * ibm.streams.resourcemgr.ClientInfo,
	 * com.ibm.streams.resourcemgr.AllocateInfo)
	 */
	@Override
	public Collection<ResourceDescriptorState> allocateResources(ClientInfo clientInfo, AllocateInfo request)
			throws ResourceTagException, ResourceManagerException {
		LOG.info("################### allocate start ###################");
		LOG.info("Request: " + request);
		LOG.info("ClientInfo: " + clientInfo);
		
		Collection<ResourceDescriptorState> states = new ArrayList<ResourceDescriptorState>();
		
		states = allocateResources(clientInfo, false, request.getCount(), request.getTags(), request.getType());

		
		LOG.info("################### allocate end ###################");

		return states;

	}
	
	/**
	 * Releases specified resources that are allocated 
	 *
	 * (non-Javadoc)
	 * @see com.ibm.streams.resourcemgr.ResourceManagerAdapter#releaseResources(com.ibm.streams.resourcemgr.ClientInfo, java.util.Collection, java.util.Locale)
	 */
	@Override
	public void releaseResources(ClientInfo client, Collection<ResourceDescriptor> descriptors, Locale locale)
			throws ResourceManagerException {

		LOG.info("################ release specified resources start ################");
		LOG.info("client: " + client);;
		LOG.info("descriptors: " + descriptors);
		LOG.info("locale: " + locale);
		for (ResourceDescriptor rd : descriptors) {
			if (!_state.getAllResources().containsKey(rd.getNativeResourceId()))
				throw new ResourceManagerException("No such resource: " + rd.getNativeResourceId());
			StreamsMesosResource smr = _state.getAllResources().get(rd.getNativeResourceId());
			smr.stop(_scheduler);
		}
		
		LOG.info("################ release specified resources end ################");

	}

	/**
	 * Releases all resources for the given client
	 * NOTE: At this time has not been tested or coded for multiple clients
	 * 
	 * (non-Javadoc)
	 * @see com.ibm.streams.resourcemgr.ResourceManagerAdapter#releaseResources(com.ibm.streams.resourcemgr.ClientInfo, java.util.Locale)
	 */
	@Override
	public void releaseResources(ClientInfo client, Locale locale) throws ResourceManagerException {
		LOG.info("################ release all client resources start ################");
		LOG.info("client: " + client);;
		LOG.info("locale: " + locale);
		for (StreamsMesosResource smr : _state.getAllResources().values()) {
			smr.stop(_scheduler);
		}
		
		LOG.info("################ release all client resources end ################");
	}

	

	/////////////////////////////////////////////
	/// MESOS FRAMEWORK INTEGRATION
	/////////////////////////////////////////////



	private static FrameworkInfo getFrameworkInfo() {
		FrameworkInfo.Builder builder = FrameworkInfo.newBuilder();
		builder.setFailoverTimeout(120000);
		builder.setUser("");
		builder.setName(StreamsMesosConstants.FRAMEWORK_NAME);
		return builder.build();
	}

	private void runMesosScheduler(String mesosMaster) {
		// private void runMesosScheduler(List<CommandInfo.URI>uriList, String
		// mesosMaster) {
		LOG.info("Creating new Mesos Scheduler...");
		// LOG.info("URI List: " + uriList.toString());
		// LOG.info("commandInfo: " + getCommandInfo(uriList));;

		_scheduler = new StreamsMesosScheduler(this);

		LOG.info("Creating new MesosSchedulerDriver...");
		_driver = new MesosSchedulerDriver(_scheduler, getFrameworkInfo(), mesosMaster);

		LOG.info("About to start the mesos scheduler driver...");
		Protos.Status driverStatus = _driver.start();
		LOG.info("...start returned status: " + driverStatus.toString());
	}


	
	///////////////////////////////////////////////
	/// PRIVATE METHODS
	///////////////////////////////////////////////
	
	/** 
	 * @param tags
	 * @param smr
	 * @throws ResourceTagException
	 * @throws ResourceManagerException
	 */
	public void convertTags(ResourceTags tags, StreamsMesosResource smr) throws ResourceTagException, ResourceManagerException {
		int cores = -1;
		int memory = -1;
		
		for (String tag : tags.getNames()) {
			try {
				TagDefinitionType definitionType = tags.getTagDefinitionType(tag);
				
				switch (definitionType) {
				case NONE:
					// use default definition (probably just a name tag (e.g. AUDIT)
					break;
				case PROPERTIES:
					Properties propsDef = tags.getDefinitionAsProperties(tag);
					LOG.trace("Tag=" + tag + " props=" + propsDef.toString());
					if (propsDef.containsKey(StreamsMesosConstants.MEMORY_TAG)) {
						memory = Math.max(memory,  Utils.getIntProperty(propsDef, StreamsMesosConstants.MEMORY_TAG));
						LOG.trace("Tag=" + tag + " memory=" + memory);
					}
					if (propsDef.containsKey(StreamsMesosConstants.CORES_TAG)) {
						cores = Math.max(cores,  Utils.getIntProperty(propsDef, StreamsMesosConstants.CORES_TAG));
						LOG.trace("Tag=" + tag + " cores=" + cores);
					}
					break;
				default:
					throw new ResourceTagException("Tag=" + tag + " has unsupported tag definition type=" + definitionType);
				}
			} catch (ResourceException rs) {
				throw new ResourceManagerException(rs);
			}
		}
		
		// Override memory and cores if they were set by the tags
		if (memory != -1){
			smr.setMemory(memory);
		}
		if (cores != -1) {
			smr.setCpu(cores);
		}
	}
	
    private void validateTagAttribute(String tag, String key, Object valueObj) throws ResourceTagException {
        //memory, cores
        if (key.equals(StreamsMesosConstants.MEMORY_TAG) || key.equals(StreamsMesosConstants.CORES_TAG)) {
            if (valueObj == null) {
                 throw new ResourceTagException("Tag: " + tag + " memory property must not be empty if it is present.");
            } else if (valueObj instanceof String) {
                try {
                    Integer.parseInt(valueObj.toString().trim());
                } catch (NumberFormatException nfe) {
                    throw new ResourceTagException("Tag: " + tag + " memory property must contain a numeric value.");
                }
            } else if (!(valueObj instanceof Long) && !(valueObj instanceof Integer)) {
                throw new ResourceTagException("Tag: " + tag + " memory property must contain a numeric value.");
            }
        } else {
            throw new ResourceTagException("Tag: " + tag + " contains an unsupported attribute");
        }
    }
    
	/*
	 * Create resources helper for both master and regular resources
	 *
	 */
	private List<ResourceDescriptorState> allocateResources(ClientInfo clientInfo, boolean isMaster, int count,
			ResourceTags tags, AllocateType rType) throws ResourceManagerException, ResourceTagException {

		List<StreamsMesosResource> newRequestsFromStreams = new ArrayList<StreamsMesosResource>();

		for (int i = 0; i < count; i++) {
			// Creates new Resource, queues, and adds to map of all resources
			StreamsMesosResource smr = _state.createNewResource(_argsMap.get(StreamsMesosConstants.DOMAIN_ID_ARG),
					_argsMap.get(StreamsMesosConstants.ZK_ARG), tags, isMaster, _uriList);
			// Put it in our local list to wait a little bit of time to see if it gets started
			newRequestsFromStreams.add(smr);
		}

		return waitForAllocation(newRequestsFromStreams, rType);

	}

	/**
	 * Attempt to wait a little bit to see if we can get resources allocated
	 * Note: If streams is being deployed to the containers, that can take a
	 * while.
	 * 
	 * @param newAllocationRequests
	 * @param rType
	 * @return
	 */
	private List<ResourceDescriptorState> waitForAllocation(List<StreamsMesosResource> newAllocationRequests,
			AllocateType rType) {
		// Depending on the type (Synchronous, Asynchronous, Flexible, etc...)
		// figure out how long to wait
		int waitTimeSecs = getWaitSecs(rType, _props);
		LOG.info("Waiting for the Streams Mesos Scheduler to allocate and run " + newAllocationRequests.size() + " resources, maxTime: "
				+ (waitTimeSecs < 0 ? "unbounded" : waitTimeSecs));
		long endTime = System.currentTimeMillis() + (waitTimeSecs * 1000);

		// Wait and poll to see if any are allocated in the given time
		int allocCount = 0;
		while (waitTimeSecs < 0 || System.currentTimeMillis() < endTime) {
			synchronized (this) {
				LOG.trace("Polling the new requests...");
				for (StreamsMesosResource smr : newAllocationRequests) {
					LOG.trace("smr {id: " + smr.getId() + ", state: " + smr.getState().toString() + "}");
					if (smr.isRunning()) {
						LOG.info("Resource is now running: " + smr);
						allocCount++;
					}
				}
				LOG.trace("Allocated Count: " + allocCount);
				if (allocCount == newAllocationRequests.size()) {		
					// We have them all, no need to continue to wait
					LOG.info("Allocated Count = # new allocation requests (" + newAllocationRequests.size() + "), stop waiting and polling");
					break;
				}
			}
			LOG.trace("...waiting");
			Utils.sleepABit(StreamsMesosConstants.SLEEP_UNIT_MILLIS);
		}
		LOG.info("Finished waiting for new requests: allocated " + allocCount + " of " + newAllocationRequests.size() + " resources");
		if (allocCount < newAllocationRequests.size()) {
			LOG.info("Some did not get allocated, *** NEED TO IMPLEMENT PENDING ***");
		}
		// We have waited long enough
		synchronized (this) {
			List<ResourceDescriptorState> descriptorStates = new ArrayList<ResourceDescriptorState>();
			for (StreamsMesosResource smr : newAllocationRequests) {
				descriptorStates.add(smr.getDescriptorState());
				//toNotifyList.add(smr.getDescriptor().getNativeResourceId());
			}
			LOG.info("Returning descriptorStates: " + descriptorStates);
			return descriptorStates;
		}
	}

	/**
	 * Lookup for how long to wait before reporting back to Streams the
	 * allocation request is complete or pending
	 * 
	 * @param rType
	 * @param props
	 * @return
	 */
	static private int getWaitSecs(AllocateType rType, Properties props) {
		switch (rType) {
		case SYNCHRONOUS:
			return Utils.getIntProperty(props, StreamsMesosConstants.PROPS_WAIT_SYNC);
		case ASYNCHRONOUS:
			return Utils.getIntProperty(props, StreamsMesosConstants.PROPS_WAIT_ASYNC);
		case FLEXIBLE:
			return Utils.getIntProperty(props, StreamsMesosConstants.PROPS_WAIT_FLEXIBLE);
		default:
			throw new RuntimeException("Unhandled Streams AllocateType: " + rType);
		}
	}
	
	///////////////////////////////////////////////
	// STREAMS PROVISIONING METHODS
	///////////////////////////////////////////////
	
	/*
	 * If we are not going to pre-install Streams, then we need to ensure it is
	 * fetched by the executor
	 */
	private void provisionStreams(Map<String, String> argsMap, List<Protos.CommandInfo.URI> uriList,
			String destinationRoot) throws StreamsMesosException, ResourceManagerException {
		String streamsInstallable = null;
		String workingDirFile = null;

		LOG.info("Creating Streams Installable in work location.");
		workingDirFile = StreamsMesosConstants.PROVISIONING_WORKDIR_PREFIX + "." + (System.currentTimeMillis() / 1000);

		try {
			if (Utils.createDirectory(workingDirFile) == false) {
				LOG.error("Failed to create working directory for (" + workingDirFile + ")");
				throw new StreamsMesosException("Failed to create working directory");
			}

			streamsInstallable = ResourceManagerUtilities.getResourceManagerPackage(workingDirFile,
					ResourceManagerPackageType.BASE_PLUS_SWS_SERVICES);
		} catch (Exception e) {
			LOG.error("Failed to create Streams Resource Manager Package: " + e.toString());
			throw new StreamsMesosException("Failed to create Streams Resource Manager Package", e);
		}

		LOG.info("Created Streams Installable: " + streamsInstallable);

		// Get it to where we need it
		LOG.info("Looking at destinationRoot(" + destinationRoot + ") to determine filesystem");
		String destinationURI;
		String destinationPath; // without prefix
		if (destinationRoot.startsWith("file://")) {
			destinationPath = destinationRoot.replaceFirst("file:/", "");
			/*** Local File System Version ***/
			LOG.info("Copying Streams Installable to shared location (" + destinationPath + ")...");
			String destPathString;
			try {
				destPathString = LocalFSUtils.copyToLocal(streamsInstallable, destinationPath);
			} catch (IOException e) {
				LOG.error("Failed to copy streamsInstallable(" + streamsInstallable
						+ ") to provisioining shared location (" + destinationPath + ")");
				LOG.error("Exception: " + e.toString());
				throw new StreamsMesosException("Failed to provision Streams executable tar to local FS: ", e);
			}
			// Needs to be an absolute path for Mesos
			destinationURI = "file://" + destPathString;
		} else if (destinationRoot.startsWith("hdfs://")) {
			destinationPath = destinationRoot.replaceFirst("hdfs:/", "");
			/*** Hadoop File System Version ***/
			LOG.info("Copying Stream Installable to HDFS location (" + destinationPath + ")");
			Path hdfsStreamsPath;
			try {
				FileSystem hdfs = HdfsFSUtils.getHDFSFileSystem();
				hdfsStreamsPath = HdfsFSUtils.copyToHDFS(hdfs, destinationPath, streamsInstallable,
						new Path(streamsInstallable).getName());
			} catch (IOException e) {
				LOG.error("Failed to copy streamsInstallable(" + streamsInstallable
						+ ") to provisioining HDFS location (" + destinationPath + ")");
				LOG.error("Exception: " + e.toString());
				throw new StreamsMesosException("Failed to provision Streams executable tar to HDFS: ", e);
			}
			destinationURI = hdfsStreamsPath.toString();
		} else {
			// Should handle http:// in the future
			LOG.error("Unexpected/Unhandled Provsioning Directory URI prefix: " + destinationRoot);
			throw new StreamsMesosException(
					"Unexpected/Unhandled Provsioning Directory URI prefix: " + destinationRoot);
		}

		// Remove working directory
		LOG.info("Deleting Streams Installable from work location...");
		Utils.deleteDirectory(workingDirFile);
		LOG.info("Deleted: " + streamsInstallable);

		// Create Mesos URI for provisioned location
		LOG.info("Creating URI for: " + destinationURI);
		CommandInfo.URI.Builder uriBuilder;
		uriBuilder = CommandInfo.URI.newBuilder();
		// uriBuilder.setCache(true);
		uriBuilder.setCache(false);
		uriBuilder.setExecutable(false);
		uriBuilder.setExtract(true);
		uriBuilder.setValue(destinationURI);

		uriList.add(uriBuilder.build());
		LOG.info("Created URI");
	}
	
	
	//////////////////////////////////////
	/// STATIC METHODS
	//////////////////////////////////////

	static ResourceDescriptor getDescriptor(String id, String host) {
		return new ResourceDescriptor(StreamsMesosConstants.RESOURCE_TYPE, ResourceKind.CONTAINER, id,
				StreamsMesosConstants.RESOURCE_TYPE + "_" + id, host);

	}

	static ResourceDescriptorState getDescriptorState(boolean isRunning, ResourceDescriptor rd) {
		AllocateState s = isRunning ? AllocateState.ALLOCATED : AllocateState.PENDING;
		return new ResourceDescriptorState(s, rd);
	}
	
	
}
