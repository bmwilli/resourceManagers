/**
 *
 */
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

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
public class StreamsMesosResourceFramework extends ResourceManagerAdapter {

	private static final Logger LOG = LoggerFactory.getLogger(StreamsMesosResourceFramework.class);

	private Map<String, String> envs = System.getenv();
	private Map<String, String> argsMap = new HashMap<String, String>();
	private Properties props = null;
	private Scheduler scheduler;
	private MesosSchedulerDriver driver;
	private List<Protos.CommandInfo.URI> uriList = new ArrayList<Protos.CommandInfo.URI>();
	private boolean deployStreams = false;

	/* StreamsMesosResource containers */
	// newRequests: Tracks new requests from Streams and checked by scheduler
	// when new offers arrive
	// Using concurrent CopyOnWriteArrayList.  It is slower than ArrayList but at the rate that 
	// we add/remove resources and requests it is more than fast enough and simplifies concurrency
	private List<StreamsMesosResource> newRequests = new CopyOnWriteArrayList<StreamsMesosResource>();

	// allResources: Tracks all resources no matter what state (e.g. requested,
	// running, etc.)
	// indexed by id that we generate
	// Using concurrentHashMap.  simplifies concurrency between the threads of the this class and the mesos scheduler
	private Map<String, StreamsMesosResource> allResources = new ConcurrentHashMap<String, StreamsMesosResource>();

	/*
	 * Constructor NOTE: Arguments passed are not in a reliable order need to
	 * identify flags you expect and others should be read as key,value
	 * sequential arguments. NOTE: These arguments come from the
	 * streams-on-mesos script and are passed through the StreamsResourceServer
	 * which it executes and which in turn constructs this class
	 */
	public StreamsMesosResourceFramework(String[] args) throws StreamsMesosException {
		LOG.debug("Constructing ResourceManagerAdapter: StreamsMesosResourceFramework");
		LOG.debug("args: " + Arrays.toString(args));
		LOG.trace("Enironment: " + envs);

		argsMap.clear();
		for (int i = 0; i < args.length; i++) {
			switch (args[i]) {
			case StreamsMesosConstants.DEPLOY_ARG:
				deployStreams = true;
				argsMap.put(args[i], "true");
				break;
			case StreamsMesosConstants.MESOS_MASTER_ARG:
			case StreamsMesosConstants.ZK_ARG:
			case StreamsMesosConstants.TYPE_ARG:
			case StreamsMesosConstants.MANAGER_ARG:
			case StreamsMesosConstants.INSTALL_PATH_ARG:
			case StreamsMesosConstants.HOME_DIR_ARG:
			case StreamsMesosConstants.PROP_FILE_ARG:
			case StreamsMesosConstants.DOMAIN_ID_ARG:
				argsMap.put(args[i], args[++i]);
				break;
			}
		}
		LOG.debug("ArgsMap: " + argsMap);

		String propsFile = null;
		// Process Properties FileReader
		if (argsMap.containsKey(StreamsMesosConstants.PROP_FILE_ARG))
			propsFile = argsMap.get(StreamsMesosConstants.PROP_FILE_ARG);
		else
			propsFile = StreamsMesosConstants.RM_PROPERTIES_FILE;
		LOG.debug("Reading Properties file from: " + propsFile);
		props = new Properties();
		try {
			props.load(new FileReader(propsFile));
		} catch (FileNotFoundException e) {
			LOG.error("Could not find properties file: " + propsFile);
			throw new StreamsMesosException("Could not find properties file: " + propsFile, e);
		} catch (IOException e) {
			LOG.error("IO Error reading the properties file: " + propsFile);
			throw new StreamsMesosException("IO Error reading the properties file: " + propsFile, e);
		}

		LOG.debug("Properties file properties: " + props.toString());

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
		super.initialize();

		// Provision Streams if necessary
		// Caution, this can take some time and cause timeouts on slow machines
		// or workstations that are overloaded
		// In testing, saw issues where Streams Resource Manager Server would
		// disconnect client.
		if (argsMap.containsKey(StreamsMesosConstants.DEPLOY_ARG)) {
			try {
				LOG.info("Deploy flag set.  Calling provisionStreams...");
				provisionStreams(argsMap, uriList, StreamsMesosConstants.PROVISIONING_SHARED_URI);
			} catch (StreamsMesosException e) {
				LOG.info("Caught error from provisionStreams");
				throw new ResourceManagerException("Initialization of Streams Mesos Failed to provision Streams", e);
			}
		}

		// Temporary, not sure why constant would ever have the master
		String master = StreamsMesosConstants.MESOS_MASTER;
		if (argsMap.containsKey(StreamsMesosConstants.MESOS_MASTER_ARG))
			master = argsMap.get(StreamsMesosConstants.MESOS_MASTER_ARG);

		// Setup and register the Mesos Scheduler
		LOG.info("About to call runMesosScheduler...");
		// runMesosScheduler(uriList,master);
		runMesosScheduler(master);

		// LOG.info("*** calling waitForTestMessage()...");
		// waitForTestMessage();

		LOG.info("StreamsMesosResourceFramework.initialize() complete");

		// LOG.info("Creating a test master...");
		// StreamsMesosResource smr =
		// createNewSMR(argsMap.get(StreamsMesosConstants.DOMAIN_ID_ARG),
		// argsMap.get(StreamsMesosConstants.ZK_ARG), 1, true);

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
		Protos.Status driverStatus = driver.stop();
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
		LOG.info("StreamsResourceServer called allocateMasterResource()");
		LOG.info("Request: " + request);
		LOG.info("ClientInfo: " + clientInfo);
		List<ResourceDescriptorState> lst = allocateResources(clientInfo, true, 1, request.getTags(),
				AllocateType.SYNCHRONOUS);
		if (lst.size() == 0)
			throw new ResourceManagerException("Streams Mesos Resource Manager could not allocate master resource");
		return lst.get(0).getDescriptor();
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
		LOG.info("StreamsResourceServer called allocateResources()");
		LOG.info("Request: " + request);
		LOG.info("ClientInfo: " + clientInfo);
		return allocateResources(clientInfo, false, request.getCount(), request.getTags(), request.getType());

	}

	/*
	 * Create resources helper for both master and regular resources
	 *
	 */
	private List<ResourceDescriptorState> allocateResources(ClientInfo clientInfo, boolean isMaster, int count,
			ResourceTags tags, AllocateType rType) throws ResourceManagerException, ResourceTagException {
		// throw new ResourceManagerException("StreamsMesosResourceFramework not
		// yet implemented");

		List<StreamsMesosResource> newRequestsFromStreams = new ArrayList<StreamsMesosResource>();

		for (int i = 0; i < count; i++) {
			// Creates new Resource, queues, and adds to map of all resources
			StreamsMesosResource smr = createNewSMR(argsMap.get(StreamsMesosConstants.DOMAIN_ID_ARG),
					argsMap.get(StreamsMesosConstants.ZK_ARG), tags, isMaster);
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
		int waitTimeSecs = getWaitSecs(rType, props);
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

	/*** Streams Resource Manager Helper Functions ***/

	static ResourceDescriptor getDescriptor(String id, String host) {
		return new ResourceDescriptor(StreamsMesosConstants.RESOURCE_TYPE, ResourceKind.CONTAINER, id,
				StreamsMesosConstants.RESOURCE_TYPE + "_" + id, host);

	}

	static ResourceDescriptorState getDescriptorState(boolean isRunning, ResourceDescriptor rd) {
		AllocateState s = isRunning ? AllocateState.ALLOCATED : AllocateState.PENDING;
		return new ResourceDescriptorState(s, rd);
	}
	
	/** 
	 * @param tags
	 * @param smr
	 * @throws ResourceTagException
	 * @throws ResourceManagerException
	 */
	private void convertTags(ResourceTags tags, StreamsMesosResource smr) throws ResourceTagException, ResourceManagerException {
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

	/***** MESOS PRIVATE SUPPORT METHODS *****/

	private static FrameworkInfo getFrameworkInfo() {
		FrameworkInfo.Builder builder = FrameworkInfo.newBuilder();
		builder.setFailoverTimeout(120000);
		builder.setUser("");
		builder.setName(StreamsMesosConstants.FRAMEWORK_NAME);
		return builder.build();
	}
/*
	private static CommandInfo getCommandInfo(List<CommandInfo.URI> uriList) {
		CommandInfo.Builder cmdInfoBuilder = Protos.CommandInfo.newBuilder();
		cmdInfoBuilder.addAllUris(uriList);
		cmdInfoBuilder.setValue(getStreamsShellCommand());
		return cmdInfoBuilder.build();
	}
*/
	private void runMesosScheduler(String mesosMaster) {
		// private void runMesosScheduler(List<CommandInfo.URI>uriList, String
		// mesosMaster) {
		LOG.info("Creating new Mesos Scheduler...");
		// LOG.info("URI List: " + uriList.toString());
		// LOG.info("commandInfo: " + getCommandInfo(uriList));;

		scheduler = new StreamsMesosResourceScheduler(this);

		LOG.info("Creating new MesosSchedulerDriver...");
		driver = new MesosSchedulerDriver(scheduler, getFrameworkInfo(), mesosMaster);

		LOG.info("About to start the mesos scheduler driver...");
		Protos.Status driverStatus = driver.start();
		LOG.info("...start returned status: " + driverStatus.toString());
	}

	/***** STREAMS PROVISIONING METHODS *****/

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

	/*
	 * Framework public methods for collaboration with scheduler
	 */

	/* StreamsMesosResource Container methods */

	// Create a new SMR and put it proper containers
	synchronized private StreamsMesosResource createNewSMR(String domainId, String zk, ResourceTags tags, boolean isMaster) throws ResourceManagerException {
		// Create the Resource object (default state is NEW)
		StreamsMesosResource smr = new StreamsMesosResource(Utils.generateNextId("smr"), domainId, zk, argsMap,
				uriList);

		smr.setMaster(isMaster);
		
		// Set resource needs (Need to integrate with tags soon)
		double memory = StreamsMesosConstants.RM_MEMORY_DEFAULT;
		double cores = StreamsMesosConstants.RM_CORES_DEFAULT;

		if (Utils.hasProperty(props, StreamsMesosConstants.PROPS_DC_MEMORY))
			memory = Utils.getDoubleProperty(props, StreamsMesosConstants.PROPS_DC_MEMORY);

		if (Utils.hasProperty(props, StreamsMesosConstants.PROPS_DC_CORES))
			cores = Utils.getDoubleProperty(props, StreamsMesosConstants.PROPS_DC_CORES);
		smr.setMemory(memory);
		smr.setCpu(cores);

		// Set the resource tags
		if (tags != null) {
			convertTags(tags, smr); // may set mem/cpu from the tags if they are specified
			smr.getTags().addAll(tags.getNames());
		}
		
		
		// Add to collections to track
		LOG.info("Queuing new Resource Request: " + smr.toString());
		newRequests.add(smr);
		allResources.put(smr.getId(), smr);

		return smr;
	}

	// Return list of new Reqeusts as an immutable list
	public List<StreamsMesosResource> getNewRequestList() {
		return newRequests;
	}

	// Update SMR and maintain lists
	// Eventually pass what to update it with
	synchronized public void updateSMRbyTaskId(String taskId, StreamsMesosResource.StreamsMesosResourceState newState) {
		LOG.info("updateSMRbyTaskId(" + taskId + ", " + newState.toString());
		LOG.debug("updateSMRbyTaskId.allResources: " + allResources.toString());
		boolean foundMatch = false;
		for (StreamsMesosResource smr : allResources.values()) {
			if (smr.getTaskId().equals(taskId)) {
				foundMatch = true;
				updateSMR(smr.getId(),newState);
			} 
		}
		if (!foundMatch) {
			LOG.warn("Update of SMR by TaskId Failed because SMR Not found (TaskID: " + taskId + ", newstate: " + newState.toString() + ")");
		}
	}
	
	// Update the SMR status 
	// Need to handle these appropriately because we have concurrency issues of iterating in the scheduler
	// and calling this to update the list
	synchronized public void updateSMR(String id, StreamsMesosResource.StreamsMesosResourceState newState) {
		LOG.info("updateSMR(" + id + ", " + newState.toString());
		LOG.debug("updateSMR.allResources: " + allResources.toString());
		StreamsMesosResource smr = null;
		//StreamsMesosResource.StreamsMesosResourceState oldState;
		// Find it in the list of all StreamsMesosResources
		if (allResources.containsKey(id)) {
			smr = allResources.get(id);
			//oldState = smr.getState();
			
			// Update state
			smr.setState(newState);
		} else {
			LOG.warn("Update of SMR Failed because SMR Not found ((id: " + id + ", newstate: " + newState.toString() + ")");
		}

	}

	/*
	 * Create the CommandInfo for initiating a Streams Resource Question: May
	 * need a second version for differences between master streams resource
	 * (when domain is started) and all others
	 */
	/*
	public CommandInfo getStreamsResourceCommand() {

		CommandInfo.Builder cmdInfoBuilder = Protos.CommandInfo.newBuilder();

		// Create command, depends on deployment mechanism
		StringBuffer cmdBuffer = new StringBuffer();

		cmdBuffer.append("echo 'Setting up Streams Environment'");
		if (argsMap.containsKey(StreamsMesosConstants.DEPLOY_ARG)) {
			// run the streams resource installer
			// create softlink for StreamsLink
		} else {
			// if --deploy not set, we assume streams is installed on all
			// machines
			String streamsInstall = argsMap.get(StreamsMesosConstants.INSTALL_PATH_ARG);
			cmdBuffer.append(";ln -s " + streamsInstall + " StreamsLink");
		}
		// Source streams install path
		cmdBuffer.append(";source StreamsLink/bin/streamsprofile.sh");

		// Verify Streamtool version
		cmdBuffer.append(";echo 'Streamtool version:'");
		cmdBuffer.append(";streamtool version");

		// Set command string
		cmdInfoBuilder.setValue(cmdBuffer.toString());

		// Add URI's (if any)
		cmdInfoBuilder.addAllUris(uriList);

		return cmdInfoBuilder.build();
	}
	*/
	
}
