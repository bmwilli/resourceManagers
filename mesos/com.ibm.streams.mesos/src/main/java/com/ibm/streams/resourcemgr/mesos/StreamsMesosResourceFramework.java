/**
 *
 */
package com.ibm.streams.resourcemgr.mesos;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;

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
import com.ibm.streams.resourcemgr.ResourceManagerAdapter;
import com.ibm.streams.resourcemgr.ResourceManagerException;
import com.ibm.streams.resourcemgr.ResourceManagerUtilities;
import com.ibm.streams.resourcemgr.ResourceManagerUtilities.ResourceManagerPackageType;
import com.ibm.streams.resourcemgr.ResourceTagException;
import com.ibm.streams.resourcemgr.ResourceTags;

/**
 * @author bmwilli
 *
 */
public class StreamsMesosResourceFramework extends ResourceManagerAdapter {

	private static final Logger LOG = LoggerFactory.getLogger(StreamsMesosResourceFramework.class);

	private Map<String,String> envs = System.getenv();
	private Map<String,String> argsMap = new HashMap<String, String>();
	private Scheduler scheduler;
	private MesosSchedulerDriver driver;
	private List<Protos.CommandInfo.URI> uriList = new ArrayList<Protos.CommandInfo.URI>();


	/* StreamsMesosREsource containers */
	// newRequests: Tracks new requests from Streams and checked by scheduler when new offers arrive
	private List<StreamsMesosResource> newRequests = new ArrayList<StreamsMesosResource>();


	/*
	 * Constructor
	 * NOTE: Arguments passed are not in a reliable order
	 * 	need to identify flags you expect and others should be read as key,value
	 * 	sequential arguments.
	 * NOTE: These arguments come from the streams-on-mesos script and are
	 * 	passed through the StreamsResourceServer which it executes
	 *	and which in turn constructs this class
	 */
	public StreamsMesosResourceFramework(String [] args) throws StreamsMesosException {
		LOG.debug("Constructing ResourceManagerAdapter: StreamsMesosResourceFramework");
		LOG.debug("args: " + Arrays.toString(args));
		LOG.trace("Enironment: " + envs);

		argsMap.clear();
		for (int i=0; i < args.length; i++) {
			switch (args[i]) {
			case StreamsMesosConstants.DEPLOY_ARG:
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
				argsMap.put(args[i],  args[++i]);
				break;
			}
		}
		LOG.debug("ArgsMap: " + argsMap);
	}


	/***** Streams Resource Manager API Methods *****/


	/* (non-Javadoc)
	 * Initialize Resource Manager when resource server starts
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
				throw new ResourceManagerException("Initialization of Streams Mesos Failed to provision Streams",e);
			}
		}

    // Temporary, not sure why constant would ever have the master
		String master = StreamsMesosConstants.MESOS_MASTER;
		if (argsMap.containsKey(StreamsMesosConstants.MESOS_MASTER_ARG))
			master = argsMap.get(StreamsMesosConstants.MESOS_MASTER_ARG);

		// Setup and register the Mesos Scheduler
		LOG.info("About to call runMesosScheduler...");
		runMesosScheduler(uriList,master);

		//LOG.info("*** calling waitForTestMessage()...");
		//waitForTestMessage();

		LOG.info("StreamsMesosResourceFramework.initialize() complete");

		LOG.info("Creating a test...");
		StreamsMesosResource smr = createNewSMR("Brian",
			argsMap.get(StreamsMesosConstants.DOMAIN_ID_ARG),
			argsMap.get(StreamsMesosConstants.ZK_ARG),
			1);


	}

	/* (non-Javadoc)
	 * Close/Shutdown/Cleanup Resource Manager when resource server stops
	 * @see com.ibm.streams.resourcemgr.ResourceManagerAdapter#close()
	 */
	@Override
	public void close() {
		LOG.debug("close()");
		LOG.info("stopping the mesos driver...");
		Protos.Status driverStatus = driver.stop();
		LOG.info("...driver stopped, status: " + driverStatus.toString());
		super.close();
	}



	/* Create master resource.  This resource is the first resource
	 * requested by Streams and is requested when the streams domain starts
	 * @see com.ibm.streams.resourcemgr.ResourceManagerAdapter#allocateMasterResource(com.ibm.streams.resourcemgr.ClientInfo, com.ibm.streams.resourcemgr.AllocateMasterInfo)
	 */
	@Override
	public ResourceDescriptor allocateMasterResource(ClientInfo clientInfo, AllocateMasterInfo info)
			throws ResourceTagException, ResourceManagerException {
		LOG.info("Allocate Master Request: " + info);
		List<ResourceDescriptorState> lst = allocateResources(clientInfo, true, 1,
				info.getTags(), AllocateType.SYNCHRONOUS);
		if (lst.size() == 0)
			throw new ResourceManagerException("Could not allocate master resource");
		return lst.get(0).getDescriptor();
	}


	/* Create non-master resources.  These resources are used for Streams instances
	 * @see com.ibm.streams.resourcemgr.ResourceManagerAdapter#allocateResources(com.ibm.streams.resourcemgr.ClientInfo, com.ibm.streams.resourcemgr.AllocateInfo)
	 */
	@Override
	public Collection<ResourceDescriptorState> allocateResources(ClientInfo clientInfo, AllocateInfo request)
			throws ResourceTagException, ResourceManagerException {
		LOG.info("Allocate Request: " + request);

		return allocateResources(clientInfo, false, request.getCount(), request.getTags(),
				request.getType());

	}

	/* Create resources helper for both master and regular resources
	 *
	 */
	private List<ResourceDescriptorState> allocateResources(ClientInfo clientInfo, boolean isMaster,
		int count, ResourceTags tags, AllocateType rType) throws
		ResourceManagerException, ResourceTagException {
			throw new ResourceManagerException("StreamsMesosResourceFramework not yet implemented");

	}

	/*** Streams Resource Manager Helper Functions ***/

	static ResourceDescriptor getDescriptor(String id, String host) {
      return new ResourceDescriptor(StreamsMesosConstants.RESOURCE_TYPE, ResourceKind.CONTAINER, id,//native resource name
              StreamsMesosConstants.RESOURCE_TYPE + "_" + id,//display name
              host);

  }

  static ResourceDescriptorState getDescriptorState(boolean isRunning, ResourceDescriptor rd) {
      AllocateState s = isRunning ? AllocateState.ALLOCATED : AllocateState.PENDING;
      return new ResourceDescriptorState(s, rd);
  }


	/***** MESOS PRIVATE SUPPORT METHODS *****/

	private static FrameworkInfo getFrameworkInfo() {
		FrameworkInfo.Builder builder = FrameworkInfo.newBuilder();
		builder.setFailoverTimeout(120000);
		builder.setUser("");
		builder.setName(StreamsMesosConstants.FRAMEWORK_NAME);
		return builder.build();
	}

	private static CommandInfo getCommandInfo(List<CommandInfo.URI>uriList) {
		CommandInfo.Builder cmdInfoBuilder = Protos.CommandInfo.newBuilder();
		cmdInfoBuilder.addAllUris(uriList);
		cmdInfoBuilder.setValue(getStreamsShellCommand());
		return cmdInfoBuilder.build();
	}

	private void runMesosScheduler(List<CommandInfo.URI>uriList, String mesosMaster) {
		LOG.info("Creating new Mesos Scheduler...");
		LOG.info("URI List: " + uriList.toString());
		LOG.info("commandInfo: " + getCommandInfo(uriList));;

		scheduler = new StreamsMesosResourceScheduler(this);

		LOG.info("Creating new MesosSchedulerDriver...");
		driver = new MesosSchedulerDriver(scheduler, getFrameworkInfo(), mesosMaster);

		LOG.info("About to start the mesos scheduler driver...");
		Protos.Status driverStatus = driver.start();
		LOG.info("...start returned status: " + driverStatus.toString());
	}



	/***** STREAMS PROVISIONING METHODS *****/

	/* If we are not going to pre-install Streams, then we need to ensure it is fetched
	 * by the executor
	 */
	private void provisionStreams(Map<String, String> argsMap,
			List<Protos.CommandInfo.URI> uriList, String destinationRoot) throws
			StreamsMesosException, ResourceManagerException {
		String streamsInstallable = null;
		String workingDirFile = null;

		LOG.info("Creating Streams Installable in work location.");
		workingDirFile = StreamsMesosConstants.PROVISIONING_WORKDIR_PREFIX + "." +
				(System.currentTimeMillis() / 1000);

		try {
			if (Utils.createDirectory(workingDirFile) == false) {
				LOG.error("Failed to create working directory for (" + workingDirFile + ")");
				throw new StreamsMesosException("Failed to create working directory");
			}

			streamsInstallable = ResourceManagerUtilities.getResourceManagerPackage(workingDirFile,
					ResourceManagerPackageType.BASE_PLUS_SWS_SERVICES);
		} catch (Exception e) {
			LOG.error("Failed to create Streams Resource Manager Package: " + e.toString());
			throw new StreamsMesosException("Failed to create Streams Resource Manager Package",e);
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
				LOG.error("Failed to copy streamsInstallable(" + streamsInstallable + ") to provisioining shared location (" + destinationPath + ")");
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
				hdfsStreamsPath = HdfsFSUtils.copyToHDFS(hdfs, destinationPath, streamsInstallable, new Path(streamsInstallable).getName());
			} catch (IOException e) {
				LOG.error("Failed to copy streamsInstallable(" + streamsInstallable + ") to provisioining HDFS location (" + destinationPath + ")");
				LOG.error("Exception: " + e.toString());
				throw new StreamsMesosException("Failed to provision Streams executable tar to HDFS: ", e);
			}
			destinationURI = hdfsStreamsPath.toString();
		} else {
			// Should handle http:// in the future
			LOG.error("Unexpected/Unhandled Provsioning Directory URI prefix: " + destinationRoot);
			throw new StreamsMesosException("Unexpected/Unhandled Provsioning Directory URI prefix: " + destinationRoot);
		}

		// Remove working directory
		LOG.info("Deleting Streams Installable from work location...");
		Utils.deleteDirectory(workingDirFile);
		LOG.info("Deleted: " + streamsInstallable);

		// Create Mesos URI for provisioned location
		LOG.info("Creating URI for: " + destinationURI);
		CommandInfo.URI.Builder uriBuilder;
		uriBuilder = CommandInfo.URI.newBuilder();
		//uriBuilder.setCache(true);
		uriBuilder.setCache(false);
		uriBuilder.setExecutable(false);
		uriBuilder.setExtract(true);
		uriBuilder.setValue(destinationURI);

		uriList.add(uriBuilder.build());
		LOG.info("Created URI");
	}


	/*
	 * Streams Command
	 * This may need to move into an executor if we have different commands for master resource
	 * vs other resources or we have trouble stopping the task
	 */
	private static String getStreamsShellCommand() {
		String cmd = "echo '*** Brians Streams Command ***';" +
				"echo 'pwd; ' `pwd`;" +
				"ls -l";
		return cmd;
	}


	/*
	 * Framework public methods for collaboration with scheduler
	 */

	/* StreamsMesosResource Container methods */

	// Create a new SMR and put it proper containers
	synchronized private StreamsMesosResource createNewSMR(String id, String domainId, String zk, int priority) {
		StreamsMesosResource smr = new StreamsMesosResource(id, domainId, zk, priority);
		newRequests.add(smr);

		return smr;
	}

	// Return first from list.  This assumes it will be removed from list by another call
	synchronized public StreamsMesosResource getNewSMR() {
		if (newRequests.size() > 0)
			return newRequests.get(0);
		else
			return null;
	}

	// Update SMR and maintain lists
	// Eventually pass what to update it with
	synchronized public void updateSMR(String id) {
		// For now just remove from new lists
		for (Iterator<StreamsMesosResource> iter = newRequests.listIterator(); iter.hasNext(); ) {
    	String item_id = iter.next().getId();
    	if (item_id == id){
        iter.remove();
    	}
		}
	}

	/* Create the CommandInfo for initiating a Streams Resource
	 * Question: May need a second version for differences between
	 * master streams resource (when domain is started) and all others
	 */
	public CommandInfo getStreamsResourceCommand() {

		CommandInfo.Builder cmdInfoBuilder = Protos.CommandInfo.newBuilder();

		// Create command, depends on deployment mechanism
		StringBuffer cmdBuffer = new StringBuffer();

		cmdBuffer.append("echo 'Setting up Streams Environment'");
		if (argsMap.containsKey(StreamsMesosConstants.DEPLOY_ARG)) {
			// run the streams resource installer
			// create softlink for StreamsLink
		} else {
			//if --deploy not set, we assume streams is installed on all machines
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
	public synchronized void waitForTestMessage() {
		LOG.info("*** About to wait() to see if schedulre can wake me up...");
		try {
			wait();
		} catch (Exception e) {
			LOG.error("*** wait exception: " + e.toString());
		}
		LOG.info("*** WAIT IS OVER!!!");
	}
	public synchronized String testMessage() {
		LOG.info("*** testMessage(); called!!!...notify()...");
		notify();
		LOG.info("*** notify() returned");
		return "You are talking to the Framework!!!";
	}

}
