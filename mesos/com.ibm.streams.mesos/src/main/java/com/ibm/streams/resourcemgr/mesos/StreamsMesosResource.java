package com.ibm.streams.resourcemgr.mesos;
/**
 * Wraps a resource request from IBM Streams Resource Server
 *
 */

//import java.net.UnknownHostException;
import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;

import java.util.List;
import java.util.Set;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.Resource;

/*
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.util.Records;
*/
import com.ibm.streams.resourcemgr.ResourceDescriptor;
import com.ibm.streams.resourcemgr.ResourceDescriptorState;
import com.ibm.streams.resourcemgr.ResourceManagerUtilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import com.ibm.streams.resourcemgr.yarn.ContainerWrapper.ContainerWrapperState;

class StreamsMesosResource {
	private static final Logger LOG = LoggerFactory.getLogger(StreamsMesosResource.class);

	public static enum StreamsMesosResourceState {
		NEW, LAUNCHED, RUNNING, STOPPING, STOPPED, RELEASED, FAILED;
		@Override
		public String toString() {
			switch (this) {
			case NEW:
				return "NEW";
			case LAUNCHED:
				return "LAUNCHED";
			case RUNNING:
				return "RUNNING";
			case STOPPING:
				return "STOPPING";
			case STOPPED:
				return "STOPPED";
			case RELEASED:
				return "RELEASED";
			case FAILED:
				return "FAILED";
			default:
				throw new IllegalArgumentException();
			}
		}
	}

	// Unique Identier used for identification within Framework and for Mesos
	// Task ID
	private String id;
	// Streams related members
	private String domainId = null;
	private String zkConnect = null;
	// Need to understand more about how this is used within containers
	// Was getting error about key not found, os went with Yarn RM approach to test
	private String homeDir = null;
	private StreamsMesosResourceState state = StreamsMesosResourceState.NEW;
	private Set<String> tags = new HashSet<String>();

	private boolean deployStreams;
	private Map<String, String> argsMap;
	private List<Protos.CommandInfo.URI> uriList;

	// Resource utilization related members
	private double memory = -1;
	private double cpu = -1;
	//private int priority = -1;

	// private ContainerRequest request ;
	// private ContainerWrapper allocatedContainer = null;
	private boolean isMaster = false;
	private boolean cancelled = false;

	// Mesos Task related members
	private String taskId = null;
	
	
	
	//public StreamsMesosResource(String id, String domainId, String zk, int priority, Map<String, String> argsMap,
	public StreamsMesosResource(String id, String domainId, String zk, Map<String, String> argsMap,
			List<Protos.CommandInfo.URI> uriList) {
		this.id = id;
		this.domainId = domainId;
		this.zkConnect = zk;
		//this.priority = priority;
		this.argsMap = argsMap;
		this.uriList = uriList;
		if (argsMap.containsKey(StreamsMesosConstants.HOME_DIR_ARG))
			homeDir = argsMap.get(StreamsMesosConstants.HOME_DIR_ARG);
	}

	public String getId() {
		return id;
	}

	public String getDomainId() {
		return domainId;
	}

	public String getZkConnect() {
		return zkConnect;
	}

	public boolean isMaster() {
		return isMaster;
	}

	public void setMaster(boolean isMaster) {
		this.isMaster = isMaster;
	}

	public double getMemory() {
		return memory;
	}

	public void setMemory(double memory) {
		this.memory = memory;
	}

	public double getCpu() {
		return cpu;
	}

	public void setCpu(double cpu) {
		this.cpu = cpu;
	}

	public Set<String> getTags() {
		return tags;
	}
	
	
	
	/**
	 * @return the homeDir
	 */
	public String getHomeDir() {
		return homeDir;
	}

	/**
	 * @param homeDir the homeDir to set
	 */
	public void setHomeDir(String homeDir) {
		this.homeDir = homeDir;
	}

	/**
	 * @return the state
	 */
	public StreamsMesosResourceState getState() {
		return state;
	}

	/**
	 * @param state the state to set
	 */
	public void setState(StreamsMesosResourceState state) {
		this.state = state;
	}
	
	

	/**
	 * @return the taskId
	 */
	public String getTaskId() {
		return taskId;
	}

	/**
	 * @param taskId the taskId to set
	 */
	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}
	
	public boolean isRunning() {
		// Fix for mesos
		return state == StreamsMesosResourceState.RUNNING;
	}

	public void cancel() {
		cancelled = true;
		// Fix for mesos
		// if(allocatedContainer!=null)
		// allocatedContainer.cancel();
	}

	public boolean isCancelled() {
		return cancelled;
	}

	/*
	 * fix for mesos public void setAllocatedContainer(ContainerWrapper cw) {
	 * allocatedContainer = cw; allocatedContainer.setId(id); if(cancelled)
	 * allocatedContainer.cancel(); } public ContainerWrapper
	 * getAllocatedContainer() { return allocatedContainer; }
	 */
	/*
	 * public boolean isAllocated() { return allocatedContainer != null; }
	 * public boolean isRunning() { return isAllocated() &&
	 * allocatedContainer.getWrapperState() == ContainerWrapperState.RUNNING; }
	 */

	/* fix these for mesos, not sure why the two ways to getDescriptor */
	public ResourceDescriptor getDescriptor() {
		// if(allocatedContainer != null)
		// return allocatedContainer.getDescriptor();
		return StreamsMesosResourceFramework.getDescriptor(id, null);
	}

	public ResourceDescriptorState getDescriptorState() {
		return StreamsMesosResourceFramework.getDescriptorState(isRunning(), getDescriptor());
	}

	// OLD UNUSED FROM YARN
	public List<String> getStartupCommand(String installPath, String homeDir, boolean deployStreams) {
		List<String> cmd = new ArrayList<String>();
		cmd.add("export HOME=" + homeDir + ";");
		if (!deployStreams) {
			cmd.add(" source " + installPath + "/bin/streamsprofile.sh; ");
			cmd.add(" ln -s " + installPath + " StreamsLink; ");
		} else {
			cmd.add(" echo 'NEED TO IMPLEMENT DEPLOYMENT';");
			// cmd.add(" ls -lR > " + ApplicationConstants.LOG_DIR_EXPANSION_VAR
			// + "/dir_contents_pre; ");
			// cmd.add(" ./" + StreamsYarnConstants.RES_STREAMS_BIN +
			// "/StreamsResourceInstall/streamsresourcesetup.sh"
			// + " --install-dir ./InfoSphereStreams"
			// + " &> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR +
			// "/streamsinstall || exit 1;");
			// cmd.add(" source ./InfoSphereStreams/*/bin/streamsprofile.sh;");
			// cmd.add(" cat $PWD/InfoSphereStreams/.streams.version.dir | " +
			// " xargs -I '{}' ln -s '$PWD/InfoSphereStreams/{}' StreamsLink;");
		}
		// cmd.add(" ls -lR > " + ApplicationConstants.LOG_DIR_EXPANSION_VAR +
		// "/dir_contents; ");
		// cmd.add(" env > " + ApplicationConstants.LOG_DIR_EXPANSION_VAR +
		// "/env ;");
		// cmd.add(" cat launch*.sh > " +
		// ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/launch_context;");
		cmd.add(ResourceManagerUtilities.getStartControllerCommand("$PWD/StreamsLink", getZkConnect(), getDomainId(),
				getDescriptor(), getTags(), false, // false = do not fork
				isMaster()));

		// cmd.add(" &>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR +
		// "/application.log");
		return cmd;
	}
	/*
	 * public ContainerRequest getCreateResourceRequest() throws
	 * UnknownHostException { if(request != null) return request; Priority
	 * priority = Records.newRecord(Priority.class);
	 * 
	 * priority.setPriority(this.priority); Resource res =
	 * Records.newRecord(Resource.class); if(memory > 0) res.setMemory(memory);
	 * if(cpu > 0) res.setVirtualCores(cpu); request =new ContainerRequest( res,
	 * null, //hosts null,//racks priority, true//relax locality ); return
	 * request; }
	 */

	/*
	 * public boolean matches(ContainerWrapper cWrapper) {
	 * 
	 * if(cWrapper == null) return false; if(priority !=
	 * cWrapper.getContainer().getPriority().getPriority()) { return false; } //
	 * if(cpu > 0 && cWrapper.getContainer().getResource().getVirtualCores() !=
	 * cpu) { // return false; // } if( memory > 0 &&
	 * cWrapper.getContainer().getResource().getMemory() < memory ) { return
	 * false; }
	 * 
	 * return true; }
	 */

	/* Mesos Helper Methods */
	/*
	 * Create the CommandInfo for initiating a Streams Resource Question: May
	 * need a second version for differences between master streams resource
	 * (when domain is started) and all others
	 */
	public CommandInfo getStreamsResourceCommand() {

		CommandInfo.Builder cmdInfoBuilder = Protos.CommandInfo.newBuilder();

		// Create command, depends on deployment mechanism
		StringBuffer cmdBuffer = new StringBuffer();

		cmdBuffer.append("echo 'Setting up Streams Environment'");
		if (getHomeDir() != null)
			//cmdBuffer.append(";export HOME=${MESOS_SANDBOX}");
			cmdBuffer.append("; export HOME=" + getHomeDir());

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
		cmdBuffer.append("; env > env.out");
		// Verify Streamtool version
		cmdBuffer.append(";streamtool version");
		// Start Streams Controller
		cmdBuffer.append(";" + ResourceManagerUtilities.getStartControllerCommand("$PWD/StreamsLink", getZkConnect(),
				getDomainId(), getDescriptor(), getTags(), false, isMaster()));
		cmdBuffer.append(" &> application.log");
		// Set command string
		LOG.debug("Task Command: " + cmdBuffer.toString());
		cmdInfoBuilder.setValue(cmdBuffer.toString());

		// Add URI's (if any)
		cmdInfoBuilder.addAllUris(uriList);

		return cmdInfoBuilder.build();
	}

	/**
	 * Create Mesos Task Info for this Resource If cpus and/or memory are not
	 * specified, use all from offer
	 */
	public Protos.TaskInfo buildStreamsMesosResourceTask(Protos.Offer offer) {

		double task_cpus = getCpu();
		double task_memory = getMemory();

		// Create taskId
		Protos.TaskID taskIdProto = Protos.TaskID.newBuilder().setValue(getId()).build();
		setTaskId(taskIdProto.getValue());

		// Calculate resources usage if configured to use it all
		if ((task_cpus == StreamsMesosConstants.USE_ALL_CORES)
				|| (task_memory == StreamsMesosConstants.USE_ALL_MEMORY)) {
			for (Resource r : offer.getResourcesList()) {
				if ((r.getName().equals("cpus")) && (task_cpus == StreamsMesosConstants.USE_ALL_CORES)) {
					task_cpus = r.getScalar().getValue();
					LOG.debug("SMR Task (" + getId() + ") using all cpus in offer: " + String.valueOf(task_cpus));
				}
				if ((r.getName().equals("mem")) && (task_memory == StreamsMesosConstants.USE_ALL_MEMORY)) {
					task_memory = r.getScalar().getValue();
					LOG.debug("SMR Task (" + getId() + ") using all memory in offer: " + String.valueOf(task_memory));
				}
			}
		}

		// Get the commandInfo from the Streams Mesos Resource
		Protos.CommandInfo commandInfo = getStreamsResourceCommand();

		// Work on getting this fillout out correctly
		Protos.TaskInfo task = Protos.TaskInfo.newBuilder().setName(getId()).setTaskId(taskIdProto)
				.setSlaveId(offer.getSlaveId()).addResources(buildResource("cpus", task_cpus))
				.addResources(buildResource("mem", task_memory))
				// .setData(ByteString.copyFromUtf8("" + taskIdCounter))
				.setCommand(Protos.CommandInfo.newBuilder(commandInfo)).build();

		return task;
	}

	private Protos.Resource buildResource(String name, double value) {
		return Protos.Resource.newBuilder().setName(name).setType(Protos.Value.Type.SCALAR)
				.setScalar(buildScalar(value)).build();
	}

	private Protos.Value.Scalar.Builder buildScalar(double value) {
		return Protos.Value.Scalar.newBuilder().setValue(value);
	}

	@Override
	public String toString() {
		return "StreamsMesosResource [id=" + id + ", state=" + state + ", domainId=" + domainId + ", zkConnect="
				+ zkConnect + ", memory=" + memory + ", cpu=" + cpu + ", isMaster="
				+ isMaster + ", cancelled=" + cancelled + ", tags=" + tags
				+ ", taskId=" + taskId
				// + ", allocatedContainer=" + allocatedContainer
				+ "]";
	}
}
