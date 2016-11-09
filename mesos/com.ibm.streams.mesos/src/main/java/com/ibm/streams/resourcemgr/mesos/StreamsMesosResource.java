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

//import com.ibm.streams.resourcemgr.yarn.ContainerWrapper.ContainerWrapperState;

class StreamsMesosResource {
	enum StreamsMesosResourceState {NEW, LAUNCHED, RUNNING, STOPPING, STOPPED, RELEASED;
		@Override
		public String toString() {
			switch(this) {
				case NEW: return "NEW";
				case LAUNCHED: return "LAUNCHED";
				case RUNNING: return "RUNNING";
				case STOPPING: return "STOPPING";
				case STOPPED: return "STOPPED";
				case RELEASED: return "RELEASED";
				default: throw new IllegalArgumentException();
			}
		}
	}

	// Unique Identier used for identification within Framework and for Mesos Task ID
	private String id;
	// Streams related members
	private String domainId = null;
	private String zkConnect = null;
	private StreamsMesosResourceState state = StreamsMesosResourceState.NEW;
	private Set<String> tags = new HashSet<String>();

	private boolean deployStreams;
	private Map<String,String> argsMap;
	private List<Protos.CommandInfo.URI> uriList;

	// Resource utilization related members
	private int memory = -1;
	private int cpu = -1
	private int priority=-1;

	//private ContainerRequest request ;
	//private ContainerWrapper allocatedContainer = null;
	private boolean isMaster = false;
	private boolean cancelled = false;

	// Mesos Task related members

	public StreamsMesosResource(String id, String domainId, String zk, int priority, Map<String,String> argsMap, List<Protos.CommandInfo.URI> uriList) {
		this.id = id;
		this.domainId = domainId;
		this.zkConnect = zk;
		this.priority = priority;
		this.argsMap = argsMap;
		this.uriList = uriList;
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
	public int getMemory() {
		return memory;
	}
	public void setMemory(int memory) {
		this.memory = memory;
	}
	public int getCpu() {
		return cpu;
	}
	public void setCpu(int cpu) {
		this.cpu = cpu;
	}
	public Set<String> getTags() {
		return tags;
	}

	public int getPriority() {
		return priority;
	}
	public void cancel() {
		cancelled = true;
    // Fix for mesos
		//if(allocatedContainer!=null)
		//	allocatedContainer.cancel();
	}
	public boolean isCancelled() {
		return cancelled ;
	}

  /* fix for mesos
	public void setAllocatedContainer(ContainerWrapper cw) {
		allocatedContainer = cw;
		allocatedContainer.setId(id);
		if(cancelled)
			allocatedContainer.cancel();
	}
	public ContainerWrapper getAllocatedContainer() {
		return allocatedContainer;
	}
	*/
  /*
	public boolean isAllocated() {
		return allocatedContainer != null;
	}
	public boolean isRunning() {
		return isAllocated() && allocatedContainer.getWrapperState() == ContainerWrapperState.RUNNING;
	}
  */

  /* fix these for mesos, not sure why the two ways to getDescriptor */
	public ResourceDescriptor getDescriptor () {
		//if(allocatedContainer != null)
		//	return allocatedContainer.getDescriptor();
		return StreamsMesosResourceFramework.getDescriptor(id, null);
	}
	public ResourceDescriptorState getDescriptorState () {
		//if(allocatedContainer != null)
		//	return allocatedContainer.getDescriptorState();
		return StreamsMesosResourceFramework.getDescriptorState(false, getDescriptor());
	}


	public List<String> getStartupCommand (String installPath, String homeDir, boolean deployStreams) {
		List<String> cmd = new ArrayList<String>();
		cmd.add("export HOME=" + homeDir + ";");
		if(!deployStreams) {
			cmd.add(" source " + installPath + "/bin/streamsprofile.sh; ");
			cmd.add(" ln -s " + installPath + " StreamsLink; ");
		}
		else {
      cmd.add(" echo 'NEED TO IMPLEMENT DEPLOYMENT';");
			//cmd.add(" ls -lR > " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/dir_contents_pre; ");
			//cmd.add(" ./" + StreamsYarnConstants.RES_STREAMS_BIN + "/StreamsResourceInstall/streamsresourcesetup.sh"
			//	+ " --install-dir ./InfoSphereStreams"
			//	+ " &> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/streamsinstall || exit 1;");
			//cmd.add(" source ./InfoSphereStreams/*/bin/streamsprofile.sh;");
			//cmd.add(" cat $PWD/InfoSphereStreams/.streams.version.dir | " +
			//		" xargs -I '{}' ln -s '$PWD/InfoSphereStreams/{}'  StreamsLink;");
		}
		//cmd.add(" ls -lR > " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/dir_contents; ");
		//cmd.add(" env > " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/env ;");
		//cmd.add(" cat launch*.sh > " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/launch_context;");
		cmd.add(ResourceManagerUtilities.getStartControllerCommand(
				"$PWD/StreamsLink",
				getZkConnect(),
				getDomainId(),
				getDescriptor(),
				getTags(),
				false, //false = do not fork
				isMaster()));

		//cmd.add(" &>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/application.log");
		return cmd;
	}
  /*
	public ContainerRequest getCreateResourceRequest() throws UnknownHostException {
		if(request != null)
			return request;
		Priority priority = Records.newRecord(Priority.class);

		priority.setPriority(this.priority);
		Resource res = Records.newRecord(Resource.class);
		if(memory > 0)
			res.setMemory(memory);
		if(cpu > 0)
			res.setVirtualCores(cpu);
		request =new ContainerRequest(
				res,
				null, //hosts
				null,//racks
				priority,
				true//relax locality
				);
		return request;
	}
  */

  /*
	public boolean matches(ContainerWrapper cWrapper) {

		if(cWrapper == null) return false;
		if(priority != cWrapper.getContainer().getPriority().getPriority()) {
			return false;
		}
//		if(cpu > 0 && cWrapper.getContainer().getResource().getVirtualCores() != cpu) {
//			return false;
//		}
		if( memory > 0 && cWrapper.getContainer().getResource().getMemory() < memory ) {
			return false;
		}

		return true;
	}
  */

	/* Mesos Helper Methods */
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

  /**
	 * Create Mesos Task Info for this Resource
	 */
	public Protos.TaskInfo buildStreamsMesosResourceTask(SlaveID targetSlave) {

		// get new way to do this from yarn
		Protos.TaskID taskId = buildNewTaskID();

		// Get the commandInfo from the Streams Mesos Resource
		Protos.CommandInfo commandInfo = smr.getStreamsResourceCommand();

		// Work on getting this fillout out correctly
		Protos.TaskInfo task = Protos.TaskInfo.newBuilder()
				.setName(id)
				.setTaskId(id)
				.setSlaveId(targetSlave)
				.addResources(buildResource("cpus",1))
				.addResources(buildResource("mem",128))
				.setData(ByteString.copyFromUtf8("" + taskIdCounter))
				.setCommand(Protos.CommandInfo.newBuilder(commandInfo))
				.build();

		return task;
	}




	@Override
	public String toString() {
		return "StreamsMesosResource [id=" + id
		   	+ ", state=" + state
				+ ", domainId=" + domainId
				+ ", zkConnect=" + zkConnect
				+ ", priority=" + priority
				+ ", memory=" + memory
				+ ", cpu=" + cpu
				+ ", isMaster=" + isMaster
				+ ", cancelled=" + cancelled
				+ ", tags=" + tags
		//		+ ", request=" + request
		//		+ ", allocatedContainer=" + allocatedContainer
				+ "]";
	}
}
