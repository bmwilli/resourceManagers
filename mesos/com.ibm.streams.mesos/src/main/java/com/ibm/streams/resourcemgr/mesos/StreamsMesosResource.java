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
/**
 * Wraps a resource request from IBM Streams Resource Server
 *
 */

//import java.net.UnknownHostException;
import java.util.List;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import java.util.Set;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.Resource;

import com.ibm.streams.resourcemgr.ClientInfo;
import com.ibm.streams.resourcemgr.ResourceDescriptor;
import com.ibm.streams.resourcemgr.ResourceDescriptorState;
import com.ibm.streams.resourcemgr.ResourceManagerException;
import com.ibm.streams.resourcemgr.ResourceManagerUtilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import com.ibm.streams.resourcemgr.yarn.ContainerWrapper.ContainerWrapperState;

class StreamsMesosResource {
	private static final Logger LOG = LoggerFactory.getLogger(StreamsMesosResource.class);

	// Represents interpretation of Mesos Task
	public static enum ResourceState {
		NEW, LAUNCHED, RUNNING, STOPPING, STOPPED, FAILED, CANCELLED;
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
			case FAILED:
				return "FAILED";
			case CANCELLED:
				return "CANCELLED";
			default:
				throw new IllegalArgumentException();
			}
		}
	}
	
	// Represents standing with IBM Streams for this request
	public static enum RequestState {
		NEW, ALLOCATED, PENDING, CANCELLED, RELEASED;
		@Override
		public String toString() {
			switch(this) {
			case NEW:
				return "NEW";
			case PENDING:
				return "PENDING";
			case ALLOCATED:
				return "ALLOCATED";
			case CANCELLED:
				return "CANCELLED";
			case RELEASED:
				return "RELEASED";
			default:
				throw new IllegalArgumentException();
			}
		}
	}
	
	// Represents completion status for the task
	public static enum TaskCompletionStatus {
		NONE, FINISHED, FAILED, ERROR, KILLED, LOST;
		@Override
		public String toString() {
			switch(this) {
			case NONE:
				return "NONE";
			case FINISHED:
				return "FINISHED";
			case FAILED:
				return "FAILED";
			case ERROR:
				return "ERROR";
			case KILLED:
				return "KILLED";
			case LOST:
				return "LOST";
			default:
				throw new IllegalArgumentException();
			}
		}
	}

	// Unique Identier used for identification within Framework and for Mesos
	// Task ID
	private String _resourceId;
	// Client Info that requested this resource
	private ClientInfo _client;
	// Resource manager
	private StreamsMesosResourceManager _manager;
	// Streams related members
	private String _domainId = null;
	private String _zkConnect = null;
	// Need to understand more about how this is used within containers
	// Was getting error about key not found, os went with Yarn RM approach to test
	private String _homeDir = null;

	private ResourceState _resourceState = ResourceState.NEW;
	private RequestState _requestState = RequestState.NEW;
	private TaskCompletionStatus _taskCompletionStatus = TaskCompletionStatus.NONE;
	
	private Set<String> _tags = new HashSet<String>();

	private boolean _deployStreams;
	private Map<String, String> _argsMap;
	private List<Protos.CommandInfo.URI> _uriList;

	// Resource utilization related members
	private double _memory = -1;
	private double _cpu = -1;

	private boolean _isMaster = false;

	// Mesos Task related members
	private String _taskId = null;
	private String _hostName = null;
	
	
	
	//public StreamsMesosResource(String id, ClientInfo client, String domainId, String zk, int priority, Map<String, String> argsMap,
	public StreamsMesosResource(String id, ClientInfo client, StreamsMesosResourceManager manager, Map<String, String> argsMap,
			List<Protos.CommandInfo.URI> uriList) {
		this._resourceId = id;
		this._client = client;
		this._manager = manager;
		this._domainId = client.getDomainId();
		this._zkConnect = client.getZkConnect();
		//this.priority = priority;
		this._argsMap = argsMap;
		this._uriList = uriList;
		if (argsMap.containsKey(StreamsMesosConstants.HOME_DIR_ARG))
			_homeDir = argsMap.get(StreamsMesosConstants.HOME_DIR_ARG);
	}

	public String getId() {
		return _resourceId;
	}

	public String getDomainId() {
		return _domainId;
	}

	public String getZkConnect() {
		return _zkConnect;
	}

	public boolean isMaster() {
		return _isMaster;
	}

	public void setMaster(boolean isMaster) {
		this._isMaster = isMaster;
	}

	public double getMemory() {
		return _memory;
	}

	public void setMemory(double memory) {
		this._memory = memory;
	}

	public double getCpu() {
		return _cpu;
	}

	public void setCpu(double cpu) {
		this._cpu = cpu;
	}

	public Set<String> getTags() {
		return _tags;
	}
	
	
	
	/**
	 * @return the homeDir
	 */
	public String getHomeDir() {
		return _homeDir;
	}

	/**
	 * @param homeDir the homeDir to set
	 */
	public void setHomeDir(String homeDir) {
		this._homeDir = homeDir;
	}

	/**
	 * @return the state
	 */
	public ResourceState getResourceState() {
		return _resourceState;
	}

	/**
	 * @param state the state to set
	 */
	public void setResourceState(ResourceState state) {
		this._resourceState = state;
	}
	
	
	

	public RequestState getRequestState() {
		return _requestState;
	}

	public void setRequestState(RequestState _requestState) {
		this._requestState = _requestState;
	}

	public TaskCompletionStatus getTaskCompletionStatus() {
		return _taskCompletionStatus;
	}

	public void setTaskCompletionStatus(TaskCompletionStatus _taskCompletionStatus) {
		this._taskCompletionStatus = _taskCompletionStatus;
	}

	/**
	 * @return the taskId
	 */
	public String getTaskId() {
		return _taskId;
	}

	/**
	 * @param taskId the taskId to set
	 */
	public void setTaskId(String taskId) {
		this._taskId = taskId;
	}
	
	public String getHostName() {
		return _hostName;
	}

	public void setHostName(String hostName) {
		this._hostName = hostName;
	}

	public boolean isRunning() {
		// Fix for mesos
		return _resourceState == ResourceState.RUNNING;
	}
	
	public boolean isAllocated() {
		return _taskId != null;
	}

	//public void cancel() {
	//	_cancelled = true;
		// Fix for mesos
		// if(allocatedContainer!=null)
		// allocatedContainer.cancel();
	//}

	//public boolean isCancelled() {
	//	return _cancelled;
	//}

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
		return StreamsMesosResourceManager.getDescriptor(_resourceId, getHostName());
	}

	public ResourceDescriptorState getDescriptorState() {
		return StreamsMesosResourceManager.getDescriptorState(isRunning(), getDescriptor());
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

		if (_argsMap.containsKey(StreamsMesosConstants.DEPLOY_ARG)) {
			// run the streams resource installer
			// create softlink for StreamsLink
			cmdBuffer.append(";./StreamsResourceInstall/streamsresourcesetup.sh");
			cmdBuffer.append(" --install-dir ./InfoSphereStreams");
			cmdBuffer.append(" &> streamsinstall.out || exit 1");
			cmdBuffer.append(";cat ./InfoSphereStreams/.streams.version.dir | ");
			cmdBuffer.append(" xargs -I '{}' ln -s './InfoSphereStreams/{}' StreamsLink");
		} else {
			// if --deploy not set, we assume streams is installed on all
			// machines
			String streamsInstall = _argsMap.get(StreamsMesosConstants.INSTALL_PATH_ARG);
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
		cmdInfoBuilder.addAllUris(_uriList);

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
		
		// Set Host it will be run on
		setHostName(offer.getHostname());

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
	
	public void stop() {
		LOG.info("*** Stopping Resource: " + _resourceId);
		
		switch(_resourceState) {
		case NEW:
		case STOPPING:
		case STOPPED:
		case FAILED:
		case CANCELLED:
			return; // nothing to do
		default:
			break;
		}
		setResourceState(ResourceState.STOPPING);
		try {
			ResourceDescriptor rd = getDescriptor();
			LOG.info("Stopping Domain Controller: " + rd);
			boolean clean = false;
			//LOG.info("stopControllerCmd: " + ResourceManagerUtilities.getStopControllerCommandElements(
			//		_argsMap.get(StreamsMesosConstants.INSTALL_PATH_ARG), getZkConnect(), getDomainId(), rd, clean));
			ResourceManagerUtilities.stopController(getZkConnect(), getDomainId(), rd, clean);
		} catch (ResourceManagerException rme) {
			LOG.warn("Error shutting down resource streams controller: " + this, rme);
			rme.printStackTrace();
		}

		// No need to stop anything directly in Mesos since we used the Mesos CommandExecutor
		// Once the controller exits, the task should exit and status update will be reported
		// to the scheduler.
		
		// It is there that we will be notified that the task has stopped and will update the
		// _resourceState to STOPPED

	}
	
	
	public void notifyClientAllocated() {
		Collection<ResourceDescriptor> descriptors = new ArrayList<ResourceDescriptor>();
		descriptors.add(getDescriptor());
		LOG.info("Sending resourcesAllocated notification for resource: " + getId());
		_manager.getResourceNotificationManager().resourcesAllocated(_client.getClientId(), descriptors);
	}
	
	public void notifyClientRevoked() {
		Collection<ResourceDescriptor> descriptors = new ArrayList<ResourceDescriptor>();
		descriptors.add(getDescriptor());
		LOG.info("Sending resourcesRevoked notification for resource: " + getId());
		_manager.getResourceNotificationManager().resourcesRevoked(_client.getClientId(), descriptors);
	}

	@Override
	public String toString() {
		return "StreamsMesosResource [resourceId=" + _resourceId + 
				", resourceState=" + _resourceState + 
				", requestState=" + _requestState +
				", domainId=" + _domainId + 
				", zkConnect=" + _zkConnect + 
				", memory=" + _memory + 
				", cpu=" + _cpu + 
				", isMaster=" + _isMaster + 
				", tags=" + _tags + 
				", taskId=" + _taskId + 
				", hostName=" + _hostName +
				", taskCompletionStatus=" + _taskCompletionStatus +
				"]";
	}
}
