/**
 * 
 */
package com.ibm.streams.resourcemgr.mesos;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

/**
 * @author bmwilli
 *
 */
public class StreamsMesosResourceScheduler implements Scheduler {
	
	private static final Logger LOG = LoggerFactory.getLogger(StreamsMesosResourceScheduler.class);
	
	private int taskIdCounter = 0;
	
	/**
	 * Framework that this scheduler communicates with
	 * The Framework is the bridge to the Streams Resource Server
	 * which receives the requests for resources
	 */
	StreamsMesosResourceFramework streamsRM;
	
	

	/**
	 * @param streamsRM
	 */
	public StreamsMesosResourceScheduler(StreamsMesosResourceFramework streamsRM) {
		super();
		this.streamsRM = streamsRM;
	}

	/* (non-Javadoc)
	 * @see org.apache.mesos.Scheduler#disconnected(org.apache.mesos.SchedulerDriver)
	 */
	@Override
	public void disconnected(SchedulerDriver schedulerDriver) {
		LOG.info("We got disconnected");
	}

	/* (non-Javadoc)
	 * @see org.apache.mesos.Scheduler#error(org.apache.mesos.SchedulerDriver, java.lang.String)
	 */
	@Override
	public void error(SchedulerDriver schedulerDriver, String s) {
		LOG.info("We got an error: " + s);
	}

	/* (non-Javadoc)
	 * @see org.apache.mesos.Scheduler#executorLost(org.apache.mesos.SchedulerDriver, org.apache.mesos.Protos.ExecutorID, org.apache.mesos.Protos.SlaveID, int)
	 */
	@Override
	public void executorLost(SchedulerDriver schedulerDriver, ExecutorID executorID, SlaveID slaveID, int i) {
		LOG.info("Lost executor on slave " + slaveID);
	}

	/* (non-Javadoc)
	 * @see org.apache.mesos.Scheduler#frameworkMessage(org.apache.mesos.SchedulerDriver, org.apache.mesos.Protos.ExecutorID, org.apache.mesos.Protos.SlaveID, byte[])
	 */
	@Override
	public void frameworkMessage(SchedulerDriver schedulerDriver, ExecutorID executorID, SlaveID slaveID, byte[] bytes) {
		LOG.info("Received message (scheduler);: " + new String(bytes) + " from " + executorID.getValue());
	}

	/* (non-Javadoc)
	 * @see org.apache.mesos.Scheduler#offerRescinded(org.apache.mesos.SchedulerDriver, org.apache.mesos.Protos.OfferID)
	 */
	@Override
	public void offerRescinded(SchedulerDriver schedulerDriver, OfferID offerID) {
		LOG.info("This offer's been rescinded: " + offerID.toString());
	}

	/* (non-Javadoc)
	 * @see org.apache.mesos.Scheduler#registered(org.apache.mesos.SchedulerDriver, org.apache.mesos.Protos.FrameworkID, org.apache.mesos.Protos.MasterInfo)
	 */
	@Override
	public void registered(SchedulerDriver schedulerDriver, FrameworkID frameworkID, MasterInfo masterInfo) {
		LOG.debug("Registered: " + frameworkID);
		
		//LOG.debug("*** StreamsMesosResourceScheduler Registered !!");
		//LOG.debug("*** Try and notify framework....");
		//LOG.debug("*** calling streamsRM.testMessage()...returned: " + streamsRM.testMessage());

	}

	/* (non-Javadoc)
	 * @see org.apache.mesos.Scheduler#reregistered(org.apache.mesos.SchedulerDriver, org.apache.mesos.Protos.MasterInfo)
	 */
	@Override
	public void reregistered(SchedulerDriver schedulerDriver, MasterInfo masterInfo) {
		LOG.debug("Re-Registered");
	}

	/* (non-Javadoc)
	 * @see org.apache.mesos.Scheduler#resourceOffers(org.apache.mesos.SchedulerDriver, java.util.List)
	 */
	@Override
	public void resourceOffers(SchedulerDriver schedulerDriver, List<Offer> offers) {
		LOG.debug("Resource Offers Made...");
		for(Protos.Offer offer : offers) {
			//LOG.debug("OFFER: " + offer.toString());
			if (taskIdCounter < 1) {
				Protos.TaskInfo task = buildNewTask(offer);
				LOG.debug("Launching task #" + String.valueOf(taskIdCounter) + "...");
				launchTask(schedulerDriver, offer, task);
				LOG.debug("...Launched task #" + String.valueOf(taskIdCounter));
			} else {
				// Decline offer
				LOG.debug("Declining offers");
				schedulerDriver.declineOffer(offer.getId());
			}
		}
	}
	
	private Protos.TaskInfo buildNewTask(Protos.Offer offer) {
		
		Protos.TaskID taskId = buildNewTaskID();
		
		// Get the commandInfo from the Framework
		Protos.CommandInfo commandInfo = streamsRM.getStreamsResourceCommand();


		Protos.TaskInfo task = Protos.TaskInfo.newBuilder()
				.setName("task " + taskId)
				.setTaskId(taskId)
				.setSlaveId(offer.getSlaveId())
				.addResources(buildResource("cpus",1))
				.addResources(buildResource("mem",128))
				.setData(ByteString.copyFromUtf8("" + taskIdCounter))
				.setCommand(Protos.CommandInfo.newBuilder(commandInfo))
				.build();	
		
		return task;
	}
	
	private Protos.TaskID buildNewTaskID() {
		return Protos.TaskID.newBuilder()
				.setValue(Integer.toString(taskIdCounter++)).build();
	}
	
	private Protos.Resource buildResource(String name, double value) {
		return Protos.Resource.newBuilder()
				.setName(name)
				.setType(Protos.Value.Type.SCALAR)
				.setScalar(buildScalar(value)).build();
	}
	
	private Protos.Value.Scalar.Builder buildScalar(double value) {
		return Protos.Value.Scalar.newBuilder().setValue(value);
	}
	
	private void launchTask(SchedulerDriver schedulerDriver, Protos.Offer offer, Protos.TaskInfo task) {
		Collection<Protos.TaskInfo> tasks = new ArrayList<Protos.TaskInfo>();
		Collection<Protos.OfferID> offerIDs = new ArrayList<Protos.OfferID>();
		tasks.add(task);
		offerIDs.add(offer.getId());
		schedulerDriver.launchTasks(offerIDs, tasks);
	}

	/* (non-Javadoc)
	 * @see org.apache.mesos.Scheduler#slaveLost(org.apache.mesos.SchedulerDriver, org.apache.mesos.Protos.SlaveID)
	 */
	@Override
	public void slaveLost(SchedulerDriver schedulerDriver, SlaveID slaveID) {
		LOG.info("Lost slave: " + slaveID.getValue());
	}

	/* (non-Javadoc)
	 * @see org.apache.mesos.Scheduler#statusUpdate(org.apache.mesos.SchedulerDriver, org.apache.mesos.Protos.TaskStatus)
	 */
	@Override
	public void statusUpdate(SchedulerDriver schedulerDriver, TaskStatus taskStatus) {
		LOG.info("Status update: " + taskStatus.getState() + " from " + taskStatus.getTaskId().getValue());
	}

}
