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
import org.apache.mesos.Protos.Resource;
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

		// Loop through offers, and exhaust the offer with resources we can satisfy
		for(Protos.Offer offer : offers) {
			boolean usedOffer = false;

			double offerCpus = 0;
    	double offerMem = 0;
    	// We always need to extract the resource info from the offer.
    	// It's a bit annoying in every language.
    	for (Resource r : offer.getResourcesList()) {
        if (r.getName().equals("cpus")) {
            offerCpus += r.getScalar().getValue();
        } else if (r.getName().equals("mem")) {
            offerMem += r.getScalar().getValue();
        }
    	}

			LOG.debug("OFFER id:" + offer.getId() + "; cpu: " + offerCpus + "; mem: " + offerMem);
			// Eventually have a version of getNewSMR that allows cpu and memory
			// to be sent in so we can find the one that matches what we want
			StreamsMesosResource smr;
			while ((smr = streamsRM.getNewSMR()) != null) {
				boolean satisfiedRequest = false;
				LOG.info("Received new StreamsMesosResource:");
				LOG.info(smr.toString());
				LOG.info("Should check to see if we can satisfy with what is left in the offer");
				if (taskIdCounter < 1) {
					usedOffer = true;
					LOG.debug("Launch 1 sample");
					Protos.TaskInfo task = buildStreamsMesosResourceTask(offer,smr);
					LOG.debug("Launching task #" + String.valueOf(taskIdCounter) + "...");
					launchTask(schedulerDriver, offer, task);
					LOG.debug("...Launched task #" + String.valueOf(taskIdCounter));
					satisfiedRequest = true;
				} else {
					LOG.debug("For testing we are just submitting one, so ignoring");
				}
				if (satisfiedRequest) {
					// Tell resource manager we have satisfied the request
					streamsRM.updateSMR(smr.getId());
				}
			} // while
			// If offer was not used at all, decline it
			if (!usedOffer) {
				LOG.info("Offer was not used, declining");
				schedulerDriver.declineOffer(offer.getId());
			}
		} // for
		LOG.info("Finished handilng offers");
	}


	// Sample build....DELETE ONCE WE CREATE OUR OWN
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


	private Protos.TaskInfo buildStreamsMesosResourceTask(Protos.Offer offer, StreamsMesosResource smr) {

		// get new way to do this from yarn
		Protos.TaskID taskId = buildNewTaskID();

		// Get the commandInfo from the Streams Mesos Resource
		Protos.CommandInfo commandInfo = smr.getStreamsResourceCommand();

		// Work on getting this fillout out correctly
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
