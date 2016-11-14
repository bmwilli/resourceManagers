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
	 * Framework that this scheduler communicates with The Framework is the
	 * bridge to the Streams Resource Server which receives the requests for
	 * resources
	 */
	StreamsMesosResourceFramework streamsRM;

	/**
	 * @param streamsRM
	 */
	public StreamsMesosResourceScheduler(StreamsMesosResourceFramework streamsRM) {
		super();
		this.streamsRM = streamsRM;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.mesos.Scheduler#disconnected(org.apache.mesos.SchedulerDriver)
	 */
	@Override
	public void disconnected(SchedulerDriver schedulerDriver) {
		LOG.info("We got disconnected");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.mesos.Scheduler#error(org.apache.mesos.SchedulerDriver,
	 * java.lang.String)
	 */
	@Override
	public void error(SchedulerDriver schedulerDriver, String s) {
		LOG.info("We got an error: " + s);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.mesos.Scheduler#executorLost(org.apache.mesos.SchedulerDriver,
	 * org.apache.mesos.Protos.ExecutorID, org.apache.mesos.Protos.SlaveID, int)
	 */
	@Override
	public void executorLost(SchedulerDriver schedulerDriver, ExecutorID executorID, SlaveID slaveID, int i) {
		LOG.info("Lost executor on slave " + slaveID);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.mesos.Scheduler#frameworkMessage(org.apache.mesos.
	 * SchedulerDriver, org.apache.mesos.Protos.ExecutorID,
	 * org.apache.mesos.Protos.SlaveID, byte[])
	 */
	@Override
	public void frameworkMessage(SchedulerDriver schedulerDriver, ExecutorID executorID, SlaveID slaveID,
			byte[] bytes) {
		LOG.info("Received message (scheduler);: " + new String(bytes) + " from " + executorID.getValue());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.mesos.Scheduler#offerRescinded(org.apache.mesos.
	 * SchedulerDriver, org.apache.mesos.Protos.OfferID)
	 */
	@Override
	public void offerRescinded(SchedulerDriver schedulerDriver, OfferID offerID) {
		LOG.info("This offer's been rescinded: " + offerID.toString());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.mesos.Scheduler#registered(org.apache.mesos.SchedulerDriver,
	 * org.apache.mesos.Protos.FrameworkID, org.apache.mesos.Protos.MasterInfo)
	 */
	@Override
	public void registered(SchedulerDriver schedulerDriver, FrameworkID frameworkID, MasterInfo masterInfo) {
		LOG.debug("Registered: " + frameworkID);

		// LOG.debug("*** StreamsMesosResourceScheduler Registered !!");
		// LOG.debug("*** Try and notify framework....");
		// LOG.debug("*** calling streamsRM.testMessage()...returned: " +
		// streamsRM.testMessage());

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.mesos.Scheduler#reregistered(org.apache.mesos.SchedulerDriver,
	 * org.apache.mesos.Protos.MasterInfo)
	 */
	@Override
	public void reregistered(SchedulerDriver schedulerDriver, MasterInfo masterInfo) {
		LOG.debug("Re-Registered");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.mesos.Scheduler#resourceOffers(org.apache.mesos.
	 * SchedulerDriver, java.util.List)
	 */
	@Override
	public void resourceOffers(SchedulerDriver schedulerDriver, List<Offer> offers) {
		LOG.debug("Resource Offers Made...");

		// Loop through offers, and exhaust the offer with resources we can
		// satisfy
		for (Protos.Offer offer : offers) {
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

			LOG.debug("OFFER: {cpu: " + offerCpus + ", mem: " + offerMem + ", id:" + offer.getId() + "}");
			// Eventually have a version of getNewSMR that allows cpu and memory
			// to be sent in so we can find the one that matches what we want
			StreamsMesosResource smr;
			while ((smr = streamsRM.getNewSMR()) != null) {
				boolean satisfiedRequest = false;
				LOG.info("There is a resource request waiting; StreamsMesosResource:");
				LOG.info(smr.toString());
				LOG.info("Should check to see if we can satisfy with what is left in the offer");
				
				usedOffer = true;
				
				Protos.TaskInfo task = smr.buildStreamsMesosResourceTask(offer);
				LOG.debug("Launching taskId: " + task.getTaskId() + "...");
				launchTask(schedulerDriver, offer, task);
				LOG.debug("...Launched taskId" + task.getTaskId());
				satisfiedRequest = true;

				if (satisfiedRequest) {
					// Tell resource manager we have satisfied the request and
					// status
					streamsRM.updateSMR(smr.getId(), StreamsMesosResource.StreamsMesosResourceState.LAUNCHED);
				}
			} // end while
				// If offer was not used at all, decline it
			if (!usedOffer) {
				LOG.debug("Offer was not used, declining");
				schedulerDriver.declineOffer(offer.getId());
			}
		} // end for
		LOG.debug("Finished handilng offers");
	}

	private void launchTask(SchedulerDriver schedulerDriver, Protos.Offer offer, Protos.TaskInfo task) {
		Collection<Protos.TaskInfo> tasks = new ArrayList<Protos.TaskInfo>();
		Collection<Protos.OfferID> offerIDs = new ArrayList<Protos.OfferID>();
		tasks.add(task);
		offerIDs.add(offer.getId());
		schedulerDriver.launchTasks(offerIDs, tasks);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.mesos.Scheduler#slaveLost(org.apache.mesos.SchedulerDriver,
	 * org.apache.mesos.Protos.SlaveID)
	 */
	@Override
	public void slaveLost(SchedulerDriver schedulerDriver, SlaveID slaveID) {
		LOG.info("Lost slave: " + slaveID.getValue());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.mesos.Scheduler#statusUpdate(org.apache.mesos.SchedulerDriver,
	 * org.apache.mesos.Protos.TaskStatus)
	 */
	@Override
	public void statusUpdate(SchedulerDriver schedulerDriver, TaskStatus taskStatus) {
		
		LOG.info("Mesos Task Status update: " + taskStatus.getState() + " from " + taskStatus.getTaskId().getValue());

		// Convert Mesos taskStatus to our own StreamsMesosResourceState
		StreamsMesosResource.StreamsMesosResourceState newState = null;
		
		switch (taskStatus.getState()) {
		case TASK_STAGING:
		case TASK_STARTING:
			newState = StreamsMesosResource.StreamsMesosResourceState.LAUNCHED;
			break;
		case TASK_RUNNING:
			newState = StreamsMesosResource.StreamsMesosResourceState.RUNNING;
			break;
		case TASK_FINISHED:
		case TASK_KILLED:
			newState = StreamsMesosResource.StreamsMesosResourceState.STOPPED;
			break;
		case TASK_ERROR:
			newState = StreamsMesosResource.StreamsMesosResourceState.FAILED;
			break;
		default:
			newState = null;
			break;
		}
		
		// Notify the StreamsMesosResourceFramework that a task status has
		// changed
		if (newState != null) {
			LOG.info("Mesos Task Status Update mapped to Resource State: " + newState.toString() );
			streamsRM.updateSMRbyTaskId(taskStatus.getTaskId().getValue(), newState);
		} else
			LOG.info("Mesos Task Status Update was not mapped to a Resource State, no action");
	}

}
