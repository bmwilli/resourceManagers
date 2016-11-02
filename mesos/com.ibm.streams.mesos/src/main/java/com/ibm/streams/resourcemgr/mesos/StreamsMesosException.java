package com.ibm.streams.resourcemgr.mesos;

public class StreamsMesosException extends Exception {
	private static final long serialVersionUID = 525505L;
	
	public StreamsMesosException(String msg) {
		super(msg);
	}
	
	public StreamsMesosException(String msg, Throwable t) {
		super(msg,t);
	}
}
