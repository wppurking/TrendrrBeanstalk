/**
 * 
 */
package com.trendrr.beanstalk;


/**
 * @author dustin
 *
 */
public class BeanstalkJob {

	private final byte[] data;
	private final long id;

	public BeanstalkJob(long id, byte[] data) {
	    this.id = id;
	    this.data = data;
	}
	
	public byte[] getData() {
		return this.data;
	}
	
	public long getId() {
		return this.id;
	}

    @Override
    public String toString() {
        return id + ": " + (data != null ? "" + data.length + " bytes": "null");
    }
	
	
}
