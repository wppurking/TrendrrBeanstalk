/**
 * 
 */
package com.trendrr.beanstalk;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * @author dustin
 *
 */
public class BeanstalkPool {

	private static final long DEFAULT_MAX_USE_TIME = 20*60*1000; //max checkout time is 20 minutes.
    private static final long DEFAULT_MAX_IDLE_TIME = 20*60*1000;

    protected final Log log = LogFactory.getLog(BeanstalkPool.class);
	
	protected final Set<PoolClient> clients = new HashSet<PoolClient>();
	private final int maxClients;
	
	private final long maxUseTime; 
	private final long maxIdleTime; //connection will be removed after no use.
	
	private final String addr;
	private final int port;
	private final String tube;
    
    private class PoolClient extends BeanstalkClient {
        private long inUseSince;
        private long lastUsed;
      
        public PoolClient(String addr, int port, String tube) {
            super(addr, port, tube);
        }
        
        @Override
        public void close() {
            // mark client as unused
            inUseSince = 0;   
        }
    }
	
	/**
	 * setup a new pool.  
	 * 
	 * @param addr address of the beanstalkd server to connection to
	 * @param port port of the beanstalkd server
	 * @param maxPoolSize maximum number of clients allowed in the pool (0 for infinity)
	 * @param tube All operations for the client will work on the tube.
	 */
	public BeanstalkPool(String addr, int port, int maxPoolSize, String tube) {
	    this(addr, port, maxPoolSize, tube, DEFAULT_MAX_USE_TIME, DEFAULT_MAX_IDLE_TIME);
	}
	
	public BeanstalkPool(String addr, int port, int maxPoolSize, String tube, long  maxUseTime, long maxIdleTime) {
	        this.addr = addr;
	        this.port = port;
	        this.maxClients = maxPoolSize;
	        this.tube = tube;
	        this.maxUseTime = maxUseTime;
	        this.maxIdleTime = maxIdleTime;
	}
	
    /**
	 * setup a new pool.  
	 * 
	 * @param addr address of the beanstalkd server to connection to
	 * @param port port of the beanstalkd server
	 * @param maxPoolSize maximum number of clients allowed in the pool (0 for infinity)
	 */
	public BeanstalkPool(String addr, int port, int maxPoolSize) {
		this(addr, port, maxPoolSize, null);
	}
	
	/**
	 * returns the number of active clients in the pool
	 * @return
	 */
	public int getPoolSize() {
		return this.clients.size();
	}
	
	/**
	 * 
	 * This gets a client from the pool.  will throw a BeanstalkException if 
	 * there are more then the maximum number of clients checked out. 
	 * 
	 * @return
	 */
	public synchronized BeanstalkClient getClient() throws BeanstalkException {
		/*
		 * synchronized, but should be fast as the client initialization code happens lazily. 
		 */
		
		Set<PoolClient> toRemove = new HashSet<PoolClient>();
		
		long max = getCurrentTime() - this.maxUseTime;
		long maxIdle = getCurrentTime() - this.maxIdleTime;
		
		BeanstalkClient returnClient = null;
		
		/*
		 * Here we iterate over all the clients and reap any that need reaping. 
		 * TODO: we could restrict this to only loop over once every minute or so.
		 * for now I don't see it being a huge problem.
		 */
		for (PoolClient client : clients) {
			if (client.inUseSince != 0 && client.inUseSince < max) {
				client.reap = true;
			}	
			if (client.lastUsed != 0 && client.lastUsed < maxIdle) {
				client.reap = true;
			}
			if (client.con != null && ! client.con.isOpen()) {
				client.reap = true;
			}
			
			
			if (client.reap) {
				toRemove.add(client);
			} else if (returnClient == null && client.inUseSince == 0) {
				//found the useable client.
				client.inUseSince = getCurrentTime();
				client.lastUsed = getCurrentTime(); //reap old connections
				returnClient = client;
			}
		}
		for (BeanstalkClient c : toRemove) {
			log.debug("REAPING Client: " + c);
			this.clients.remove(c);
			// don't close client
		}
		if (returnClient != null) {
			return returnClient;
		}
		
		//add a new client if they are all closed.
		if (this.maxClients > 0 && this.clients.size() >= this.maxClients) {
			log.error("Too many clients in use!");	
			throw new BeanstalkException("To many clients in use");
		}
		PoolClient client = new PoolClient(this.addr, this.port, this.tube);
	
		this.clients.add(client);
		client.inUseSince = getCurrentTime();
		client.lastUsed = getCurrentTime();
		return client;
	}

    protected long getCurrentTime() {
        return System.currentTimeMillis();
    }
}
