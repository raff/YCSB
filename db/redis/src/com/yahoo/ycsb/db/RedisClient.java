/**
 * Redis client binding for YCSB.
 *
 * Created by Raffaele Sena on 04/12/2011
 *
 */

package com.yahoo.ycsb.db;

import java.util.*;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

import redis.clients.jedis.*;

/**
* Redis client for YCSB framework.
*
* add -p redis.properties=true to the command line to see all available properties
*
* @author raff
*
*/
public class RedisClient extends DB {

    private static final String INDEX = "__index__";

    private Jedis jedis;

    /**
     * Initialize any state for this DB. Called once per DB instance; there is
     * one DB instance per client thread.
     */
    public void init() throws DBException {
        // initialize Redis driver
        Properties props = getProperties();
        String host = props.getProperty("redis.host", "localhost");
        int db = getIntProperty(props, "redis.db", 0);
	int port = Protocol.DEFAULT_PORT;

	boolean verbose = getBooleanProperty(props, "redis.properties", false);

	if (verbose) {
	    System.out.println("");
	    System.out.println("Redis driver properties:");
	    System.out.println("  jcr.host: " + host);
	    System.out.println("  jcr.db:   " + db);
	}

        try {
	    String parts[] = host.split(":");
	    if (parts.length > 1) {
		host = parts[0];
		port = Integer.parseInt(parts[1]);
	    }
		
	    jedis = new Jedis(host, port);
	    if (db != 0)
		jedis.select(db);	    
        } catch (Exception e) {
	    e.printStackTrace();
            throw new DBException(e);
        }
    }

    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    public void cleanup() throws DBException
    {
        try {
	   jedis.quit();
        } catch (Exception e) {
	    e.printStackTrace();
            throw new DBException(e);
        }
    }

    public boolean getBooleanProperty(Properties p, String name, boolean defaultValue)
    {
	String value = p.getProperty(name);
	return (value == null)
		?  defaultValue
		: value.equals("yes") || value.equals("true") || value.equals("1");
    }

    public int getIntProperty(Properties p, String name, int defaultValue)
    {
	String value = p.getProperty(name);
	if (value == null)
		return defaultValue;

	try {
		return Integer.parseInt(value);
	} catch(Exception e) {
		// should report error
		return defaultValue;
	}
    }

    /**
     * Utility method to return a Redis key
     */
    public String getRedisKey(String table, String key)
    {
	return table + ":" + key;
    }

    @Override
    /**
     * Delete database table
     *
     * @param table The name of the table
     */
    public int truncate(String table) {

	    try {
		jedis.del(table); // XXX
		jedis.del(INDEX);
		return 0;
	    } catch(Exception e) {
                //logger.error(e + "", e);
                return 1;
	    }
    }

    @Override
    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error. 
     *  See this class's description for a discussion of error codes.
     */
    public int delete(String table, String key) {
       
        try {
	    String rKey = getRedisKey(table, key);
	    jedis.del(rKey);
	    jedis.srem(INDEX, rKey);
	    return 0;
        } catch (Exception e) {
            //logger.error(e + "", e);
            return 1;
        }
    }


    @Override
    /**
     * Insert a record in the database. 
     * Any field/value pairs in the specified values HashMap will be written 
     * into the record with the specified record key.
     *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error. 
     *  See this class's description for a discussion of error codes.
     */
    public int insert(String table, String key, HashMap<String, String> values) {
        try {
	    String rKey = getRedisKey(table, key);
	    jedis.hmset(rKey, values);
	    jedis.sadd(INDEX, rKey);
	    return 0;
        } catch (Exception e) {
            //logger.error(e + "", e);
            return 1;
        }

    }

    @Override
    @SuppressWarnings("unchecked")
    /**
     * Read a record from the database. 
     * Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    public int read(String table, String key, Set<String> fields,
            HashMap<String, String> result) {
        
        try {
	    String rKey = getRedisKey(table, key);

	    if (fields == null) {
		    jedis.hgetAll(rKey);
	    } else {
		    jedis.hmget(rKey, fields.toArray(new String[0]));
	    }
	    return 0;
        } catch (Exception e) {
            //logger.error(e + "", e);
            return 1;
        }

    }


    @Override
    /**
     * Update a record in the database. 
     * Any field/value pairs in the specified values HashMap will be written 
     * into the record with the specified record key, overwriting any existing 
     * values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    public int update(String table, String key, HashMap<String, String> values) {
     
        try {
	    String rKey = getRedisKey(table, key);
	    jedis.hmset(rKey, values);
	    return 0;
        } catch (Exception e) {
            //logger.error(e + "", e);
            return 1;
        }

    }

    @Override
    @SuppressWarnings("unchecked")
    /**
     * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param fields The list of fields to read, or null for all of them
     * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    public int scan(String table, String startkey, int recordcount,
            Set<String> fields, Vector<HashMap<String, String>> result) {

        try {
	    String rKey = getRedisKey(table, startkey);

	    	// Do I really need to sort the full list
		// just to get the subset ?
	    SortingParams params = new SortingParams().alpha();
	    List<String> keys = jedis.sort(INDEX, params);
	    int startIndex = keys.indexOf(rKey);

	    String fieldsArray[] = fields.toArray(new String[0]);

	    for (int i=0; i < recordcount; i++) {
		String key = keys.get(i);
		List<String> values = jedis.hmget(key, fieldsArray);

		HashMap<String, String> record = new HashMap<String, String>();

		for (int j=0; i < fieldsArray.length; j++) {
			record.put(fieldsArray[j], values.remove(0));
		}

		result.add(record);
	    }

            return 0;
        } catch (Exception e) {
            //logger.error(e + "", e);
            return 1;
        }
    }

}
