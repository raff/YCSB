/**
 * JCR client binding for YCSB.
 *
 * Created by Raffaele Sena on 03/17/2011
 *
 */

package com.yahoo.ycsb.db;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

import javax.jcr.*;
import org.apache.jackrabbit.commons.JcrUtils;

/**
* JCR client for YCSB framework.
*
* add -p jcr.properties=true to the command line to see all available properties
*
* @author raff
*
*/
public class JCRClient extends DB {

    private static final Logger logger = LoggerFactory.getLogger(JCRClient.class);

    private Session session;
    private Node dbNode;
    private boolean writeCommit;

    /**
     * Initialize any state for this DB. Called once per DB instance; there is
     * one DB instance per client thread.
     */
    public void init() throws DBException {
        // initialize JCR driver
        Properties props = getProperties();
        String url = props.getProperty("jcr.url", "http://localhost:8080/rmi");
        String workspace = props.getProperty("jcr.workspace", "default");
	String username = props.getProperty("jcr.username", "admin");
	String password = props.getProperty("jcr.password", "admin");
        String database = props.getProperty("jcr.database", "ycsb");

	writeCommit = getBooleanProperty(props, "jcr.writeCommit", true);
	boolean verbose = getBooleanProperty(props, "jcr.properties", false);

	if (verbose) {
	    System.out.println("");
	    System.out.println("JCR driver properties:");
	    System.out.println("  jcr.url:         " + url);
	    System.out.println("  jcr.workspace:   " + workspace);
	    System.out.println("  jcr.username:    " + username);
	    System.out.println("  jcr.password:    " + password);
	    System.out.println("  jcr.database:    " + database);
	    System.out.println("  jcr.writeCommit: " + writeCommit);
	    System.out.println("");
	    System.out.println("For CRX use:");
            System.out.println("  jcr.url:         rmi://localhost:1234/crx");
            System.out.println("  jcr.workspace:   crx.default");
	}

        try {
		
	    Repository repository = JcrUtils.getRepository(url);
	    session = repository.login(
		new SimpleCredentials(username,password.toCharArray()), workspace);

	    Node root = session.getRootNode();
	    if (root.hasNode(database)) {
		dbNode = root.getNode(database);
	    } else {
		dbNode = root.addNode(database);
		session.save();
	    }

        } catch (Exception e) {
	    if (session != null) {
		try {
			session.logout();
		} catch(Exception e1) {
			logger.warn("Could not release JCR session", e);
		}
	    }

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
	   if (session.hasPendingChanges())
		session.save();

	   session.logout();
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

    /**
     * Utility method to return a "table" node
     */
    public Node getTableNode(String table) throws Exception
    {
	Node tableNode = null;

	if (dbNode.hasNode(table))
	    tableNode = dbNode.getNode(table);
	else {
	    tableNode = dbNode.addNode(table);
	    session.save();
	}

	return tableNode;
    }

    public void commit() throws Exception {
	if (writeCommit) session.save();
    }

    @Override
    /**
     * Delete database table
     *
     * @param table The name of the table
     */
    public int truncate(String table) {

	    try {
		if (dbNode.hasNode(table)) {
		    Node tableNode = dbNode.getNode(table);
		    tableNode.remove();
		    session.save();
		}

		return 0;
	    } catch(Exception e) {
                logger.error(e + "", e);
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
	    Node tableNode = getTableNode(table);

	    if (tableNode.hasNode(key)) {
		Node n = tableNode.getNode(key);
		n.remove();
		commit();
	        return 0;
	    } else {
		return 1;
	    }

        } catch (Exception e) {
            logger.error(e + "", e);
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
	    Node tableNode = getTableNode(table);
	    if (tableNode.hasNode(key))
		return 1; // record already exists

	    Node n = tableNode.addNode(key);

	    for (Map.Entry<String, String> e : values.entrySet())
		n.setProperty(e.getKey(), e.getValue());
	    
	    commit();
	    return 0;
        } catch (Exception e) {
            logger.error(e + "", e);
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
	    Node tableNode = getTableNode(table);
	    Node n = tableNode.getNode(key);

	    if (fields == null) {
		// return all properties
		PropertyIterator i = n.getProperties();
		while (i.hasNext()) {
			Property p = i.nextProperty();
			String name = p.getName();
			if (! name.startsWith("jcr:"))
				result.put(name, p.getString());
		}
	    } else {
		// return specified properties
		for (String f : fields)
			result.put(f, n.getProperty(f).getString());
	    }

	    return 0;
        } catch (Exception e) {
            logger.error(e + "", e);
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
	    Node tableNode = getTableNode(table);
	    if (!tableNode.hasNode(key))
		return 1; // record doesn't exists

	    Node n = tableNode.getNode(key);

	    for (Map.Entry<String, String> e : values.entrySet())
		n.setProperty(e.getKey(), e.getValue());
	    
	    commit();
	    return 0;
        } catch (Exception e) {
            logger.error(e + "", e);
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
/*       
        try {
	    Node tableNode = getTableNode(table);
	    NodeIterator nodes = tableNode.getNodes(key + "*");

	    if (fields == null) {
		// return all properties
		PropertyIterator i = n.getProperties();
		while (i.hasNext()) {
			Property p = i.nextProperty();
			result.put(p.getName(), p.getString());
		}
            return 0;
        } catch (Exception e) {
            logger.error(e + "", e);
            return 1;
        }
*/
	logger.warn("JCR does not support Scan semantics");
	return 1;
	
    }

}
