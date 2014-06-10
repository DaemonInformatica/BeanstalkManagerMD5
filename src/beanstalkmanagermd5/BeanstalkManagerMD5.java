/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package beanstalkmanagermd5;

import dk.safl.beanstemc.Beanstemc;
import dk.safl.beanstemc.BeanstemcException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import org.json.simple.JSONObject;

/**
 *
 * @author matrix
 */
public class BeanstalkManagerMD5 
{
    private                 Connection  m_conn;
    private final           String      m_user              = "root";
    private final           String      m_password          = "";
    private final           String      m_dbName            = "dbMD5Decode";
    private static final    int         LONG_JOB            = 5000;
    private static final    int         SHORT_JOB           = 2000;
    private static final    String      LONG_JOB_TUBENAME   = "longtube";
    private static final    String      SHORT_JOB_TUBENAME  = "shorttube";
    private static final    int         MAX_JOBS_FOR_USER   = 5;
    private static final    int         LOOP_SLEEP_INTERVAL = 10;
    private                 Beanstemc   m_beanstemc         = null;
    
    public BeanstalkManagerMD5() 
    {
        boolean bConnect    = connect();
        boolean bBeanstalk  = initBeanstalk();
        
        if(bConnect && bBeanstalk)
        {
            purgeOldJobs();
            System.out.println("Initialization complete. Starting loop..");
            loop();
        }
        else
        {
            System.out.println("Initialization failed. Exiting..");
        }
    }
    
    private boolean initBeanstalk()
    {
        System.out.print("initializing beanstalk... ");
        try
        {
            m_beanstemc = new Beanstemc("localhost", 9000);
        }
        
        catch(UnknownHostException uhE) 
        { 
            System.err.println("Unknown host!"); 
            return false;
        }
        catch(IOException ioE)          
        {
            System.err.println("IOError trying to initialize beanstalk."); 
            return false;
        }
        
        System.out.println("Done.");
        
        return true;
    }
    
    private boolean connect()
    {
        System.out.print("Initializing database connection.... ");
        
        try 
        {
            Class.forName("com.mysql.jdbc.Driver"); 
            
            m_conn = DriverManager.getConnection("jdbc:mysql://localhost/" 
                    + m_dbName + "?" 
                    + "user=" + m_user 
                    + "&password=" + m_password);
            
        } 
        catch (SQLException ex) 
        {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
            
            return false;
        }    
        catch(ClassNotFoundException cE)
        {
            System.err.println("Class not found exception: " + cE.toString());
            return false;
        }
        
        System.out.println("Done.");
        return true;
    }

    private void purgeOldJobs()
    {
        String      sql = "UPDATE tblMD5Process SET status='waiting' WHERE status <> 'completed'";
        ResultSet   rsWaiting;
        
        try
        {
            PreparedStatement pSt = m_conn.prepareStatement(sql);
            pSt.execute();
            
            // after any jobs are set to waiting, they'll have to be re-submitted.
            // get waiting jobs. 
                                sql         = "SELECT id, start, end FROM tblMD5Process WHERE status='waiting'";
            PreparedStatement   pStWaiting  = m_conn.prepareStatement(sql);
                                rsWaiting   = pStWaiting.executeQuery();

            // for each job waiting: 
            System.out.println("Pushing old job...");
            while(rsWaiting.next())
            {
                // gather parameters. 
                int     md5ID   = rsWaiting.getInt(1);
                String  start   = rsWaiting.getString(2);
                String  end     = rsWaiting.getString(3);

                // call pushbeanjob. 
                pushBeanJob(md5ID, start, end, true);
            }
            
            pSt.close();
            rsWaiting.close();
        }
        catch(SQLException sqlE)
        {
            System.err.println("purgeOldJobs: SQL Exception: " + sqlE.toString());   
            return;
        }    
    }
    
    
    private ArrayList<Integer> getUncrackedMD5IDs() throws SQLException
    {
        ArrayList<Integer>  arrIDs  = new ArrayList<Integer>();
        String              sql     = "SELECT id FROM tblMD5 WHERE md5decoded=''";
        Statement           st      = m_conn.createStatement();
        ResultSet           rs      = st.executeQuery(sql);
        
        while(rs.next())
        {
            int id = rs.getInt(1);
            arrIDs.add(id);
        }
        
        st.close();
        rs.close();
        
        return arrIDs;
    }
    
    private String getInitialStart(int md5ID) throws SQLException
    {
        String              start   = "";
        String              sql     = "SELECT start FROM tblMD5 WHERE id=?;";
        PreparedStatement   st      = m_conn.prepareStatement(sql);
        
        st.setInt(1, md5ID);
        ResultSet rs = st.executeQuery();
        
        if(rs.next())
            start = rs.getString(1);
        
        st.close();
        rs.close();
        
        return start;
    }
    
    private String getLastSetEnd(int md5ID)
    {
        PreparedStatement   st;
        String              str = "";
        
        try
        {
            String sql  = "SELECT strEnd FROM tblMD5Process WHERE md5ID=? ORDER BY created DESC";
            st          = m_conn.prepareStatement(sql);
            
            st.setInt(1, md5ID);
            
            ResultSet rs = st.executeQuery();
            
            if(rs.next())
                str = rs.getString(1);
            else
                str = getInitialStart(md5ID);
            
            st.clearBatch();
            st.close();
            rs.close();
        }
        catch(SQLException sqlE) 
        {
            System.err.println("Error fetching strEnd for md5: " + md5ID + ": " + sqlE.toString());
        }
        
        return str;
    }
    
    private String[] calcNewSet(String prevEndString, boolean bLongSet)
    {
                prevEndString       = prevEndString.length() == 0 ? "!" : prevEndString;
        String  newSet[]            = new String[2];
        String  strStart            = "";
        String  strEnd              = "";
        int     prevEndLen          = prevEndString.length();
        byte    prevEndByteArray[]  = new byte[prevEndLen];
        
        // split the prevEndString into a byte array. 
        for(int i = 0; i <= prevEndLen - 1; i++)
            prevEndByteArray[i] = (byte)prevEndString.charAt(i);
        
        // generate two new byte arrays of appropriate length
        int newLen          = prevEndByteArray[0] == 122 ? prevEndLen + 1 : prevEndLen;
        byte byteArrStart[] = new byte[newLen];
        byte byteArrEnd[];
        
        // if the first byte is already at its max (122) set all bytes to the first value (33).
        if(newLen > prevEndLen)
        {
            for(int i = 0; i < newLen; i++)
                byteArrStart[i] = 33;
        }
        else
        {   
            for(int i = newLen - 1; i >= 0; i--)
                byteArrStart[i] = prevEndByteArray[i];
        }
        
        // fill byte array 2 with an offset appropriate for long job or not. 
                byteArrEnd  = byteArrStart.clone();
        int     jobSize     = bLongSet == true ? LONG_JOB : SHORT_JOB;
        
        while(jobSize > 0 && byteArrEnd[0] != 122)
        {
            // run through the entire byte array to see if values have to be shifted. 
            for(int i = byteArrEnd.length - 1; i > 0 ; i--)
            {
                if(byteArrEnd[i] == 122)
                {
                    byteArrEnd[i - 1]  += 1;
                    byteArrEnd[i]       = 33;
                }
            }

            // get the first byte. 
            byte currB = byteArrEnd[byteArrEnd.length - 1]; 
            
            // calculate update space for this byte 
            byte space = (byte)(122 - currB);
            
            // update the byte and update jobSize. 
            byte updateValue                    = jobSize > space ? space : (byte)jobSize;
            byteArrEnd[byteArrEnd.length - 1]  += updateValue;
            jobSize                            -= updateValue;
        }
        
        // convert both arrays to a string and write to newSet. 
        try
        {
            strStart    = new String(byteArrStart, "utf-8");
            strEnd      = new String(byteArrEnd, "utf-8");
        }
        catch(UnsupportedEncodingException ueE)
        {
            System.err.println("Encountered UnsupportedEncodingException: " + ueE.toString());
        }
        
        newSet[0] = strStart;
        newSet[1] = strEnd;
        
        // return the new set. 
        return newSet;
    }
    
    private int insertProcess(int md5ID, String start, String end, String tubename, int ownerID) throws SQLException
    {
        // sql
        /*
         tblMD5Process
            - id        INT PRIMARY KEY AUTO_INCREMENT
            - md5ID     INT
            - status    ENUM('waiting', 'processing', 'completed')
            - strStart  VARCHAR(40)
            - strEnd    VARCHAR(40)
            - tsStart   BIGINT
            - tsEnd     BIGINT
            - tubeName  VARCHAR(32)
            - created   DATETIME
            - updated   DATETIME
            - createdBy INT
            - updatedBy INT

         */
        String              sql         = "INSERT INTO tblMD5Process VALUES(0, ?, 'waiting', ?, ?, 0, 0, ?, ?, NULL, ?, 0);";
        PreparedStatement   pSt         = m_conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);        
        Date                created     = new Date();
        Object              pCreated    = new java.sql.Timestamp(created.getTime());
        
        pSt.setInt(1, md5ID);       // md5ID
        pSt.setString(2, start);    // strStart
        pSt.setString(3, end);      // strEnd 
        pSt.setString(4, tubename); // tubeName
        pSt.setObject(5, pCreated); // created
        pSt.setInt(6, ownerID);     // createdBy
        
        // execute
        pSt.executeUpdate();
        
        ResultSet   rs = pSt.getGeneratedKeys();
        int         id = 0;
        
        if(rs.next())    
            id = rs.getInt(1);
        
        pSt.close();
        rs.close();
        
        return id;
    }
    
    private int getUserIDByMd5ID(int md5ID) throws SQLException
    {
        // int userID = 0;
        String              sql = "SELECT createdBy FROM tblMD5 WHERE id=?";
        PreparedStatement   pSt = m_conn.prepareStatement(sql);
        
        pSt.setInt(1, md5ID);
        
        ResultSet   rs      = pSt.executeQuery();
        int         userID  = rs.next() == true ? rs.getInt(1) : 0;
        
        pSt.close();
        rs.close();
        
        return userID;
    }
    
    
    private String getMD5ByID(int id)
    {
        String sql = "SELECT md5string FROM tblMD5 WHERE id=?";
        String md5 = "";
        
        try
        {
            PreparedStatement st = m_conn.prepareStatement(sql);
            
            st.setInt(1, id);
            
            ResultSet rs = st.executeQuery();
            
            if(rs.next())
                md5 = rs.getString(1);
            
            st.close();
            rs.close();
            
        }
        catch(SQLException sqlE)
        {
            System.err.println("Error fetching md5 for record " + id + ": " + sqlE.toString());
        }
        
        return md5;
    }
    
    private byte[] stringToByteArr(String s)
    {
        int len     = s.length();
        byte data[] = s.getBytes();
        
        return data;
    }
    
    private void pushToBeanstalk(String data, String tubename)
    {
        try
        {            
            // create JSON object to send as result. 
            byte ba[]   = stringToByteArr(data);

            m_beanstemc.use(tubename);
            m_beanstemc.put(ba);
        }
        catch(UnknownHostException uhE) { System.err.println("Unknown host: " + uhE.toString()); } 
        catch(IOException ioE)          { System.err.println("IOException: " + ioE.toString()); } 
        catch(BeanstemcException bsE)   { System.err.println("Beanstalk Exception: " + bsE.toString()); } 
    }

    private void pushBeanJob(int md5ID, String start, String end, boolean bLongJob)
    {
        // get the MD5 version of the password
        String md5 = getMD5ByID(md5ID);
        
        // create a JSON string. 
        JSONObject obj = new JSONObject();
        
        obj.put("id",       md5ID);
        obj.put("md5",      md5);
        obj.put("start",    start);
        obj.put("end",      end);
        
        // create a new beanstalk job. 
        String data     = obj.toJSONString();
        String tubename = bLongJob == true ?  LONG_JOB_TUBENAME : SHORT_JOB_TUBENAME;
        
        pushToBeanstalk(data, tubename);
    }
    
    private int countRunningJobsByUserID(int userID)
    {
        String  sql     = "SELECT COUNT(id) FROM tblMD5Process WHERE createdBy=? and status <> 'completed'";
        int     count   = 999999;
        
        try
        {
            PreparedStatement st = m_conn.prepareStatement(sql);

            st.setInt(1, userID);

            ResultSet rs    = st.executeQuery();
            count           = 0;

            if(rs.next())
                count = rs.getInt(1);
            
            st.close();
            rs.close();
        }
        catch(SQLException sqlE)
        {
            System.err.println("SQLException encountered counting running jobs for user " + userID + ": " + sqlE.toString());
        }
        
        return count;
        
    }
    
    private void loop()
    {
        List<Integer> arrIDs = new ArrayList<Integer>();
        
        while(true)
        {
            // cleanup the arrIDs list. 
            arrIDs.clear();
            
            // for each uncracked md5: 
            try
            {
                arrIDs = getUncrackedMD5IDs();
            }
            catch(SQLException sqlE)
            {
                System.err.println("SQL Exception caught when getting md5ID's: " + sqlE.toString());
                return;
            }

            Iterator<Integer> iI = arrIDs.iterator();

            try
            {
                while(iI.hasNext())
                {
                    // if current running processes >= max processes for owner: continue
                    int md5ID           = iI.next();
                    int userID          = getUserIDByMd5ID(md5ID);
                    int currentRunning  = countRunningJobsByUserID(userID);

                    // System.out.println("md5ID: " + md5ID + "; jobs for userID: " + userID + ": " + currentRunning);
                    
                    if(currentRunning >= MAX_JOBS_FOR_USER)
                        continue;

                    // get the last process. 
                    String lastStr  = getLastSetEnd(md5ID);

                    // calculate new set. 
                    String newSet[] = calcNewSet(lastStr, true);

                    // write it to the database. 
                    int procID;
                    
                    try
                    {
                        int ownerID = getUserIDByMd5ID(md5ID);
                            procID  = insertProcess(md5ID, newSet[0], newSet[1], LONG_JOB_TUBENAME, ownerID);
                    }
                    catch(SQLException sqlE)
                    {
                        System.err.println("SQL Exception, trying to execute inserting a new process row: " + sqlE.toString());
                        return;
                    }

                    // push it on the beanstalk. 
                    System.out.println("Pushing new job to beanstalk: md5ID: " + md5ID + " - setStart: " + newSet[0] + " - setEnd: " + newSet[1]);
                    pushBeanJob(procID, newSet[0], newSet[1], true);
                }
            }
            catch(SQLException sqlE)
            {
                System.err.println("SQLException in main loop: " + sqlE.toString());
            }
            catch(OutOfMemoryError oomE)
            {
                System.err.println("out of memory exception caught. size of array: " + arrIDs.size());
            }

            // wait some time. 
            try
            {
                Thread.sleep(LOOP_SLEEP_INTERVAL);
            }
            catch(InterruptedException iE)
            {
                System.err.println("Interrupted during my beautysleep. Naaah! I'm off my groove!\n" + iE.toString());
                return;
            }
        }
        
        
    }
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) 
    {
        // TODO code application logic here
        BeanstalkManagerMD5 app = new BeanstalkManagerMD5();
    }
}
