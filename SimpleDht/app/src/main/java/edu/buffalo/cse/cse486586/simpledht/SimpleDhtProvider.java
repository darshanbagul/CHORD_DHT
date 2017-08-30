package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleDhtProvider extends ContentProvider {
    private static final String TAG = SimpleDhtProvider.class.getSimpleName();

    // Instantiate all the variables below

    // All network ports
    static final int SERVER_PORT = 10000;

    static final int REMOTE_PORT0 = 11108;
    static final int REMOTE_PORT1 = 11112;
    static final int REMOTE_PORT2 = 11116;
    static final int REMOTE_PORT3 = 11120;
    static final int REMOTE_PORT4 = 11124;

    public Uri mUri;
    // We are using ConcurrentHashMap to store the <key,value> pairs in our implementation of a DHT.
    // This is because it supports full concurrency of retrievals and adjustable expected
    // concurrency for updates. Implementation details can be obtained in javadocs:
    // https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/ConcurrentHashMap.html
    private static ConcurrentHashMap<String, String> hashTable = new ConcurrentHashMap<String, String>();
    private static final ConcurrentHashMap<String, String> INIT_EMPTY_TABLE = null;

    // Initialise an empty message that will act as a buffer while we implement blocking of nodes.
    // We can retrieve the necessary data from this variable after blocking
    private Message blockMsg = new Message(INIT_EMPTY_TABLE,null, null, 0, 0, MessageType.NULL);

    // Each emulator instance has information about neighboring nodes in DHT
    private int currPort;
    private int prevPort;
    private int nextPort;

    // '*Id' variables shall hold the port number hashed generated keys
    private String nodeId;
    private String prevNodeId;
    private String nextNodeId;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        delete_handler(selection, currPort);
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        insert_handler(values.getAsString("key"), values.getAsString("value"));
        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
        TelephonyManager tel = (TelephonyManager)this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        currPort = Integer.parseInt(portStr) * 2;

        try {
            nodeId = genHash(portStr);
        } catch (NoSuchAlgorithmException e) {
            Log.e("HASHING_PORT", "NoSuchAlgorithmException: " + e.getMessage());
            return false;
        }

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
//            ServerTask(serverSocket);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e("SERVER_SOCKET_IO", "IOException: " + e.getMessage());
            return false;
        }

        //Initialise the neighbors (prev, next) of this emulator to itself, to begin with
        // These values be modified when this emulator requests to join the ring
        prevNodeId = nodeId;
        prevPort = currPort;

        nextNodeId = nodeId;
        nextPort = currPort;

        // This is where we ensure that AVD emulator-5554 is always the first one to be initiated
        if (currPort != REMOTE_PORT0) {
            new ClientTask(INIT_EMPTY_TABLE, REMOTE_PORT0, nodeId, null, currPort, currPort,
                    MessageType.JOIN).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
        }
        return true;
    }

    /*
     * This is the function which handles the overriden method for query, and returns the Cursor
     * object. We have two sub functions that handle the querying, where if the the query is '*' or
     * '@', then we need to return all the data objects in the DHT, else another function for
     * querying objects in the hashtable.
     */
    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        if (selection.equals("*") || selection.equals("@")) {
            return fetchAllData(new ConcurrentHashMap<String, String>(), currPort, selection, true);
        }
        else {
            return query_handler(selection, currPort, true);
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    /* Server and Client Tasks can be written using an AsyncTask as we have been doing for earlier
     * assignments, but I faced a lot of issues trying to debug an AsyncTask implementation for
     * this assignment, especially handling the concurrencies. Hence I decided to try out the
     * basics of implementing a simple Java Thread based approach for client and server tasks.
     * The benefit of this method is in the use of synchronized blocks, which help us cope with
     * concurrency via locking mechanism.
     * https://docs.oracle.com/javase/tutorial/essential/concurrency/
     *
     * EDIT - However, I figured out achieving concurrency using Asynctasks in the end, and have
     * implemented it. Good exercise for understanding multithreading and concurrency concepts!
     *
     * We shall use synchronized blocks for tasks where we have to query the DHT, because the same
     * object can be being accessed by other nodes for reading/writing. Hence we need a lock.
     */

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try {
                while(true) {
                    Socket socket = serverSocket.accept();
                    ObjectInputStream in_stream = new ObjectInputStream(socket.getInputStream());
                    Message message = (Message) in_stream.readObject();
                    /*
                     * Based on the type of message, we will call individual function which
                     * handles each operation
                     */
                    if(message.operation == MessageType.JOIN) {
                        // Handle join request message
                        join_handler(message);
                    }
                    else if(message.operation == MessageType.ACCEPT){
                        // Handle accept requests, where we set the previous node to previous
                        // node in message and next node to one in message received
                        prevNodeId = message.key;
                        prevPort = message.prevNode;
                        nextNodeId = message.value;
                        nextPort = message.nextNode;
                    }
                    else if(message.operation == MessageType.INSERT){
                        // Handle inserting new message in the DHT
                        insert_handler(message.key, message.value);
                    }
                    else if(message.operation == MessageType.QUERY){
                        // Handling querying the DHT. The important part here is that we have
                        // a blocking mechanism if the value previous node in received
                        // message is equal to the value of the emulator port. The blockMsg
                        // message stores the data that we need after we finish the blocking
                        if (currPort == message.prevNode) {
                            blockMsg.value = message.value;
                            synchronized(blockMsg) {
                                blockMsg.notify();
                            }
                        } else {
                            query_handler(message.key, message.prevNode, false);
                        }
                    }
                    else if(message.operation == MessageType.FETCH_ALL){
                        // Handles fetch all queries over the distributed hash table created.
                        // As in case of querying above, we have to implement the same blocking
                        // mechanism if value of the previous node in the message is equal to
                        // the emulator port value.
                        if (currPort == message.prevNode) {
                            blockMsg.map = message.map;
                            synchronized(blockMsg) {
                                blockMsg.notify();
                            }
                        } else {
                            fetchAllData(message.map, message.prevNode, message.key,false);
                        }
                    }
                    else if(message.operation == MessageType.DELETE){
                        // Handles the deletion of data from our distributed hashtable
                        delete_handler(message.key, message.prevNode);
                    }
                    else if(message.operation == MessageType.SET_NEXT_NODE){
                        // Set the next node in the chord to the values in message
                        nextNodeId = message.key;
                        nextPort = message.nextNode;
                    }
                    else{
                        Log.e(TAG, "Message type not known: " + message.operation);
                    }
                }
            } catch (ClassNotFoundException e) {
                Log.e("SERVER_INPUTSTREAM", "ClassNotFoundException: " + e.getMessage());
            } catch (UnknownHostException e) {
                Log.e("SERVER_UNKNOWN_HOST", "UnknownHostException: " + e.getMessage());
            } catch (IOException e) {
                Log.e("SERVER_IOEXCEPTION", "IOException: " + e.getMessage());
            }
            return null;
        }
    }

    private class ClientTask extends AsyncTask<Void, Void, Void> {
        ConcurrentHashMap<String, String> cursor;
        int remotePort;
        String key;
        String value;
        int prevNode;
        int nextNode;
        MessageType operation;
        ClientTask(ConcurrentHashMap<String, String> cursor, int remotePort, String key,
                   String value, int prevNode, int nextNode, MessageType operation){
            this.cursor = cursor;
            this.remotePort = remotePort;
            this.key = key;
            this.value = value;
            this.prevNode = prevNode;
            this.nextNode = nextNode;
            this.operation = operation;
        }

        @Override
        protected Void doInBackground(Void... params) {
            try {
                Socket socket = new Socket(
                        InetAddress.getByAddress(new byte[] {10, 0, 2, 2}), remotePort);
                ObjectOutputStream out_stream = new ObjectOutputStream(
                        socket.getOutputStream());
                Message message = new Message(cursor, key, value, prevNode, nextNode, operation);
                out_stream.writeObject(message);
                out_stream.close();
                socket.close();
            } catch (UnknownHostException e) {
                Log.e("CLIENT_UNKNOWN_HOST", "UnknownHostException: " + e.getMessage());
            } catch (IOException e) {
                Log.e("CLIENT_IO", "IOException: " + e.getMessage());
            }
            return null;
        }
    }

    /*
     * Below we implement helper functions for handling operations for each type of message -
     * join, insert, delete and querying DHT.
     */

    private void join_handler(Message message) {
        if (checkKeyPresent(message.key)) {
            if (currPort == prevPort) {
                new ClientTask(INIT_EMPTY_TABLE, message.prevNode, prevNodeId, nextNodeId, prevPort, nextPort, MessageType.ACCEPT).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
                prevNodeId = message.key;
                nextNodeId = message.key;
                prevPort = message.prevNode;
                nextPort = message.nextNode;
            } else {
                new ClientTask(INIT_EMPTY_TABLE, prevPort, message.key, null, 0, message.nextNode, MessageType.SET_NEXT_NODE).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
                new ClientTask(INIT_EMPTY_TABLE, message.prevNode, prevNodeId, nodeId, prevPort, currPort, MessageType.ACCEPT).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
                prevNodeId = message.key;
                prevPort = message.prevNode;
            }
        } else {
            new ClientTask(message.map, nextPort, message.key, message.value, message.prevNode, message.nextNode, message.operation).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
        }
    }

    private void insert_handler(String key, String value) {
        // Check if the generated hashed key is ideally suited to be handled by this node or else pass
        // it to next node for checking. Continue until we find appropriate node to store the key.
        try {
            String hashed_key = genHash(key);
            if (checkKeyPresent(hashed_key)) {
                hashTable.put(key, value);
            } else {
                // Send key to next port for storing
                new ClientTask(INIT_EMPTY_TABLE, nextPort, key, value, 0, 0,
                        MessageType.INSERT).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
            }
        } catch (NoSuchAlgorithmException e) {
            Log.e("INSERT_HASHING", "NoSuchAlgorithmException: " + e.getMessage());
        }
    }

    private void delete_handler(String key, int startNode) {
        // If Delete all query, clear key, value storage - implementation of delete("*") query,
        // even though it is not being tested in grader.
        if (key.equals("@") || key.equals("*")) {
            hashTable.clear();
            if (key.equals("*") && nextPort != startNode) {
                new ClientTask(INIT_EMPTY_TABLE, nextPort, key, null, startNode, 0,
                        MessageType.DELETE).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
            }
        }
        else {
            try
            {
                String hashedKey = genHash(key);
                if (checkKeyPresent(hashedKey)) {
                    hashTable.remove(key);
                }
                else {
                    new ClientTask(INIT_EMPTY_TABLE, nextPort, key, null, startNode, 0,
                            MessageType.DELETE).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
                }
            } catch (NoSuchAlgorithmException e) {
                Log.e("DELETE_HASHING", "NoSuchAlgorithmException: " + e.getMessage());
            }
        }
    }

    private Cursor query_handler(String key, int startNode, boolean wait) {
        // MatrixCursor is useful if you have a collection of data that is not in the database,
        // and you want to create a cursor for it.  You simply construct it with an array of
        // column names, and optionally an initial capacity. Then, you can add one row at a
        // time to it by passing either an array of objects or an Iterable to its addRow() method.
        // Refer: https://developer.android.com/reference/android/database/MatrixCursor.html
        MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
        String value = "";
        try {
            String hashedKey = genHash(key);
            if (checkKeyPresent(hashedKey)) {
                if (currPort == startNode) {
                    value = hashTable.get(key);
                    cursor.addRow(new String[]{key, value});
                } else {
                    new ClientTask(INIT_EMPTY_TABLE, startNode, null, hashTable.get(key), startNode, 0,
                            MessageType.QUERY).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);

                }
                return cursor;
            }
            else {
                new ClientTask(INIT_EMPTY_TABLE, nextPort, key, null, startNode, 0, MessageType.QUERY).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
                if (wait) {
                    /*
                     * Since different nodes(threads) can be accessing the data stored in this object, we need to perform
                     * locking so that there cannot be simultaneous modifications to the object. This is done by a
                     * synchronized block or a method in Java.
                     *
                     * A synchronized block in Java is synchronized on some object.
                     * All synchronized blocks synchronized on the same object can only have one thread executing
                     * inside them at the same time. All other threads attempting to enter the synchronized block are
                     * blocked until the thread inside the synchronized block exits the block.
                     * Refer here for details: https://docs.oracle.com/javase/tutorial/essential/concurrency/locksync.html
                    */
                    synchronized(blockMsg) {
                        try {
                            blockMsg.wait();
                        } catch (InterruptedException e) {
                            Log.e("QUERY_INTERRUPT", "InterruptedException: "+ e.getMessage());
                        }
                    }
                    value = blockMsg.value;
                    cursor.addRow(new String[]{key, value});
                    blockMsg.value = null;
                }
            }
        } catch (NoSuchAlgorithmException e) {
            Log.e("QUERY_HASH", "NoSuchAlgorithmException: " + e.getMessage());
        }
        return cursor;
    }

    private Cursor fetchAllData(ConcurrentHashMap<String, String> map, int startNode, String key,
                                boolean wait) {
        MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
        if (currPort == nextPort || key.equals("@")) {
            for(Entry<String, String> entry : hashTable.entrySet()) {
                cursor.addRow(new String[]{entry.getKey(), entry.getValue()});
            }
            return cursor;
        }
        else {
            map.putAll(hashTable);
            new ClientTask(map, nextPort, key, null, startNode, 0,
                    MessageType.FETCH_ALL).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
            if (wait) {
                synchronized(blockMsg) {
                    try {
                        blockMsg.wait();
                    } catch (InterruptedException e) {
                        Log.e("FETCH_ALL", "InterruptedException: " + e.getMessage());
                    }
                }
                for(Entry<String, String> entry : blockMsg.map.entrySet()) {
                    cursor.addRow(new String[]{entry.getKey(), entry.getValue()});
                }
                // Clear the blocking data
                blockMsg.map = INIT_EMPTY_TABLE;
            }
        }
        return cursor;
    }

    /*
     * Partitioning Function implementation
     */

    private boolean checkKeyPresent(String key) {
        BigInteger prevNode = new BigInteger(prevNodeId, 16);
        BigInteger node = new BigInteger(nodeId, 16);
        // If previous node and this node are same, then true
        if (node.compareTo(prevNode) == 0){
            return true;
        }

        BigInteger id = new BigInteger(key, 16);
        // If prevNode > current node
        if (prevNode.compareTo(node) == 1) {
            // If prevNode > current node => key > prevNode and current node then True
            if (id.compareTo(prevNode) == 1 && id.compareTo(node) == 1){
                return true;
            }
            // If prevNode > current node => key < prevNode and key <= current node then True
            if (id.compareTo(prevNode) == -1 && id.compareTo(node) <= 0){
                return true;
            }
        }
        // If prevNode < key <= current node
        if (id.compareTo(prevNode) == 1 && id.compareTo(node) < 1){
            return true;
        }
        return false;
    }

    /*
     * Helper functions for hashing a key and building an URI for content provider.
     */
    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
}