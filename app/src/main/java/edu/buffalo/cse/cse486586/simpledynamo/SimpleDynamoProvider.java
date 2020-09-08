package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import java.util.TreeSet;

import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

class DataObj implements Serializable {
    String requestType;
    Object dataCapsule;
}

class DaQueue {

    private List queue = new LinkedList();
    private int limit;

    public DaQueue(int limit) {
        this.limit = limit;
    }


    public synchronized void put(Object item)
            throws InterruptedException {
        while (this.queue.size() == this.limit) {
            wait();
        }
        this.queue.add(item);
        if (this.queue.size() == this.limit) {
            notifyAll();
        }
    }


    public synchronized Object take()
            throws InterruptedException {
        while (this.queue.size() == 0) {
            wait();
        }
        if (this.queue.size() == this.limit) {
            notifyAll();
        }

        return this.queue.remove(0);
    }
}

public class SimpleDynamoProvider extends ContentProvider {

    String TAG = SimpleDynamoProvider.class.getSimpleName();
    private final Uri myUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
    static final Integer SERVER_PORT = 10000;
    static String thisAVD = "";
    boolean flagRecovery;
    DaQueue Queue = new DaQueue(1);
    static List<String> portList;
    static TreeSet<String> hashedPortSet = new TreeSet<String>();
    static List<String> orderedPortSet;
    static Map<String, String> portHolder;

    private SQLiteDatabase sqldb;
    createDB database;

    public void initializeNodes() {
        try {

            orderedPortSet = new ArrayList<String>();
            portHolder = new HashMap<String, String>();
            portList = new ArrayList<String>(Arrays.asList("5554", "5556", "5558", "5560", "5562"));
            for (String s : portList) {
                String key = genHash(s);
                hashedPortSet.add(key);
                portHolder.put(key, s);
            }
            orderedPortSet.addAll(hashedPortSet);
        } catch (Exception e) {

        }
    }

    public void deleteFiles(List<String> fileNames) {
        for (String fileName : fileNames) {
            database.delectCurrentValue(fileName);
        }
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        File directory = getContext().getFilesDir();
        List<String> keyToDelete = new ArrayList<String>();
        if (selection.contains("@") || selection.contains("*")) {
            for (File file : directory.listFiles()) {
                keyToDelete.add(file.getName());
            }
        } else {
            try {
                if (selectionArgs == null) {
                    DataObj dataObj = new DataObj();
                    dataObj.requestType = "Delete@" + selection;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, dataObj);
                }
                String key = selection.split("-")[0];
                keyToDelete.add(key);
            } catch (Exception e) {

            }
        }
        deleteFiles(keyToDelete);
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        return null;
    }

    public List<String> getNextNodeAsList(String inputKey) throws NoSuchAlgorithmException {
        String key = genHash(inputKey);
        List<String> dataList = new LinkedList<String>();

        int n = orderedPortSet.size();
        for (int i = 0; i < orderedPortSet.size(); i++) {
            if (orderedPortSet.get(i).compareTo(key) > 0) {
                int current = i % n;
                int next = (i + 1) % n;
                int nextNext = (i + 2) % n;

                dataList.add(portHolder.get(orderedPortSet.get(current)));
                dataList.add(portHolder.get(orderedPortSet.get(next)));
                dataList.add(portHolder.get(orderedPortSet.get(nextNext)));
                return dataList;
            } else if (i == portList.size() - 1) {
                dataList.add(portHolder.get(orderedPortSet.get(0)));
                dataList.add(portHolder.get(orderedPortSet.get(1)));
                dataList.add(portHolder.get(orderedPortSet.get(2)));
                return dataList;
            }
        }
        return dataList;
    }

    public String newValueToInsert(String fileName, String newFileContent) {
        String defaultValue = newFileContent;

        /* database */
        Cursor cursor = database.getData(fileName);
        String exisitngValue = null;
        if (cursor.moveToFirst()) {
            exisitngValue = cursor.getString(cursor.getColumnIndex("value"));
        }
        /* database */

        String newTime = newFileContent.split(":")[1];

        if (exisitngValue == null) {
            defaultValue = newFileContent;
        } else if (exisitngValue.split(":")[1].compareTo(newTime) > 0) {
            defaultValue = exisitngValue;
        }

        return defaultValue;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String fileName = values.getAsString("key");
        String newFileContent = values.getAsString("value");
        try {
            if (newFileContent.contains(":")) {
                newFileContent = newValueToInsert(fileName, newFileContent);
                values.put("value", newFileContent);
                database.insert(values);
            } else if (!newFileContent.contains(":")) {

                Long time = System.currentTimeMillis();
                newFileContent = newFileContent + ":" + time;
                newFileContent = newValueToInsert(fileName, newFileContent);

                List<String> nodes = getNextNodeAsList(fileName);
                for (String port : nodes) {
                    if (port.equals(thisAVD)) {
                        values.put("value", newFileContent);
                        database.insert(values);
                    } else {
                        DataObj dataObj = new DataObj();
                        dataObj.requestType = "Insert@" + fileName + "-" + newFileContent;
                        dataObj.dataCapsule = Integer.parseInt(port) * 2;
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, dataObj);
                    }
                }
            }

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return uri;
    }


    @Override
    public boolean onCreate() {

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String port = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        thisAVD = String.valueOf((Integer.parseInt(port)));
        flagRecovery = false;
        initializeNodes();

        database = new createDB(getContext());
        sqldb = database.getWritableDatabase();

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {


        }
        DataObj dataObj = new DataObj();
        dataObj.requestType = "Rewrite";
        new RecoverTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, dataObj);

        return false;
    }

    public Cursor waitTillRecovery(Uri uri, String selection,
                                   String[] selectionArgs) {
        try {
            while (!flagRecovery) {
                Thread.sleep(100);
            }
        } catch (InterruptedException e) {

            e.printStackTrace();
        }

        return query(uri, null, selection, selectionArgs, null);
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String key,
                        String[] selectionArgs, String sortOrder) {
        MatrixCursor Cursor = new MatrixCursor(new String[]{"key", "value"});
        try {
            if (key.contains("@") || key.contains("*")) {
                Cursor allCursor = database.getAll();

                allCursor.moveToFirst();
                for (allCursor.moveToFirst(); !allCursor.isAfterLast(); allCursor.moveToNext()) {
                    String value = allCursor.getString(allCursor.getColumnIndex("value"));
                    if (selectionArgs == null) {
                        value = value.split(":")[0];
                    }
                    Cursor.addRow(new String[]{allCursor.getString(allCursor.getColumnIndex("key")), value});
                }
                if (key.contains("*")) {
                    DataObj dataObj = new DataObj();
                    dataObj.requestType = "*@";
                    dataObj.dataCapsule = String.valueOf(Integer.parseInt(thisAVD) * 2);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, dataObj);

                    String data = (String) Queue.take();
                    Log.v(TAG, "The data here is :" + data);
                    String[] pairs = data.split("-");
                    for (String pair : pairs) {
                        if (!pair.equals("") && !(pair == null)) {
                            String keyValue = pair.split(":")[0];
                            Cursor.addRow(new String[]{keyValue.split("_")[0], keyValue.split("_")[1]});
                        }
                    }
                }
                return Cursor;
            } else {
                String value = null;
                if (selectionArgs == null) {

                    List<String> validPorts = getNextNodeAsList(key);
                    String data = "";
                    if (validPorts.contains(thisAVD)) {
                        Cursor allCursor = database.getData(key);
                        if (allCursor.moveToFirst()) {
                            value = allCursor.getString(allCursor.getColumnIndex("value"));
                        }
                        data = key + "_" + value;
                    }

                    String msg = "Query@" + key + "-" + thisAVD;

                    DaQueue Queue0 = new DaQueue(1);
                    DataObj dataObj = new DataObj();
                    dataObj.requestType = msg;
                    dataObj.dataCapsule = Queue0;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, dataObj);

                    String response = (String) Queue0.take();

                    if (!data.equals("")) {
                        response = response + data;
                    }

                    Log.v("booga", response);

                    String[] responseData = response.split("-");
                    Arrays.sort(responseData, new Comparator<String>() {
                        @Override
                        public int compare(String o1, String o2) {
                            return o2.split(":")[1].compareTo(o1.split(":")[1]);
                        }
                    });

                    Cursor.addRow(new String[]{responseData[0].split("_")[0], responseData[0].split("_")[1].split(":")[0]});

                    return Cursor;

                } else if (selectionArgs != null) {
                    String content = null;
                    Cursor allCursor = database.getData(key);
                    if (allCursor.moveToFirst()) {
                        content = allCursor.getString(allCursor.getColumnIndex("value"));
                    }
                    String record[] = {key, content};
                    Cursor.addRow(record);
                    return Cursor;
                }
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            BufferedReader bufferedReader = null;
            PrintWriter printWriter = null;

            while (true) {
                try {

                    Socket socket = serverSocket.accept();
                    socket.setSoTimeout(100);
                    InputStream inputStream = socket.getInputStream();
                    OutputStream outputStream = socket.getOutputStream();
                    InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                    String inputFromClient = "";
                    try {
                        bufferedReader = new BufferedReader(inputStreamReader);
                        inputFromClient = bufferedReader.readLine();
                        String[] message = inputFromClient.split("@");
                        if (message[0].equals("Insert")) {

                            ContentValues data = new ContentValues();
                            data.put("key", message[1].split("-")[0]);
                            data.put("value", message[1].split("-")[1]);
                            insert(myUri, data);

                            printWriter = new PrintWriter(outputStream, true);
                            printWriter.println("InsertSuccessful");

                        } else if (message[0].equals("Delete")) {
                            String key = message[1];
                            delete(myUri, key, new String[]{"deleteitall"});

                            printWriter = new PrintWriter(outputStream, true);
                            printWriter.println("Acknowledgement");

                        } else if (message[0].equals("Rewrite")) {

                            String ack = "Acknowledge#";
                            Cursor cursor = query(myUri, null, "@", new String[]{"MissedIt"}, null);

                            cursor.moveToFirst();
                            for (cursor.moveToFirst(); !cursor.isAfterLast(); cursor.moveToNext()) {
                                ack += cursor.getString(0) + "_" + cursor.getString(1) + "-";
                            }

                            printWriter = new PrintWriter(outputStream, true);
                            printWriter.println(ack);

                        } else if (message[0].equals("*")) {

                            Cursor cursor = waitTillRecovery(myUri, "@", null);
                            String ack = "Acknowledge#";

                            cursor.moveToFirst();
                            for (cursor.moveToFirst(); !cursor.isAfterLast(); cursor.moveToNext()) {
                                ack += cursor.getString(0) + "_" + cursor.getString(1) + "-";
                            }

                            ack += "#" + thisAVD;

                            printWriter = new PrintWriter(outputStream, true);
                            printWriter.println(ack);
                        } else if (message[0].equals("Query")) {

                            try {

                                String[] data = message[1].split("-");
                                Cursor cursor = waitTillRecovery(myUri, data[0], new String[]{"rand"});
                                if (cursor.moveToFirst()) {
                                    printWriter = new PrintWriter(outputStream, true);
                                    printWriter.println("Acknowledge#" + cursor.getString(0) + "_" + cursor.getString(1));
                                }
                            } catch (Exception e) {

                            }
                        }

                    } catch (IOException e) {
                        e.printStackTrace();
                    } finally {
                        outputStream.close();
                        inputStream.close();
                        socket.close();
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

    }

    private class ClientTask extends AsyncTask<Object, Void, Void> {

        @Override
        protected Void doInBackground(Object... msgs) {

            try {

                DataObj request = (DataObj) msgs[0];
                String msgToSend = request.requestType;

                String[] message = msgToSend.split("@");

                if (message[0].equals("*")) {
                    String result = "";
                    Log.v(TAG, "done");
                    for (String port : portList) {
                        try {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port) * 2);
                            OutputStream outputStream = socket.getOutputStream();
                            PrintWriter printWriter = new PrintWriter(outputStream, true);
                            printWriter.println(msgToSend);

                            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            String acknowledgement = in.readLine();
                            if (acknowledgement != null) {
                                if (acknowledgement.startsWith("Acknowledge")) {
                                    result += acknowledgement.split("#")[1];
                                    in.close();
                                    socket.close();
                                }
                            } else {
                                if (acknowledgement == null)
                                    throw new NullPointerException();
                            }
                        } catch (Exception e) {

                        }
                    }
                    Queue.put(result);
                } else if (message[0].equals("Insert")) {
                    try {
                        int port = (Integer) request.dataCapsule;
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);

                        OutputStream outputStream = socket.getOutputStream();
                        PrintWriter printWriter = new PrintWriter(outputStream, true);
                        printWriter.println(msgToSend);

                        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        String ack = in.readLine();
                        if (ack != null) {
                            if (ack.equals("InsertSuccessful")) {
                                in.close();
                                socket.close();
                            }
                        } else {
                            if (ack == null) {
                                throw new NullPointerException();
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else if (message[0].equals("Delete")) {

                    try {
                        String[] key = message[1].split("-");

                        List<String> ports = getNextNodeAsList(key[0]);
                        ports.remove(thisAVD);
                        for (String port : ports) {
                            try {
                                if (port != "") {
                                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port) * 2);

                                    OutputStream outputStream = socket.getOutputStream();
                                    PrintWriter printWriter = new PrintWriter(outputStream, true);
                                    printWriter.println(msgToSend);

                                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                                    String ack = in.readLine();
                                    if (ack != null) {
                                        if (ack.equals("Acknowledgement")) {
                                            in.close();
                                            socket.close();
                                        }
                                    } else {
                                        if (ack == null) {
                                            throw new NullPointerException();
                                        }
                                    }
                                }
                            } catch (Exception e) {

                            }
                        }

                    } catch (Exception e) {

                    }
                } else if (message[0].equals("Query")) {

                    int write = 0;
                    String result = "";
                    DaQueue dataQueue = (DaQueue) request.dataCapsule;
                    List<String> node = getNextNodeAsList(message[1].split("-")[0]);
                    if (node.remove(thisAVD))
                        write++;

                    for (String each : node) {
                        try {
                            Log.v("Ack", "Sending acknowledgement");
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(each) * 2);
                            OutputStream outputStream = socket.getOutputStream();
                            PrintWriter printWriter = new PrintWriter(outputStream, true);
                            printWriter.println(msgToSend);

                            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            String acknowledgement = in.readLine();

                            if (acknowledgement != null) {
                                if (acknowledgement.startsWith("Acknowledge")) {
                                    result += acknowledgement.split("#")[1] + "-";
                                    write++;
                                    if (write == 2) {
                                        dataQueue.put(result);
                                    }
                                    in.close();
                                    socket.close();

                                }
                            } else {
                                if (acknowledgement == null) {
                                    throw new NullPointerException();
                                }
                            }
                        } catch (UnknownHostException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (Exception e) {

                        }
                    }
                }

            } catch (Exception e) {

            }

            return null;
        }
    }

    private class RecoverTask extends AsyncTask<Object, Void, Void> {

        @Override
        protected Void doInBackground(Object... msgs) {

            try {
                String data = "";
                String msg = "";
                for (String port : portList) {
                    try {
                        if (!port.equals(thisAVD)) {

                            Socket sockets = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port) * 2);

                            OutputStream outputStream = sockets.getOutputStream();
                            PrintWriter printWriter = new PrintWriter(outputStream, true);

                            msg = "Rewrite";
                            printWriter.println(msg);

                            BufferedReader in = new BufferedReader(new InputStreamReader(sockets.getInputStream()));
                            String acknowledgement = in.readLine();

                            if (acknowledgement.startsWith("Acknowledge")) {
                                in.close();
                                sockets.close();
                                String[] temp = acknowledgement.split("#");
                                if (temp.length > 1) {
                                    Log.v(TAG, "adding data");
                                    data += acknowledgement.split("#")[1];
                                }

                            }

                        }
                    } catch (Exception e) {

                    }
                }

                if (data.length() > 0) {
                    Log.v(TAG, "The data is :" + data);
                    String[] keyValues = data.split("-");
                    HashMap<String, String> hashmap = new HashMap<String, String>();
                    Log.v(TAG, "initialising map");
                    for (String pair : keyValues) {
                        List<String> nextPorts = getNextNodeAsList(pair.split("_")[0]);

                        if (nextPorts.contains(thisAVD)) {
                            if (!hashmap.containsKey(pair.split("_")[0])) {
                                hashmap.put(pair.split("_")[0], pair.split("_")[1]);
                            } else {
                                if (pair.split("_")[1].split(":")[1].compareTo(hashmap.get(pair.split("_")[0]).split(":")[1]) > 0) {
                                    hashmap.put(pair.split("_")[0], pair.split("_")[1]);
                                }
                            }
                        }
                    }

                    Iterator itr = hashmap.keySet().iterator();
                    while (itr.hasNext()) {
                        ContentValues values = new ContentValues();
                        String key = (String) itr.next();
                        values.put("key", key);
                        values.put("value", hashmap.get(key));
                        insert(myUri, values);
                    }
                }

                flagRecovery = true;

            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

            return null;
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        return 0;
    }

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