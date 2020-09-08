package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 */
public class GroupMessengerActivity extends Activity {

    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    static List<String> portList = new LinkedList<String>(Arrays.asList("11108", "11112", "11116", "11120", "11124"));
    static Map<String, Integer> frequencyCounter = new HashMap<String, Integer>();
    static Map<String, Socket> alivePortsVal = new HashMap<String, Socket>() {
        {
            put("11108", null);
            put("11112", null);
            put("11116", null);
            put("11120", null);
            put("11124", null);
        }
    };

    PriorityQueue<String> prioritise = new PriorityQueue<String>(5, new Comparator<String>() {
        @Override
        public int compare(String lhs, String rhs) {
            String val1[] = lhs.split("\\.");
            String val2[] = rhs.split("\\.");

            if(val1[0].equals(val2[0]))
            {
                return Integer.valueOf(val2[0]) - Integer.valueOf(val1[0]);
            }

            return Integer.valueOf(val2[1]) - Integer.valueOf(val1[1]);

        }
    });

    static Set<String> deadAvds = new HashSet<String>();
    static int counter = 0;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);


        setContentView(R.layout.activity_group_messenger);

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        final int portNumber = Integer.valueOf(portStr);

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            //Log.e(TAG, "Server socket creation failed");
            return;
        }

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */
        Button sendButton = (Button) findViewById(R.id.button4);
        sendButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                EditText editText = (EditText) findViewById(R.id.editText1);
                String msg = editText.getText().toString();
                editText.setText("");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);

            }
        });
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Socket socket = null;
            BufferedReader bufferedReader = null;
            PrintWriter printWriter = null;
            try {

                while (true) {
                    try {
                        socket = serverSocket.accept();

                            InputStream inputStream = socket.getInputStream();
                            OutputStream outputStream = socket.getOutputStream();
                            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                            try {
                                bufferedReader = new BufferedReader(inputStreamReader);
                                do {
                                    String inputFromClient = bufferedReader.readLine();
                                    if (inputFromClient != null && inputFromClient.contains("~Ping")) {
                                        inputFromClient = inputFromClient.replace("~Ping", "");

                                        String porNum = inputFromClient.split("%")[1];

                                        int messageCounter = 0;
                                        if (!frequencyCounter.containsKey(porNum)) {
                                            frequencyCounter.put(porNum, messageCounter);
                                        }
                                        Log.e("Input from client", inputFromClient);

                                        Log.e("Freqeuncy", "Frequency of " + porNum + " is" + frequencyCounter.get(porNum));
                                        int freqCounter = frequencyCounter.get(porNum)+1;
                                        //printwriter sends an acknowledgement message back to the client which can be used to if the server has recieved the message.
                                        //reference : https://www.oracle.com/webfolder/technetwork/tutorials/obe/java/SocketProgramming/SocketProgram.html#overview
                                        printWriter = new PrintWriter(outputStream, true); //second parameter is to denote autoflush
                                        printWriter.println(freqCounter+"."+porNum);
                                    } else {

                                        //writer.println(msgs[0] + "~" + proposedSequence +'*' + alivePort) ;
                                        String[] values = inputFromClient.split("\\~");
                                        String[] highestAndPort = values[1].split("\\*");
                                        Log.e("Yov", highestAndPort[1]);
                                        String port = highestAndPort[1] == null ? highestAndPort[1] : "0";
                                        int val = Integer.valueOf((int) Math.floor(Double.valueOf(port)));

                                        frequencyCounter.put(highestAndPort[1],val);
                                        publishProgress(values[0]);
                                        socket.close();
                                    }
                                } while (!socket.isClosed());
                            } catch (Exception e) {
                                Log.e("General", e.getMessage());
                            }

                    } catch (IOException e) {
                        //Log.e(TAG, e.getMessage());
                    } catch (Exception e) {
                        //Log.e(TAG, e.getMessage());
                    } finally {

                    }
                }
            } catch (Exception e) {
                //Log.e(TAG, e.getMessage());
            }
            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */
            return null;
        }

        protected void onProgressUpdate(String... strings) {

            try {


                /*
                 * The following code displays what is received in doInBackground().
                 */
                String strReceived = strings[0].trim();
                TextView textView = (TextView) findViewById(R.id.textView1);
                textView.setMovementMethod(new ScrollingMovementMethod());
                textView.append(strReceived + "\n");


                Uri.Builder uriBuilder = new Uri.Builder();
                uriBuilder.authority("edu.buffalo.cse.cse486586.groupmessenger2.provider");
                uriBuilder.scheme("content");
                Uri dataUri = uriBuilder.build();

                ContentValues keyValueToInsert = new ContentValues();
                keyValueToInsert.put("key", counter + "");
                //Log.i("------", strReceived);
                keyValueToInsert.put("value", strReceived);
                counter++;
                getContentResolver().insert(dataUri, keyValueToInsert);

                /*
                 * The following code creates a file in the AVD's internal storage and stores a file.
                 *
                 * For more information on file I/O on Android, please take a look at
                 * http://developer.android.com/training/basics/data-storage/files.html
                 */
            } catch (Exception e) {
                Log.i("seees", e.getMessage());
            }

        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            for (String remotePort : portList) {
                try {
                    Socket socket = null;
                    try {
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(remotePort));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    String msgToSend = msgs[0];

                    PrintWriter printWriter = null;
                    BufferedReader bufferedReader = null;

                    try {
                        OutputStream outputStream = socket.getOutputStream();
                        InputStream inputStream = socket.getInputStream();

                        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                        try {
                            printWriter = new PrintWriter(outputStream, true);

                            Log.i("remote", remotePort);
                            printWriter.println(msgToSend + "%" + remotePort + "~Ping");

                            bufferedReader = new BufferedReader(inputStreamReader);

                            boolean isMessageRecieved = false;

                            while (!isMessageRecieved) {
                                //Waiting for ack from server's print writer
                                if (bufferedReader != null) {

                                    String seqNumber = bufferedReader.readLine();
                                    Log.e("buffer reader~~~~~", seqNumber);
                                    prioritise.add(seqNumber);
                                    alivePortsVal.put(remotePort, socket);
                                    isMessageRecieved = true;
                                }
                            }
                        } catch (UnknownHostException uhEx) {
                            deadAvds.add(remotePort);
                        } catch (SocketTimeoutException socEx) {
                            deadAvds.add(remotePort);
                        } catch (IOException ex) {
                            deadAvds.add(remotePort);
                        } catch (Exception e) {
                            deadAvds.add(remotePort);
                        }
                        /*
                         * TODO: Fill in your client code that sends out a message.
                         */
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            Log.e("Done", String.valueOf(alivePortsVal.size()));
            for (String alivePort : alivePortsVal.keySet())
            {
                Socket aliveSockets = alivePortsVal.get(alivePort);
                if(aliveSockets==null || aliveSockets.isClosed() || deadAvds.contains(alivePort))
                {
                    continue;
                }
                PrintWriter writer = null;
                try {
                    writer = new PrintWriter(aliveSockets.getOutputStream(), true);
                    String proposedSequence = prioritise.poll();
                    Log.e("dem alive", alivePort);
                    writer.println(msgs[0] + "~" + proposedSequence +'*' + alivePort) ;
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    if (aliveSockets != null && !aliveSockets.isClosed()) {
                        try {
                            aliveSockets.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }

            prioritise.clear();
            deadAvds = new HashSet<String>();
            return null;
        }
    }
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }
}