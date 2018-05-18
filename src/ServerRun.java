import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.SQLException;
import java.util.*;

public class ServerRun extends Thread{

    private static int serverPort;
    private static ServerSocket serverSocket;

    private static Map<String,Socket> userMap = new HashMap<>();

    public static Map<String, Socket> getUserMap() {
        return userMap;
    }

    public ServerRun(int port) throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        this.serverPort = port;
        try {
            serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
        }
        SQLHandler.init();
    }

    @Override
    public void run() {
        while(true){
            Socket s = null;
            String ip = null;
            try {
                s = serverSocket.accept();
                ip = s.getInetAddress().getHostAddress();

                //每个用户都会处理请求，并发送数据
                ServerManiputify serverManiputify = new ServerManiputify(s);
                serverManiputify.start();
            } catch (IOException e) {
                try {
                    s.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
                e.printStackTrace();
            }
        }
    }
}
class ServerManiputify extends Thread{

    private Map<String, Socket> userMap = ServerRun.getUserMap();
    private Socket clientSocket;
    private DataOutputStream dataOutputStream;
    private String ip;

    public ServerManiputify(Socket socket){
        clientSocket = socket;
    }

    //服务器
    @Override
    public void run() {
        try {
            ip = clientSocket.getInetAddress().getHostAddress();
            dataOutputStream = new DataOutputStream(clientSocket.getOutputStream());
            DataInputStream inputStream = new DataInputStream(clientSocket.getInputStream());
            ByteArrayOutputStream baos = null;
            byte[] by = new byte[2048];
            int n;
            while ((n = inputStream.read(by)) != -1) {
                baos = new ByteArrayOutputStream();
                baos.write(by, 0, n);
                String rcvJsonStr = new String(baos.toByteArray());
                try {
                    JSONObject json = new JSONObject(rcvJsonStr);
                    int purpose = json.getInt("purpose");
                    JSONObject rtnMsg = null;
                    switch (purpose) {
                        case 0:
                            rtnMsg = ServerAction.doRegister(json);
                            break;
                        case 1:
                            rtnMsg = ServerAction.doLogin(json);
                            if (rtnMsg.get("result").equals(1)) {
                                userMap.put(rtnMsg.getString("user_id"), clientSocket);
                                System.out.println(rtnMsg.getString("user_id"));
                            }
                            break;
                        case 2:
                            rtnMsg = ServerAction.doQueryUser(json);
                            break;
                        case 3:
                            rtnMsg = ServerAction.doCreateGroup(json);
                            break;
                        case 4:
                            rtnMsg = ServerAction.doJoinGroup(json);
                            break;
                        case 5:
                            rtnMsg = ServerAction.doQueryGroupByUserId(json);
                            break;
                        case 6:
                            rtnMsg = ServerAction.doQueryDataNodesByUserId(json);
                            break;
                        case 7:
                            rtnMsg = ServerAction.doInsertGroupDataRegisterRelation(json);
                            break;
                        case 8:
                            rtnMsg = ServerAction.doQueryDataNodesByGroupId(json);
                            break;
                        case 9:
                            rtnMsg = ServerAction.doTask(json);
                            break;
                        case 10:
                            rtnMsg = ServerAction.doPowerSlaver(json,userMap);
                            break;
                        case 11:
                            rtnMsg = ServerAction.doProcessMsg(json,userMap);
                            break;
                        case 12:
                            rtnMsg = ServerAction.doStop(json,userMap);
                            break;
                        case 13:
                            rtnMsg = ServerAction.doQueryDataNameByUserIdAndGroupId(json);
                            break;
                        default://请求不合法
                            rtnMsg = new JSONObject().put("result", 0);
                    }
                    if(rtnMsg.keys().hasNext()) {
                        dataOutputStream.write(rtnMsg.toString().getBytes());
                        dataOutputStream.flush();
                    }
                }catch (JSONException e){
                    HashMap<String,Object> hm = new HashMap<>();
                    hm.put("result",0);
                    JSONObject rtnJson = new JSONObject(hm);
                    byte[] errotBytes = rtnJson.toString().getBytes();
                    dataOutputStream.write(errotBytes);
                    dataOutputStream.flush();
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}