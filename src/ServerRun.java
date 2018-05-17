import Entity.ComputeTask;
import Entity.DataNode;
import Entity.GroupNode;
import Entity.UserNode;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.acl.Group;
import java.sql.SQLException;
import java.util.*;

public class ServerRun extends Thread{

    private static int serverPort;
    private static ServerSocket serverSocket;

    private static Map<String,Socket> userMap = new HashMap<>();

    public static Map<String, Socket> getUserMap() {
        return userMap;
    }

    public ServerRun(int port){
        this.serverPort = port;
        try {
            serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        while(true){
            Socket s = null;
            String ip = null;
            try {
                s = serverSocket.accept();
                ip = s.getInetAddress().getHostAddress();
                if(!userMap.keySet().contains(ip)){
                    userMap.put(ip, s);
                    System.out.println("新上线用户的IP地址是："+ip);
//                    PrintWriter pw = new PrintWriter(s.getOutputStream());
//                    pw.println("登录成功，下面发送已上线用户IP");
//                    pw.flush();
//                    for(String curIp:userMap.keySet()){
//                        pw.println(curIp);
//                        pw.flush();
//                    }
//                    pw.println("发送完毕");
//                    pw.flush();
                }

                //每个用户都会处理请求，并发送数据


                ServerManiputify serverManiputify = new ServerManiputify(s);
                serverManiputify.start();
            } catch (IOException e) {
                try {
                    s.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
                userMap.remove(ip);
                System.out.println(ip+"下线了，当前在线人数为："+userMap.size());
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
                JSONObject json = new JSONObject(rcvJsonStr);
                int purpose = json.getInt("purpose");
                JSONObject rtnMsg = null;
                switch (purpose){
                    case 0:
                        rtnMsg = ServerAction.doRegister(json);
                        break;
                    case 1:
                        rtnMsg = ServerAction.doLogin(json);
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
                        rtnMsg = ServerAction.doTask(json,new ArrayList(userMap.values()));
                        break;
                    case 10:
                        rtnMsg = ServerAction.doProcessMsg(json);//还得修改
                    case 11:
                        rtnMsg = ServerAction.doStop(json);
                    default://请求不合法
                        rtnMsg = new JSONObject().put("result",0);
                }
                dataOutputStream.write(rtnMsg.toString().getBytes());
                dataOutputStream.flush();

//                String codeStr = json.getString("purpose");
//                System.out.println("服务器收到的Json Code是:\n" + codeStr);
//                String IPStr = json.getString("SelectedIP");
//                List<String> IPList = Tools.stringToList(IPStr);
//                for (String dstIP : IPList) {
//                    if (userMap.keySet().contains(dstIP)) {
//                        Socket dstSocket = userMap.get(dstIP);
//                        PrintWriter pw = new PrintWriter(dstSocket.getOutputStream());
//                        pw.println("代码任务，来自" + ip + "\n" + ip);
//                        pw.flush();
//                        pw.write(codeStr);
//                        pw.write("\r\n");
//                        pw.flush();
//                        pw.println("代码发送完毕");
//                        pw.flush();
//                    } else {
//                        System.out.println("非法IP");
//                    }
//                }

            }
        } catch (IOException e) {
            userMap.remove(ip);
            System.out.println(ip + "下线了");
            e.printStackTrace();
        } catch (JSONException e) {
            e.printStackTrace();
    }

    }
}