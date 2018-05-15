import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
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
                    PrintWriter pw = new PrintWriter(s.getOutputStream());
                    pw.println("登录成功，下面发送已上线用户IP");
                    pw.flush();
                    for(String curIp:userMap.keySet()){
                        pw.println(curIp);
                        pw.flush();
                    }
                    pw.println("发送完毕");
                    pw.flush();
                }

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

    public ServerManiputify(Socket socket){
        clientSocket = socket;
    }

    @Override
    public void run() {
        String ip = clientSocket.getInetAddress().getHostAddress();
        while (true) {
            try {
                //接收消息
                InputStream is = clientSocket.getInputStream();
                byte[] b = new byte[1024];
                is.read(b);
                String rcvStr = new String(b);
                System.out.println("客户端发来的是：" + rcvStr);

//                    BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
//                    while(true){
//                        String info = null;
//                        if((info=br.readLine())!=null){
//                            rcvStr = rcvStr + info + "\n";
//                        }
//                    }

                //判断请求的目的
                if (rcvStr.contains("发送代码")) {
                    //假如这里是要向选择的机器发送代码，将要解析收到的IP地址，存到listIP中
                    String strIPs = rcvStr.substring(rcvStr.indexOf("选择IP：") + 5, rcvStr.indexOf(" IP选择完毕"));
                    List<String> listIP = Arrays.asList(strIPs.split(","));

                    //提取代码
                    String code = rcvStr.substring(rcvStr.indexOf("CodeBegin... ") + 13, rcvStr.indexOf(" CodeFinish..."));
                    //发送推送消息，即向当前选择的机器发送代码
                    for (String dstIP : listIP) {
                        if (userMap.keySet().contains(dstIP)) {
                            Socket dstSocket = userMap.get(dstIP);
                            PrintWriter pw = new PrintWriter(dstSocket.getOutputStream());
                            pw.println("代码任务，来自" + ip + "\n" + code);
                            pw.flush();
                            pw.println("代码发送完毕");
                            pw.flush();
                        } else {
                            System.out.println("非法IP");
                        }
                    }
                }
            } catch (IOException e) {
                userMap.remove(ip);
                System.out.println(ip + "下线了");
                e.printStackTrace();
            }
        }
    }
}