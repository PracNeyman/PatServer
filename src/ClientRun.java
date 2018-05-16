import org.json.JSONObject;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class ClientRun extends Thread {
    private int serverPort;
    private String serverIP;
    private Socket socket;

    public ClientRun(String IP, int port){
        this.serverIP = IP;
        this.serverPort = port;
    }

    @Override
    public void run(){
        try {
            socket = new Socket(serverIP,serverPort);
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

//            PrintWriter pw = new PrintWriter(socket.getOutputStream());

            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());



            //启动接收数据线程
            ClientRcvThread clientRcvThread = new ClientRcvThread(socket);
            clientRcvThread.start();

            //发送消息
            while(true){
                String info = null;
                if((info=br.readLine())!=null){
                    if(info.equals("over"))
                        break;
//                    pw.println(info);
//                    pw.flush();
                    dataOutputStream.write(getJsonByte(Integer.parseInt(info)));
                    dataOutputStream.flush();
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String[] codes = {"int main\nprintf(\"hello world\");\n","return 0;\n"};
    private String[] IPs = {"172.20.47.112","192.168.86.128"};//默认IP都选择
    private byte[] getJsonByte(int index){
        HashMap<String,String> hashMap = new HashMap<>();
        hashMap.put("Code",codes[index%2]);
        hashMap.put("SelectedIP",Tools.listToString(new ArrayList<>(Arrays.asList(IPs))));
        JSONObject jsonObject = new JSONObject(hashMap);
        String jsonString = jsonObject.toString();
        System.out.println("Client发送的信息是"+jsonString);
        byte[] jsonoByte = jsonString.getBytes();
        return jsonoByte;
    }
}

class ClientRcvThread extends Thread{
    private Socket s;
    public ClientRcvThread(Socket socket){
        s = socket;
    }
    @Override
    public void run(){
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
            while(true){
                String info = null;
                if((info=br.readLine())!=null){
                    System.out.println(info);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

