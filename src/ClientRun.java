import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

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
            PrintWriter pw = new PrintWriter(socket.getOutputStream());

            //启动接收数据线程
            ClientRcvThread clientRcvThread = new ClientRcvThread(socket);
            clientRcvThread.start();

            //发送消息
            while(true){
                String info = null;
                if((info=br.readLine())!=null){
                    pw.println(info);
                    pw.flush();
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
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

