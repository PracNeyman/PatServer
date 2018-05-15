public class Test {

    private static int port = 8888;
    private static String ip = "172.20.33.68";
    public static void main(String[] args){
        ServerRun serverRun = new ServerRun(port);
        serverRun.start();

        ClientRun clientRun1 = new ClientRun(ip, port);
        clientRun1.start();

//        ClientRun clientRun2 = new ClientRun(ip,port);
//        clientRun2.start();

    }
}
