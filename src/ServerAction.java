import Entity.ComputeTask;
import Entity.DataNode;
import Entity.GroupNode;
import Entity.UserNode;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ServerAction {

    //处理注册事件
    public static JSONObject doRegister(JSONObject json) throws IOException {
        //注意这里是Object，不是String
        HashMap<String,Object> hashMap = new HashMap<>();
        try {
            String userName = json.getString("user_name");
            String password = json.getString("password");
            String email = json.getString("email");
            String userId = Tools.getRandomID("user");
            SQLHandler.insertUser(new UserNode(userName,password,email,userId));
            //成功插入
            hashMap.put("result",1);
            hashMap.put("user_id",userId);
        } catch (JSONException | SQLException e) {
            hashMap.put("result",0);
            e.printStackTrace();
        }
        return new JSONObject(hashMap);
    }

    //处理登录事件
    public static JSONObject doLogin(JSONObject json){
        HashMap<String,Object> hashMap = new HashMap<>();
        try {
            String email = json.getString("email");
            String password = json.getString("password");
            String result = SQLHandler.isUserExistedByUserNode(new UserNode(password,email));
            if(result.equals("NOTFIND")){
                hashMap.put("result",0);
            }
            else{
                hashMap.put("result",1);
                hashMap.put("user_id",result);
                hashMap.put("user_name",SQLHandler.queryUserByID(result).getUser_name());
            }
        } catch (JSONException | SQLException e) {
            e.printStackTrace();
            hashMap.put("result",0);
        }
        return new JSONObject(hashMap);
    }

    //处理通过ID查询用户的请求
    public static JSONObject doQueryUser(JSONObject json){
        HashMap<String, Object> hashMap = new HashMap<>();
        try{
            String user_id = json.getString("user_id");
            UserNode userNode = SQLHandler.queryUserByID(user_id);
            if(userNode==null){
                hashMap.put("result",0);
            }
            else{
                hashMap.put("result", 1);
                hashMap.put("user_id",user_id);
                hashMap.put("user_name",userNode.getUser_name());
                hashMap.put("email",userNode.getEmail());
            }
        }catch (JSONException e){
            e.printStackTrace();
            hashMap.put("result",0);
        }
        return new JSONObject(hashMap);
    }

    //处理创建群组的请求
    public static JSONObject doCreateGroup(JSONObject json){
        HashMap<String,Object> hashMap = new HashMap<>();
        try{
            String creatorId = json.getString("creator_id");
            String groupName = json.getString("group_name");
            String dataType = json.getString("data_type");
            String description = json.getString("description");
            //是否应该有member_num???????????????????????????????????????/
            int memberNum = json.getInt("member_nums");

            String groupId = Tools.getRandomID("group");
            UserNode userNode = SQLHandler.queryUserByID(creatorId);
            if (userNode==null){
                hashMap.put("result",0);
            }
            else {
                GroupNode groupNode = new GroupNode(groupId, groupName, dataType);
                groupNode.setOwner(userNode);
                groupNode.setOwner_id(creatorId);
                groupNode.setDescription(description);
                groupNode.setMember_num(memberNum);
                SQLHandler.insertGroup(groupNode);
                hashMap.put("result",1);
                hashMap.put("group_id",groupId);
            }
        }catch (JSONException e){
            e.printStackTrace();
            hashMap.put("result",0);
        }
        return new JSONObject(hashMap);
    }

    //处理用户加入群组的请求
    public static JSONObject doJoinGroup(JSONObject json){
        HashMap<String,Object> hashMap = new HashMap<>();
        try{
            String userId = json.getString("user_id");
            String groupId = json.getString("group_id");
            boolean success = SQLHandler.insertGroupUserRelation(userId,groupId);
            if(success){
                hashMap.put("result",1);
            }else{
                hashMap.put("result",0);
            }
        }catch (JSONException e){
            hashMap.put("result",0);
            e.printStackTrace();
        }
        return new JSONObject(hashMap);
    }

    //处理查询用户分组的请求
    public static JSONObject doQueryGroupByUserId(JSONObject json) {
        HashMap<String, Object> hashMap = new HashMap<>();
        JSONObject jsonObject = new JSONObject();
        try{
            List<GroupNode> groupNodes = SQLHandler.queryGroupsByUserID(json.getString("user_id"));
            if(groupNodes==null||groupNodes.size()==0){
                jsonObject.put("result",0);
//                hashMap.put("result",0);
            }else{
                JSONArray jsonArray = new JSONArray();
                for(GroupNode groupNode:groupNodes){
                    JSONObject group = new JSONObject();
                    group.put("group_id",groupNode.getGroup_id());
                    group.put("group_name",groupNode.getGroup_name());
                    group.put("creator_id",groupNode.getOwner_id());
                    group.put("create_date",groupNode.getCreat_date());
                    group.put("description",groupNode.getDescription());
                    group.put("member_num",groupNode.getMember_num());
                    group.put("data_type",groupNode.getType());
                    jsonArray.put(group);
                }
                jsonObject.put("groups",jsonArray);
                jsonObject.put("result",1);

            }
        }catch (JSONException e){
            //JSON异常处理中，不可用json
            hashMap.put("result",0);
            e.printStackTrace();
        }
        if(hashMap.containsKey("result")){
            return new JSONObject(hashMap);
        }else
            return jsonObject;
    }

    //处理查询用户数据集的请求
    public static JSONObject doQueryDataNodesByUserId(JSONObject json) {
        HashMap<String, Object> hashMap = new HashMap<>();
        JSONObject jsonObject = new JSONObject();
        try{
            List<DataNode>  dataNodes = SQLHandler.queryDataNodesByID(json.getString("user_id"));
            if(dataNodes==null||dataNodes.size()==0){
                jsonObject.put("result",0);
//                hashMap.put("result",0);
            }else{
                JSONArray jsonArray = new JSONArray();
                for(DataNode dataNode:dataNodes){
                    JSONObject data = new JSONObject();
                    data.put("user_id",dataNode.getUser_id());
                    data.put("data_name",dataNode.getData_name());
                    data.put("data_type",dataNode.getData_type());
                    data.put("row_nums",dataNode.getRow_nums());
                    data.put("attr_nums",dataNode.getAttr_nums());
                    data.put("file_path",dataNode.getFile_path());
                    jsonArray.put(data);
                }
                jsonObject.put("datas",jsonArray);
                jsonObject.put("result",1);

            }
        }catch (JSONException e){
            //JSON异常处理中，不可用json
            hashMap.put("result",0);
            e.printStackTrace();
        }
        if(hashMap.containsKey("result")){
            return new JSONObject(hashMap);
        }else
            return jsonObject;
    }

    //用户把新的数据集添加进群组
    public static JSONObject doInsertGroupDataRegisterRelation(JSONObject json) {
        HashMap<String,Object> hashMap = new HashMap<>();
        try{
            String groupId = json.getString("group_id");
            String userId = json.getString("user_id");
            String dataName = json.getString("data_name");
            String dataType = json.getString("data_type");
            DataNode dataNode = new DataNode();
            dataNode.setUser_id(userId);
            dataNode.setData_name(dataName);
            dataNode.setData_type(dataType);

            //DataNode 在构造时，尽量不使用下面的几个属性么？？？？？？？？？？？？？？？？？？？？？？？？？？
//            String rowNums = json.getString("row_nums");
//            String attrNums = json.getString("attr_nums");
//            String filePath = json.getString("file_path");

            //假设这里一定能找到群组吧
            GroupNode groupNode = SQLHandler.queryGroupByGroupId(groupId);
            boolean success2 = SQLHandler.insertGroupDataRegisterRelation(groupNode,dataNode);
            boolean success1 = SQLHandler.insertDataNode(dataNode);
            if(success1&success2){
                hashMap.put("result",1);
            }else{
                hashMap.put("result",0);
            }
        }catch (JSONException e){
            e.printStackTrace();
            hashMap.put("result",0);
        }
        return new JSONObject(hashMap);
    }

    //处理查询群组数据集的请求
    public static JSONObject doQueryDataNodesByGroupId(JSONObject json) {
        HashMap<String, Object> hashMap = new HashMap<>();
        JSONObject jsonObject = new JSONObject();
        try{
            List<DataNode>  dataNodes = SQLHandler.queryRegisterdDataNodesByGroupID(json.getString("group_id"));
            if(dataNodes==null||dataNodes.size()==0){
                jsonObject.put("result",0);
//                hashMap.put("result",0);
            }else{
                JSONArray jsonArray = new JSONArray();
                for(DataNode dataNode:dataNodes){
                    JSONObject data = new JSONObject();
                    data.put("user_id",dataNode.getUser_id());
                    data.put("data_name",dataNode.getData_name());
                    data.put("data_type",dataNode.getData_type());
                    data.put("row_nums",dataNode.getRow_nums());
                    data.put("attr_nums",dataNode.getAttr_nums());
                    data.put("file_path",dataNode.getFile_path());
                    jsonArray.put(data);
                }
                jsonObject.put("datas",jsonArray);
                jsonObject.put("result",1);

            }
        }catch (JSONException e){
            //JSON异常处理中，不可用json
            hashMap.put("result",0);
            e.printStackTrace();
        }
        if(hashMap.containsKey("result")){
            return new JSONObject(hashMap);
        }else
            return jsonObject;
    }

    //处理任务创建,设置现在就返回IP
    public static JSONObject doTask(JSONObject json, Map<String,Socket> userMap){
        JSONObject jsonObject = new JSONObject();
        HashMap<String,Object> hashMap = new HashMap<>();
        try{
            String taskId = Tools.getRandomID("task");
            String taskName = json.getString("task_name");
            String initiatorId = json.getString("initiator_id");
            String groupId = json.getString("group_id");
            ComputeTask computeTask = new ComputeTask();
            //因为这里只有一种构造方法，所以只能用set
            computeTask.setInitiator_id(initiatorId);
            computeTask.setTask_name(taskName);
            computeTask.setTask_id(taskId);
            Boolean success = SQLHandler.insertComputeTask(computeTask);
            if(success){
                jsonObject.put("task_id",taskId);
                List<String> slavers = new ArrayList<>();
                JSONObject powerMsg = doPowerSlaver();
                powerMsg.put("result",2);
                List<DataNode> dataNodes = SQLHandler.queryRegisterdDataNodesByGroupID(groupId);
                for(DataNode dataNode : dataNodes) {
                    String slaverId = dataNode.getUser_id();
                    Socket socket = userMap.get(slaverId);
                    if(socket!=null && socket.isConnected()) {
                        DataOutputStream dataOutputStream = null;
                        try {
                            dataOutputStream = new DataOutputStream(socket.getOutputStream());
                            dataOutputStream.write(powerMsg.toString().getBytes());
                            dataOutputStream.flush();


                            //这里需要把任务添加到到works_on ,但是有一些问题，目前的works_on是task_id和 group_id，
                            //一个group中可能只有部分人在线，我想是不是应该加上 slaver_id 呢？？？？？？？，另外group_id，可以直接放到computetask表中去


                            slavers.add(slaverId);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
                if(!slavers.isEmpty()) {
                    jsonObject.put("result",1);
                    jsonObject.put("slavers_id", new JSONArray(slavers.toArray()));
                }else {
                    jsonObject.put("result",0);
                }

            }
            else{
                jsonObject.put("result",0);
            }
        }catch (JSONException e){
            hashMap.put("result",0);
            e.printStackTrace();
        }
        if(hashMap.containsKey("result"))
            return new JSONObject(hashMap);
        return jsonObject;
    }

    //启动slaver
    public static JSONObject doPowerSlaver(){
        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject.put("result","OK");
            jsonObject.put("text","int main()\n{printf(\"hello world!\");}\n");
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonObject;
    }

    //处理slaver报告的中间过程消息
    public static JSONObject doProcessMsg(JSONObject json){
        JSONObject jsonObject = new JSONObject();
        try{
            String userId = json.getString("user_id");
            String taskId =json.getString("task_id");
            String text = json.getString("text");

            //利用taskId找到创建的用户，这个方法暂时还没写
            //String userId = SQLHandler.quer

            jsonObject.put("user_id",userId);
            jsonObject.put("text",text);

            //

        }catch (JSONException e){
            e.printStackTrace();
            return new JSONObject(new HashMap<>().put("result",3));
        }
        return jsonObject;
    }

    //处理终止消息
    public static JSONObject doStop(JSONObject json){
        JSONObject jsonObject = new JSONObject();
        return jsonObject;
    }
}
