import Entity.ComputeTask;
import Entity.DataNode;
import Entity.GroupNode;
import Entity.UserNode;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.xml.crypto.Data;
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
            hashMap.put("reply",0);
            hashMap.put("user_id",userId);
        } catch (JSONException | SQLException e) {
            e.printStackTrace();
            return sendErrorMsg(0);
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
//                hashMap.put("result",0);
                return sendErrorMsg(1);
            }
            else{
                hashMap.put("result",1);
                hashMap.put("reply",1);
                hashMap.put("user_id",result);
                hashMap.put("user_name",SQLHandler.queryUserByID(result).getUser_name());
            }
        } catch (JSONException | SQLException e) {
            e.printStackTrace();
//            hashMap.put("result",0);
            return sendErrorMsg(1);
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
                return sendErrorMsg(2);
            }
            else{
                hashMap.put("result", 1);
                hashMap.put("reply",2);
                hashMap.put("user_id",user_id);
                hashMap.put("user_name",userNode.getUser_name());
                hashMap.put("email",userNode.getEmail());
            }
        }catch (JSONException e){
            e.printStackTrace();
            return sendErrorMsg(2);
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
                hashMap.put("reply",3);
            }
            else {
                GroupNode groupNode = new GroupNode(groupId, groupName, dataType);
                groupNode.setOwner(userNode);
                groupNode.setOwner_id(creatorId);
                groupNode.setDescription(description);
                groupNode.setMember_num(memberNum);
                SQLHandler.insertGroup(groupNode);
                hashMap.put("result",1);
                hashMap.put("reply",3);
                hashMap.put("group_id",groupId);
            }
        }catch (JSONException e){
            e.printStackTrace();
            hashMap.put("result",0);
            hashMap.put("reply",3);
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
                hashMap.put("reply",4);
            }else{
                hashMap.put("result",0);
                hashMap.put("reply",4);
            }
        }catch (JSONException e){
            hashMap.put("result",0);
            hashMap.put("reply",4);
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
                jsonObject.put("reply",5);
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
                jsonObject.put("reply",5);
            }
        }catch (JSONException e){
            //JSON异常处理中，不可用json
            hashMap.put("result",0);
            hashMap.put("reply",5);
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
                jsonObject.put("reply",6);
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
                jsonObject.put("reply",6);
            }
        }catch (JSONException e){
            //JSON异常处理中，不可用json
            hashMap.put("result",0);
            hashMap.put("reply",6);
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
                hashMap.put("reply",7);
            }else{
                hashMap.put("result",0);
                hashMap.put("reply",7);
            }
        }catch (JSONException e){
            e.printStackTrace();
            hashMap.put("result",0);
            hashMap.put("reply",7);
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
                jsonObject.put("reply",8);
//                hashMap.put("result",0);
            }else{
                JSONArray jsonArray = new JSONArray();
                for(DataNode dataNode:dataNodes){
                    JSONObject data = new JSONObject();
                    data.put("user_name",dataNode.getUser_name());
//                    data.put("user_id",dataNode.getUser_id());
                    data.put("data_name",dataNode.getData_name());
//                    data.put("data_type",dataNode.getData_type());
                    data.put("row_nums",dataNode.getRow_nums());
                    data.put("attr_nums",dataNode.getAttr_nums());
//                    data.put("file_path",dataNode.getFile_path());
                    jsonArray.put(data);
                }
                jsonObject.put("datas",jsonArray);
                jsonObject.put("result",1);
                jsonObject.put("reply",8);
            }
        }catch (JSONException e){
            //JSON异常处理中，不可用json
            hashMap.put("result",0);
            hashMap.put("reply",8);
            e.printStackTrace();
        }
        if(hashMap.containsKey("result")){
            return new JSONObject(hashMap);
        }else
            return jsonObject;
    }

    //处理任务创建
    public static JSONObject doTask(JSONObject json){
        JSONObject jsonObject = new JSONObject();
        try{
            String taskId = Tools.getRandomID("task");
            String taskName = json.getString("task_name");
            String initiatorId = json.getString("initiator_id");
            String groupId = json.getString("group_id");
            String code = json.getString("code");
            int state = json.getInt("state");
            ComputeTask computeTask = new ComputeTask();
            //因为这里只有一种构造方法，所以只能用set
            computeTask.setInitiator_id(initiatorId);
            computeTask.setCode(code);
            computeTask.setTask_name(taskName);
            computeTask.setTask_id(taskId);
            computeTask.setState(state);
            computeTask.setGroup_id(groupId);
            Boolean success = SQLHandler.insertComputeTask(computeTask);
            if(success){
                jsonObject.put("result",1);
                jsonObject.put("reply",9);
            }else{
                jsonObject.put("result",0);
                jsonObject.put("reply",9);
            }
        }catch (JSONException e){
            e.printStackTrace();
            return sendErrorMsg(9);
        }
        return jsonObject;
    }

    //启动slaver,并发送代码
    public static JSONObject doPowerSlaver(JSONObject json, Map<String,Socket> userMap){
        JSONObject jsonObject = new JSONObject();
        try {
            String taskId = json.getString("task_id");
            ComputeTask computeTask = SQLHandler.queryTaskByTaskId(taskId);
            if(computeTask==null){
                return sendErrorMsg(10);
            }
            List<String> slavers = new ArrayList<>();
            JSONObject powerMsg = new JSONObject();
            powerMsg.put("result",1);
            powerMsg.put("push",0);
            powerMsg.put("task_id",taskId);
            powerMsg.put("data_type",computeTask.getData_type());
            powerMsg.put("cost",computeTask.getCost());
            powerMsg.put("initiator_id",computeTask.getInitiator_id());
            powerMsg.put("security_score",computeTask.getSecurity_score());
            powerMsg.put("start_time",computeTask.getStart_time());
            powerMsg.put("end_time",computeTask.getEnd_time());
            powerMsg.put("state",computeTask.getState());
            powerMsg.put("task_name",computeTask.getTask_name());
            powerMsg.put("group_id",computeTask.getGroup_id());
            powerMsg.put("code",computeTask.getCode());
            List<DataNode> dataNodes = SQLHandler.queryRegisterdDataNodesByGroupID(SQLHandler.queryTaskByTaskId(taskId).getGroup_id());
            for(DataNode dataNode : dataNodes) {
                String slaverId = dataNode.getUser_id();
                Socket socket = userMap.get(slaverId);
                if(socket!=null && socket.isConnected()) {
                    DataOutputStream dataOutputStream = null;
                    try {
                        dataOutputStream = new DataOutputStream(socket.getOutputStream());
                        dataOutputStream.write(powerMsg.toString().getBytes());
                        dataOutputStream.flush();
                        SQLHandler.insertWorksOn(taskId,slaverId);
                        slavers.add(slaverId);
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
            if(!slavers.isEmpty()) {
                jsonObject.put("result",1);
                jsonObject.put("reply",9);
                jsonObject.put("slavers_id", new JSONArray(slavers.toArray()));
            }else {
                jsonObject.put("result",0);
                jsonObject.put("reply",9);
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonObject;
    }

    //处理slaver报告的中间过程消息
    public static JSONObject doProcessMsg(JSONObject json,Map<String,Socket> userMap){
        JSONObject jsonObject = new JSONObject();
        try{
            String slaverId = json.getString("slaver_id");
            String taskId =json.getString("task_id");
            String text = json.getString("text");
            String masterId = SQLHandler.queryTaskByTaskId(taskId).getInitiator_id();
            if(userMap.keySet().contains(masterId)) {
                Socket socket = userMap.get(masterId);
                if(socket.isConnected()) {
                    JSONObject midMsg = new JSONObject();
                    midMsg.put("push", 1);
                    midMsg.put("slaver_id", slaverId);
                    midMsg.put("text", text);
                    DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                    dataOutputStream.write(midMsg.toString().getBytes());
                    dataOutputStream.flush();
                    jsonObject.put("result",1);
                    jsonObject.put("reply",11);
                }
            }
            //

        }catch (JSONException e){
            e.printStackTrace();
            return sendErrorMsg(11);
        } catch (IOException e) {
            e.printStackTrace();
            return sendErrorMsg(11);
        }
        return jsonObject;
    }

    //处理终止消息
    public static JSONObject doStop(JSONObject json,Map<String,Socket> userMap){
        JSONObject jsonObject = new JSONObject();
        try{
            String taskId = json.getString("task_id");
            ArrayList<String> slavers = SQLHandler.querySlaverIdByTaskId(taskId);
            for(String slaverId:slavers){
                if(userMap.keySet().contains(slaverId)){
                    Socket slaverSocekt = userMap.get(slaverId);
                    if(slaverSocekt.isConnected()){
                        JSONObject stopMsg = new JSONObject();
                        DataOutputStream dataOutputStream = new DataOutputStream(slaverSocekt.getOutputStream());
                        stopMsg.put("push",2);
                        stopMsg.put("result",1);
                        dataOutputStream.write(stopMsg.toString().getBytes());
                        dataOutputStream.flush();
                    }
                }
            }
            jsonObject.put("result",1);
            jsonObject.put("reply",12);
        }catch (JSONException e){
            e.printStackTrace();
            return sendErrorMsg(12);
        } catch (IOException e) {
            e.printStackTrace();
            return sendErrorMsg(12);
        }
        return jsonObject;
    }

    //通过状态和发起者的id来查询任务
    public static JSONObject doQueryTaskByStateAndUserId(JSONObject json){
        JSONObject jsonObject = new JSONObject();
        try{
            String userId = json.getString("user_id");
            int state = json.getInt("state");
            ArrayList<ComputeTask> computeTasks = SQLHandler.queryComputeTaskByInitiatorIDAndState(userId,state);
            JSONArray jsonArray = new JSONArray();
            for(ComputeTask task : computeTasks){
                JSONObject tmp = new JSONObject();
                tmp.put("task_id",task.getTask_id());
                tmp.put("code",task.getCode());
                tmp.put("group_id",task.getGroup_id());
                tmp.put("data_type",task.getData_type());
                tmp.put("cost",task.getCost());
                tmp.put("initiator_id",task.getInitiator_id());
                tmp.put("security_score",task.getSecurity_score());
                tmp.put("start_time",task.getStart_time());
                tmp.put("end_time",task.getEnd_time());
                tmp.put("state",task.getState());
                tmp.put("task_name",task.getTask_name());
                tmp.put("group_id",task.getGroup_id());
                jsonArray.put(tmp);
            }
            jsonObject.put("tasks",jsonArray);
            jsonObject.put("reply",12);
            jsonObject.put("result",1);
        }catch (JSONException e){
            e.printStackTrace();
            return new JSONObject(new HashMap().put("result",0));
        }
        return jsonObject;
    }


    //根据用户id和群组id找到对应的数据集
    public static JSONObject doQueryDataNameByUserIdAndGroupId(JSONObject json){
        JSONObject jsonObject = new JSONObject();
        try{
            String userId = json.getString("user_id");
            String groupId = json.getString("group_id");
            ArrayList<String> dataNames = SQLHandler.queryDataSetNameByUserIdAndGroupID(userId,groupId);
            JSONArray jsonArray = new JSONArray();
            for(String dataName : dataNames){
                jsonArray.put(dataName);
            }
            jsonObject.put("data_names",jsonArray);
            jsonObject.put("reply",13);
            jsonObject.put("result",1);

        }catch (JSONException e){
            e.printStackTrace();
            return new JSONObject(new HashMap().put("result",0));
        }
        return jsonObject;
    }


    static JSONObject sendErrorMsg(int reply){
        HashMap<String,Integer> hashMap =new HashMap<>();
        hashMap.put("reply",reply);
        hashMap.put("result",0);
        JSONObject jsonObject = new JSONObject(hashMap);
        return jsonObject;
    }

}
