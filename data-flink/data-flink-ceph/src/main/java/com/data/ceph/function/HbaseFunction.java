package com.data.ceph.function;

import com.google.inject.internal.cglib.proxy.$NoOp;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.List;


public class HbaseFunction {

    public static void main(String[] args) throws Exception {
        getUserInfo("veUiAtDPgH9qJI5XAXNv");

//        scanTable();
    }

    private static Connection connection = null;
    private static ResultScanner rs = null;
    private static Table table = null;

    public static Table open() throws Exception {
        System.setProperty("zookeeper.sasl.client", "false");
        Configuration hconf = HBaseConfiguration.create();
        hconf.set(HConstants.ZOOKEEPER_QUORUM, "172.16.23.37,172.16.23.38,172.16.23.39");
        hconf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
        hconf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase");
        //指定用户名为hbase的用户去访问hbase服务
        UserGroupInformation userGroupInformation = UserGroupInformation.createRemoteUser("hive");
        connection = ConnectionFactory.createConnection(hconf, User.create(userGroupInformation));
        table = connection.getTable(TableName.valueOf("cloud:user_info"));
        return table;
    }


    public static void getUserInfo(String access_key) throws Exception {
        if (table == null) open();
        Get get = new Get(Bytes.toBytes(access_key));
        Result result = table.get(get);
        List<Cell> cells = result.listCells();

        if (CollectionUtils.isEmpty(cells)) {
            System.out.println("查询为空 : " + access_key);
            return;
        }
        for (Cell cell : cells) {
            String qualifier = new String(CellUtil.cloneQualifier(cell));
            String value = new String(CellUtil.cloneValue(cell), "UTF-8");
            System.out.println(access_key + "  :  " + qualifier + "\t" + value);
        }
        System.out.println("==============================================================" + "\n");
//        close();
    }

    public static void scanTable() throws Exception {
        if (table == null) open();
        //b. 扫描操作
        Scan scan = new Scan();

        String regex = "^[a-z0-9A-Z]+$";

        //c. 获取返回值
        ResultScanner rs = table.getScanner(scan);
        //d. 打印扫描信息
        for (Result r : rs) {
            //单元格
            Cell[] cells = r.rawCells();

            for (Cell cs : cells) {
                if (Bytes.toString(CellUtil.cloneRow(cs)).matches(regex)) {
                    System.out.println("RowKey:" + Bytes.toString(CellUtil.cloneRow(cs)));
                    System.out.println("ColumnFamilly:" + Bytes.toString(CellUtil.cloneFamily(cs)));
                    System.out.println("Column:" + Bytes.toString((CellUtil.cloneQualifier(cs))));
                    System.out.println("Value:" + Bytes.toString((CellUtil.cloneValue(cs))));
                } else {
                    continue;
                }
            }
        }
    }

    public static void close() throws Exception {
        if (rs != null) rs.close();
        if (table != null) table.close();
        if (connection != null) connection.close();
    }

}
