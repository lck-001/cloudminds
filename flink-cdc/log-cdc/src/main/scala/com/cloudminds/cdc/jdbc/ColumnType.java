package com.cloudminds.cdc.jdbc;


public class ColumnType {
    public static String convertColumnType(String column){
        String columnType = "";
        switch (column.toLowerCase()){
            case "int":
            case "int4":
            case "tinyint":
            case "smallint":
            case "mediumint":
            case "serial":
            case "smallserial":
            case "integer":
                columnType="int";
                break;
            case "float":
            case "real":
                columnType="float";
                break;
            case "double":
            case "double precision":
//            case "decimal":
            case "numeric":
            case "float8":
                columnType="double";
                break;
//            case "blob":
//            case "tinyblob":
//            case "mediumblob":
//            case "longblob":
//            case "bytea":
//                columnType="bytes";
//                break;
            case "int8":
            case "bigint":
            case "bigserial":
                columnType="long";
                break;
            case "boolean":
                columnType="boolean";
                break;
            default:
                columnType="string";

        }

        return columnType;
    }
}
