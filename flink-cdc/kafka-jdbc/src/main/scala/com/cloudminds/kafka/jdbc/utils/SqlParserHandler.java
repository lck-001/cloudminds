package com.cloudminds.kafka.jdbc.utils;

import com.cloudminds.kafka.jdbc.jdbc.ExcludeColumns;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.util.TablesNamesFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SqlParserHandler {
    private static final Logger logger = LoggerFactory.getLogger(SqlParserHandler.class);

//    private static List excludeColumns = Arrays.asList("db","table","bigdata_method","event_time");
    /**
     * sql 获取列值
     * @param singleSql
     * @return 返回列
     */
    public static List<String> getSelectColumns(String singleSql, HashMap<String, HashMap<String, String>> schemaMap) throws Exception{
        if (singleSql == null) {
            throw new Exception("params is null!");
        }
        CCJSqlParserManager ccjSqlParserManager = new CCJSqlParserManager();
        Statement statement;
        List<String> columns = new ArrayList<String>();
        try {
            statement = ccjSqlParserManager.parse(new StringReader(singleSql));
            if (statement instanceof Select) {
                Select selectStatement = (Select) statement;
                SelectBody selectBody = selectStatement.getSelectBody();
                List<SelectItem> selectItems =  ((PlainSelect) selectBody).getSelectItems();
                if (selectItems != null) {
                    for (SelectItem item : selectItems ) {
                        if (item instanceof AllColumns) {
                            /*String column = item.toString();
                            columns.add(column);*/
                            List<String> convertColumns = convertColumn(singleSql, schemaMap);
                            columns.addAll(convertColumns);
                        }
                        if (item instanceof AllTableColumns) {
                            columns.add(item.toString());
                        }
                        if (item instanceof SelectExpressionItem) {
                            Alias alias = ((SelectExpressionItem) item).getAlias();
                            Expression expression = ((SelectExpressionItem) item).getExpression();
                            if (alias != null) {
                                String column = alias.getName();
                                columns.add(column);
                            } else if (expression != null) {
                                columns.add(expression.toString());
                            }
                        }
                    }
                }
            }
        } catch (JSQLParserException e) {
            logger.error(e.getMessage());
            throw new JSQLParserException(e.getMessage());
        }
        return columns;
    }

    /**
     * @param singleSql
     * @return 获取查询的表名称
     */
    public static List<String> getSelectTables(String singleSql) throws Exception{
        if (singleSql == null) {
            throw new Exception("params is null!");
        }
        CCJSqlParserManager ccjSqlParserManager = new CCJSqlParserManager();
        Statement statement;
        List<String> tables = new ArrayList<String>();
        try {
            statement = ccjSqlParserManager.parse(new StringReader(singleSql));
            TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
            List<String> tableList = tablesNamesFinder.getTableList(statement);
            tables.addAll(tableList);
        }catch (JSQLParserException e){
            logger.error(e.getMessage());
            throw new JSQLParserException(e.getMessage());
        }
        return tables;
    }

    private static List<String> convertColumn(String singleSql,HashMap<String, HashMap<String, String>> schemaMap) throws Exception {
        List<String> columns = new ArrayList<String>();
        List<String> tables = getSelectTables(singleSql);
        HashMap<String, String> columnMap = (HashMap<String, String>) schemaMap.get(tables.get(0));
        for (Map.Entry<String,String> field : columnMap.entrySet()) {
            if (!ExcludeColumns.createMap().containsKey(field.getKey())){
                columns.add(field.getKey());
            }
        }
        return columns;
    }
}
