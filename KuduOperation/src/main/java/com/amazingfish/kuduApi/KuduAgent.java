package com.amazingfish.kuduApi;

import com.amazingfish.beans.KuduColumn;
import com.amazingfish.beans.KuduRow;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KuduAgent {

    private static final Logger logger = LoggerFactory.getLogger(KuduAgent.class);

    private KuduClient client;
    private final static SessionConfiguration.FlushMode FLASH_MODE_MULT = SessionConfiguration.FlushMode.MANUAL_FLUSH;
    private final static SessionConfiguration.FlushMode FLASH_MODE_SINGLE = SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC;
    private final static int BUFFER_SPACE = 1000;

    public void setClient(KuduClient client) {
        this.client = client;
    }

    //    static {
//        client = new KuduClient.KuduClientBuilder(master).build();
//    }

    /**
     * 打印kudu表中所有的字段及字段的详细信息
     *
     * @param table kudu表表名
     * @throws KuduException Exception
     */
    public void detailOpen(String table) throws KuduException {
        KuduTable kuduTable = client.openTable(table);
        Schema schema = kuduTable.getSchema();
        List<ColumnSchema> columns = schema.getColumns();
        for (ColumnSchema column : columns) {
            KuduColumn kuduColumn = new KuduColumn();
            kuduColumn.setColumnType(column.getType());
            kuduColumn.setDefaultValue(column.getDefaultValue());
            kuduColumn.setColumnName(column.getName());
            kuduColumn.setNullAble(column.isNullable());
            kuduColumn.setPrimaryKey(column.isKey());
            kuduColumn.setEncoding(column.getEncoding());
            System.out.println(kuduColumn.toString());
            System.out.println("======================");
        }
    }

    /**
     * 查询 返回多条数据
     *
     * @param table    kudu表名
     * @param client   kuduClient
     * @param entities
     * @return
     */
    public List<Map<String, Object>> select(String table, KuduClient client, List<KuduColumn> entities) {
        KuduTable kuduTable = null;
        KuduScanner build = null;
        KuduScanner.KuduScannerBuilder kuduScannerBuilder = null;
        List<Map<String, Object>> resList = new ArrayList<Map<String, Object>>();
        try {
            kuduTable = client.openTable(table);
            List<String> columnNames = KuduAgentUtils.getColumnNames(entities);
            kuduScannerBuilder = client.newScannerBuilder(kuduTable);
            kuduScannerBuilder = kuduScannerBuilder.setProjectedColumnNames(columnNames);
            KuduAgentUtils.setKuduPredicates(kuduTable, entities, kuduScannerBuilder);
            build = kuduScannerBuilder.build();
            while (build.hasMoreRows()) {
                RowResultIterator results = build.nextRows();
                while (results.hasNext()) {
                    RowResult result = results.next();
                    Map<String, Object> rowsResult = KuduAgentUtils.getRowsResult(result, entities);
                    resList.add(rowsResult);
                }
            }
        } catch (KuduException e) {
            e.printStackTrace();
            logger.error("kudu执查询操作失败，失败信息:cause-->{},message-->{}", e.getCause(), e.getMessage());
//            throw new CustomerException(ExceptionConstant.KUDU_ERROR_CODE, ExceptionConstant.AGENT_ERROR_SYS, e.getMessage());
        } finally {
            KuduAgentUtils.close(build, client);
        }
        return resList;
    }


    /**
     * 批量插入
     *
     * @param table   表名
     * @param client  kuduClient
     * @param entitys
     * @throws KuduException
     */
    public void insert(String table, KuduClient client, List<KuduRow> entitys) {
        KuduSession session = null;
        try {
            KuduTable kuduTable = client.openTable(table);
            session = client.newSession();
            session.setFlushMode(FLASH_MODE_MULT);
            session.setMutationBufferSpace(BUFFER_SPACE);
            for (KuduRow entity : entitys) {
                Insert insert = kuduTable.newInsert();
                KuduAgentUtils.operate(entity, insert, session);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("kudu执行插入操作失败，失败信息:cause-->{},message-->{}", e.getCause(), e.getMessage());
//            throw new CustomerException(ExceptionConstant.KUDU_ERROR_CODE, ExceptionConstant.AGENT_ERROR_SYS, e.getMessage());
        } finally {
            List<OperationResponse> res = KuduAgentUtils.close(session, client);
        }
    }

    /**
     * 单条插入
     *
     * @param table
     * @param client
     * @param entity
     * @throws KuduException
     */
    public void insert(String table, KuduClient client, KuduRow entity) throws KuduException {
        KuduSession session = null;
        try {
            KuduTable kuduTable = client.openTable(table);
            session = client.newSession();
            session.setFlushMode(FLASH_MODE_SINGLE);
            Insert insert = kuduTable.newInsert();
            OperationResponse operate = KuduAgentUtils.operate(entity, insert, session);
            logger.info("insert 插入数据:{}", operate.getRowError());
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("kudu执行插入操作失败，失败信息:cause-->{},message-->{}", e.getCause(), e.getMessage());
//            throw new CustomerException(ExceptionConstant.KUDU_ERROR_CODE, ExceptionConstant.AGENT_ERROR_SYS, e.getMessage());
        } finally {
            KuduAgentUtils.close(session, client);
        }

    }

    /**
     * 批量更新
     *
     * @param table
     * @param client
     * @param entitys
     * @throws KuduException
     */
    public void update(String table, KuduClient client, List<KuduRow> entitys) {
        KuduSession session = null;
        try {
            KuduTable kuduTable = client.openTable(table);
            session = client.newSession();
            session.setFlushMode(FLASH_MODE_MULT);
            session.setMutationBufferSpace(BUFFER_SPACE);
            for (KuduRow entity : entitys) {
                Update update = kuduTable.newUpdate();
                KuduAgentUtils.operate(entity, update, session);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("kudu执行更新操作失败，失败信息:cause-->{},message-->{}", e.getCause(), e.getMessage());
//            throw new CustomerException(ExceptionConstant.KUDU_ERROR_CODE, ExceptionConstant.AGENT_ERROR_SYS, e.getMessage());
        } finally {
            List<OperationResponse> res = KuduAgentUtils.close(session, client);
        }
    }

    /**
     * 单条更新
     *
     * @param table
     * @param client
     * @param entity
     * @throws KuduException
     */
    public void update(String table, KuduClient client, KuduRow entity) throws KuduException {
        KuduSession session = null;
        try {
            KuduTable kuduTable = client.openTable(table);
            session = client.newSession();
            session.setFlushMode(FLASH_MODE_SINGLE);
            Update update = kuduTable.newUpdate();
            OperationResponse operate = KuduAgentUtils.operate(entity, update, session);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("kudu执行更新操作失败，失败信息:cause-->{},message-->{}", e.getCause(), e.getMessage());
//            throw new CustomerException(ExceptionConstant.KUDU_ERROR_CODE, ExceptionConstant.AGENT_ERROR_SYS, e.getMessage());
        } finally {
            KuduAgentUtils.close(session, client);
        }
    }

    /**
     * 批量删除 删除只能是主键
     *
     * @param table
     * @param client
     * @param entitys
     * @throws KuduException
     */
    public void delete(String table, KuduClient client, List<KuduRow> entitys) throws KuduException {
        KuduSession session = null;
        try {
            KuduTable kuduTable = client.openTable(table);
            session = client.newSession();
            session.setFlushMode(FLASH_MODE_MULT);
            session.setMutationBufferSpace(BUFFER_SPACE);
            for (KuduRow entity : entitys) {
                Delete delete = kuduTable.newDelete();
                KuduAgentUtils.operate(entity, delete, session);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("kudu执行删除操作失败，失败信息:cause-->{},message-->{}", e.getCause(), e.getMessage());
//            throw new CustomerException(ExceptionConstant.KUDU_ERROR_CODE, ExceptionConstant.AGENT_ERROR_SYS, e.getMessage());
        } finally {
            KuduAgentUtils.close(session, client);
        }

    }

    /**
     * 单条删除 删除只能是主键
     *
     * @param table
     * @param client
     * @param entity
     * @throws KuduException
     */
    public void delete(String table, KuduClient client, KuduRow entity) throws KuduException {
        KuduSession session = null;
        try {
            KuduTable kuduTable = client.openTable(table);
            session = client.newSession();
            session.setFlushMode(FLASH_MODE_SINGLE);
            Delete delete = kuduTable.newDelete();
            OperationResponse operate = KuduAgentUtils.operate(entity, delete, session);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("kudu执行删除操作失败，失败信息:cause-->{},message-->{}", e.getCause(), e.getMessage());
//            throw new CustomerException(ExceptionConstant.KUDU_ERROR_CODE, ExceptionConstant.AGENT_ERROR_SYS, e.getMessage());
        } finally {
            KuduAgentUtils.close(session, client);
        }
    }

    /**
     * 针对表的操作
     * 修改表名 删除表  增加字段 删除字段
     *
     * @param client
     * @param entitys
     * @throws KuduException
     */
    public void alter(KuduClient client, List<KuduRow> entitys) throws KuduException {
        try {
            for (KuduRow entity : entitys) {
                if (entity.getAlterTableEnum().equals(KuduRow.AlterTableEnum.RENAME_TABLE)) {
                    AlterTableResponse alterTableResponse = renameTable(client, entity);
                    continue;
                }
                if (entity.getAlterTableEnum().equals(KuduRow.AlterTableEnum.DROP_TABLE)) {
                    DeleteTableResponse deleteTableResponse = dropTable(client, entity);
                    continue;
                }
                AlterTableResponse alterTableResponse = alterColumn(client, entity);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("kudu执行表alter操作失败，失败信息:cause-->{},message-->{}", e.getCause(), e.getMessage());
//            throw new CustomerException(ExceptionConstant.KUDU_ERROR_CODE, ExceptionConstant.AGENT_ERROR_SYS, e.getMessage());
        } finally {
            KuduAgentUtils.close((KuduSession) null, client);
        }

    }

    /**
     * 针对表的操作
     * 修改表名 删除表  增加字段 删除字段
     *
     * @param client
     * @param entity
     * @throws KuduException
     */
    public void alter(KuduClient client, KuduRow entity) throws KuduException {
        try {
            if (entity.getAlterTableEnum().equals(KuduRow.AlterTableEnum.RENAME_TABLE)) {
                AlterTableResponse alterTableResponse = renameTable(client, entity);
                return;
            }
            if (entity.getAlterTableEnum().equals(KuduRow.AlterTableEnum.DROP_TABLE)) {
                DeleteTableResponse deleteTableResponse = dropTable(client, entity);
                return;
            }
            AlterTableResponse alterTableResponse = alterColumn(client, entity);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("kudu执行表alter操作失败，失败信息:cause-->{},message-->{}", e.getCause(), e.getMessage());
//            throw new CustomerException(ExceptionConstant.KUDU_ERROR_CODE, ExceptionConstant.AGENT_ERROR_SYS, e.getMessage());
        } finally {
            KuduAgentUtils.close((KuduSession) null, client);
        }

    }

    /**
     * 修改表名
     *
     * @param client
     * @param entity
     * @return
     * @throws KuduException
     */
    private AlterTableResponse renameTable(KuduClient client, KuduRow entity) throws KuduException {
        AlterTableOptions ato = new AlterTableOptions();
        ato.renameTable(entity.getNewTableName());
        return client.alterTable(entity.getTableName(), ato);
    }

    /**
     * 删除表
     *
     * @param client
     * @param entity
     * @return
     * @throws KuduException
     */
    private DeleteTableResponse dropTable(KuduClient client, KuduRow entity) throws KuduException {
        return client.deleteTable(entity.getTableName());
    }


    /**
     * 列级别的alter
     *
     * @param client
     * @param entity
     * @return
     * @throws KuduException
     */
    private AlterTableResponse alterColumn(KuduClient client, KuduRow entity) throws KuduException {
        AlterTableOptions ato = new AlterTableOptions();
        for (KuduColumn column : entity.getRows()) {
            if (column.getAlterColumnEnum().equals(KuduColumn.AlterColumnEnum.ADD_COLUMN) && !column.isNullAble()) {
                ato.addColumn(column.getColumnName(), column.getColumnType(), column.getDefaultValue());
            } else if (column.getAlterColumnEnum().equals(KuduColumn.AlterColumnEnum.ADD_COLUMN) && column.isNullAble()) {
                ato.addNullableColumn(column.getColumnName(), column.getColumnType());
            } else if (column.getAlterColumnEnum().equals(KuduColumn.AlterColumnEnum.DROP_COLUMN)) {
                ato.dropColumn(column.getColumnName());
            } else if (column.getAlterColumnEnum().equals(KuduColumn.AlterColumnEnum.RENAME_COLUMN)) {
                ato.renameColumn(column.getColumnName(), column.getNewColumnName());
            } else {
                continue;
            }
        }
        AlterTableResponse alterTableResponse = client.alterTable(entity.getTableName(), ato);
        return alterTableResponse;
    }

}

