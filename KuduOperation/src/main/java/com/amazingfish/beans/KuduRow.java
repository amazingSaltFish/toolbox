package com.amazingfish.beans;

import java.util.List;

public class KuduRow {

    private String tableName;
    private String newTableName;
    private List<KuduColumn> rows;
    private AlterTableEnum alterTableEnum = AlterTableEnum.NONE;

    public String getTableName() {
        return tableName;
    }

    public KuduRow setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public String getNewTableName() {
        return newTableName;
    }

    public KuduRow setNewTableName(String newTableName) {
        this.newTableName = newTableName;
        return this;
    }

    public List<KuduColumn> getRows() {
        return rows;
    }

    public void setRows(List<KuduColumn> rows) {
        this.rows = rows;
    }

    public AlterTableEnum getAlterTableEnum() {
        return alterTableEnum;
    }

    public void setAlterTableEnum(AlterTableEnum alterTableEnum) {
        this.alterTableEnum = alterTableEnum;
    }

    @Override
    public String toString() {
        return "KuduRow{" +
                "tableName='" + tableName + '\'' +
                ", newTableName='" + newTableName + '\'' +
                ", rows=" + rows +
                ", alterTableEnum=" + alterTableEnum +
                '}';
    }

    public enum AlterTableEnum {

        DROP_TABLE("DROP_TABLE", "删除表"),
        RENAME_TABLE("RENAME_TABLE", "重命名表"),
        NONE("NONE", "不做操作");

        private String type;
        private String desc;

        AlterTableEnum(String type, String desc) {
            this.type = type;
            this.desc = desc;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getDesc() {
            return desc;
        }

        public void setDesc(String desc) {
            this.desc = desc;
        }
    }
}

