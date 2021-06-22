package com.amazingfish.beans;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduPredicate;

public class KuduColumn {

    /**
     * crud
     */
    private String columnName;
    private Type columnType;
    private Object columnValue;
    private boolean isUpdate;

    private boolean isPrimaryKey = false;

    /**
     * alter
     */
    private AlterColumnEnum alterColumnEnum = AlterColumnEnum.NONE;
    private Object defaultValue;
    private boolean isNullAble;
    private String newColumnName;
    private ColumnSchema.Encoding encoding;

    public ColumnSchema.Encoding getEncoding() {
        return encoding;
    }

    public void setEncoding(ColumnSchema.Encoding encoding) {
        this.encoding = encoding;
    }

    /**
     * select
     */
    private KuduPredicate.ComparisonOp comparisonOp;
    private Object comparisonValue;
    private boolean isSelect;

    public String getColumnName() {
        return columnName;
    }

    public KuduColumn setColumnName(String columnName) {
        this.columnName = columnName;
        return this;
    }

    public Type getColumnType() {
        return columnType;
    }

    public KuduColumn setColumnType(Type columnType) {
        this.columnType = columnType;
        return this;
    }

    public Object getColumnValue() {
        return columnValue;
    }

    public KuduColumn setColumnValue(Object columnValue) {
        this.columnValue = columnValue;
        return this;
    }

    public boolean isUpdate() {
        return isUpdate;
    }

    public KuduColumn setUpdate(boolean update) {
        isUpdate = update;
        return this;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public KuduColumn setDefaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }


    public boolean isNullAble() {
        return isNullAble;
    }

    public KuduColumn setNullAble(boolean nullAble) {
        isNullAble = nullAble;
        return this;
    }

    public AlterColumnEnum getAlterColumnEnum() {
        return alterColumnEnum;
    }

    public KuduColumn setAlterColumnEnum(AlterColumnEnum alterColumnEnum) {
        this.alterColumnEnum = alterColumnEnum;
        return this;
    }

    public String getNewColumnName() {
        return newColumnName;
    }

    public KuduColumn setNewColumnName(String newColumnName) {
        this.newColumnName = newColumnName;
        return this;
    }

    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    public KuduColumn setPrimaryKey(boolean primaryKey) {
        isPrimaryKey = primaryKey;
        return this;
    }

    public KuduPredicate.ComparisonOp getComparisonOp() {
        return comparisonOp;
    }

    public KuduColumn setComparisonOp(KuduPredicate.ComparisonOp comparisonOp) {
        this.comparisonOp = comparisonOp;
        return this;
    }

    public Object getComparisonValue() {
        return comparisonValue;
    }

    public KuduColumn setComparisonValue(Object comparisonValue) {
        this.comparisonValue = comparisonValue;
        return this;
    }

    public boolean isSelect() {
        return isSelect;
    }

    public KuduColumn setSelect(boolean select) {
        isSelect = select;
        return this;
    }

    @Override
    public String toString() {
        return "KuduColumn{" +
                "\ncolumnName='" + columnName + '\'' +
                ",\n columnType=" + columnType +
                ",\n encoding=" + encoding +
                ",\n columnValue=" + columnValue +
                ",\n isUpdate=" + isUpdate +
                ",\n isPrimaryKey=" + isPrimaryKey +
                ",\n alterColumnEnum=" + alterColumnEnum +
                ",\n defaultValue=" + defaultValue +
                ",\n isNullAble=" + isNullAble +
                ",\n newColumnName='" + newColumnName + '\'' +
                ",\n comparisonOp=" + comparisonOp +
                ",\n comparisonValue=" + comparisonValue +
                ",\n isSelect=" + isSelect +
                '}';
    }

    public enum AlterColumnEnum {

        ADD_COLUMN("ADD_COLUMN", "添加列"),
        DROP_COLUMN("DROP_COLUMN", "删除列"),
        RENAME_COLUMN("RENAME_COLUMN", "重命名列"),
        NONE("NONE", "不操作");

        private String type;
        private String desc;

        AlterColumnEnum(String type, String desc) {
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
