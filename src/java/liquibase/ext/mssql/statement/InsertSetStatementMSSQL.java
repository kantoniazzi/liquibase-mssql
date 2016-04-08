package liquibase.ext.mssql.statement;

import liquibase.statement.core.InsertSetStatement;
import liquibase.statement.core.InsertStatement;

public class InsertSetStatementMSSQL extends InsertSetStatement {

    private Boolean identityInsertEnabled;

    private String pkColumns;

    public InsertSetStatementMSSQL(InsertSetStatement statement, Boolean identityInsertEnable, String pkColumns) {
        super(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName());
        for (InsertStatement insertStatement : statement.getStatements()) {
            addInsertStatement(insertStatement);
        }
        this.identityInsertEnabled = identityInsertEnable;
        this.pkColumns = pkColumns;
    }

    public InsertSetStatementMSSQL(InsertSetStatement statement, Boolean identityInsertEnable, String pkColumns, int batchSize) {
        super(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName(), batchSize);
        this.identityInsertEnabled = identityInsertEnable;
        this.pkColumns = pkColumns;
    }

    public Boolean getIdentityInsertEnabled() {
        return identityInsertEnabled;
    }

    public void setIdentityInsertEnabled(Boolean identityInsertEnabled) {
        this.identityInsertEnabled = identityInsertEnabled;
    }

    public String getPkColumns() {
        return pkColumns;
    }

    public void setPkColumns(String pkColumns) {
        this.pkColumns = pkColumns;
    }
}
