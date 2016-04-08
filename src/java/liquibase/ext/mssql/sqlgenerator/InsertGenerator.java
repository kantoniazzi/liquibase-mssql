package liquibase.ext.mssql.sqlgenerator;

import java.util.*;

import liquibase.database.Database;
import liquibase.database.core.MSSQLDatabase;
import liquibase.datatype.DataTypeFactory;
import liquibase.exception.ValidationErrors;
import liquibase.ext.mssql.statement.InsertStatementMSSQL;
import liquibase.logging.LogFactory;
import liquibase.logging.Logger;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.InsertStatement;

public class InsertGenerator extends liquibase.sqlgenerator.core.InsertGenerator {

    private Logger log = LogFactory.getLogger();

    public static final String IF_TABLE_HAS_IDENTITY_STATEMENT = "IF ((select objectproperty(\n"
                    + "            object_id(N'${schemaName}.${tableName}'),\n"
                    + "           'TableHasIdentity')) = 1)\n" + "\t${then}\n";

    @Override
    public int getPriority() {
        return 15;
    }

    public boolean supports(InsertStatement statement, Database database) {
        return database instanceof MSSQLDatabase;
    }

    public ValidationErrors validate(InsertStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        return sqlGeneratorChain.validate(statement, database);
    }

    @Override
    public Sql[] generateSql(InsertStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        Boolean identityInsertEnabled = false;
        String pkColumns = null;

        if (statement instanceof InsertStatementMSSQL) {
            identityInsertEnabled = ((InsertStatementMSSQL)statement).getIdentityInsertEnabled();
            pkColumns = ((InsertStatementMSSQL)statement).getPkColumns();
        }
        if (identityInsertEnabled == null || !identityInsertEnabled) {
            return super.generateSql(statement, database, sqlGeneratorChain);
        }
        String tableName = database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName());
        String enableIdentityInsert = "SET IDENTITY_INSERT " + tableName + " ON";
        String disableIdentityInsert = "SET IDENTITY_INSERT " + tableName + " OFF";
        String safelyEnableIdentityInsert = ifTableHasIdentityColumn(enableIdentityInsert, statement, database.getDefaultSchemaName());
        String safelyDisableIdentityInsert = ifTableHasIdentityColumn(disableIdentityInsert, statement, database.getDefaultSchemaName());

        String checkByPkColumns = ifHasPkColumns(statement, database, pkColumns);

        List<Sql> sql = new ArrayList<Sql>();
        sql.add(new UnparsedSql(safelyEnableIdentityInsert));

        //Add If check before generateSql
        StringBuilder sb = new StringBuilder();

        if (checkByPkColumns.length() > 0){
            sb.append(checkByPkColumns);
            sb.append("\n");
        }

        List<Sql> sqlTmp = Arrays.asList(sqlGeneratorChain.generateSql(statement, database));

        for (Sql sql1 : sqlTmp) {
            sb.append(sql1.toSql());
            sb.append("\n");
        }

        sql.add(new UnparsedSql(sb.toString()));

        sql.add(new UnparsedSql(safelyDisableIdentityInsert));
        return sql.toArray(new Sql[sql.size()]);
    }

    private String ifHasPkColumns(InsertStatement statement, Database database, String pkColumnsString) {
        StringBuilder sb = new StringBuilder();

        List<String> pkColumns = new ArrayList<String>();
        if (pkColumnsString != null && pkColumnsString.length() > 0){
            pkColumns = Arrays.asList(pkColumnsString.split(","));
        }

        //generate where condition
        for (String pkColumn : pkColumns) {
            log.info("pk field: "+pkColumn);
            if (sb.length() > 0){
                sb.append(" and ");
            }
            sb.append(pkColumn);

            Object newValue = statement.getColumnValues().get(pkColumn);
            if (newValue == null || newValue.toString().equalsIgnoreCase("NULL")) {
                sb.append("is NULL");
            } else if (newValue instanceof String && !looksLikeFunctionCall(((String) newValue), database)) {
                sb.append(" = "+ DataTypeFactory.getInstance().fromObject(newValue, database).objectToSql(newValue, database));
            } else if (newValue instanceof Date) {
                sb.append(" = "+database.getDateLiteral(((Date) newValue)));
            } else if (newValue instanceof Boolean) {
                if (((Boolean) newValue)) {
                    sb.append(" is "+DataTypeFactory.getInstance().getTrueBooleanValue(database));
                } else {
                    sb.append(" is "+DataTypeFactory.getInstance().getFalseBooleanValue(database));
                }
            }else {
                sb.append(" = "+newValue);
            }

        }

        if (sb.length() > 0) {
            sb.insert(0,"IF NOT EXISTS (SELECT 1 FROM "+statement.getTableName()+" WHERE ");
            sb.append(")");
        }

        return sb.toString();
    }

    private String ifTableHasIdentityColumn(String then, InsertStatement statement, String defaultSchemaName) {
        String tableName = statement.getTableName();
        String schemaName = statement.getSchemaName();
        if (schemaName == null) {
            if (defaultSchemaName != null && !defaultSchemaName.isEmpty()) {
            schemaName = defaultSchemaName;
            } else {
            schemaName = "dbo";
            }
        }

        Map<String, String> tokens = new HashMap<String, String>();
        tokens.put("${tableName}", tableName);
        tokens.put("${schemaName}", schemaName);
        tokens.put("${then}", then);
        return performTokenReplacement(IF_TABLE_HAS_IDENTITY_STATEMENT, tokens);
    }

    private String performTokenReplacement(String input, Map<String, String> tokens) {
        String result = input;
        for (Map.Entry<String, String> entry : tokens.entrySet()) {
            result = result.replace(entry.getKey(), entry.getValue());
        }
        return result;
    }
}