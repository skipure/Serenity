using System.Collections.Generic;
using System.Data;
using System.Linq;
using System;

namespace Serenity.Data.Schema
{
    public class OracleSchemaProvider : ISchemaProvider
    {
        public string DefaultSchema { get { return null; } }

        public IEnumerable<FieldInfo> GetFieldInfos(IDbConnection connection, string schema, string table)
        {
            return connection.Query<FieldInfo>(@"
                SELECT 
                    c.column_name ""FieldName"",
                    c.data_type ""DataType"",
                    COALESCE(NULLIF(c.data_precision, 0), c.char_length) ""Size"",
                    c.data_scale ""Scale"",
                    CASE WHEN c.nullable = 'N' THEN 1 ELSE 0 END ""IsNullable""
                FROM all_tab_columns c
                WHERE 
                    c.owner = :sma
                    AND c.table_name = :tbl
                ORDER BY c.column_id
            ", new
            {
                sma = schema,
                tbl = table
            });
        }

        public IEnumerable<ForeignKeyInfo> GetForeignKeys(IDbConnection connection, string schema, string table)
        {
            return connection.Query<ForeignKeyInfo>(@"
                SELECT 
                    a.constraint_name FKName,                     
                    a.column_name FKColumn,
                    c.r_owner PKSchema,
                    c_pk.table_name PKTable,
                    uc.column_name PKColumn
                FROM all_cons_columns a
                JOIN all_constraints c ON a.owner = c.owner AND a.constraint_name = c.constraint_name
                JOIN all_constraints c_pk ON c.r_owner = c_pk.owner AND c.r_constraint_name = c_pk.constraint_name
                JOIN user_cons_columns uc ON uc.constraint_name = c.r_constraint_name
                WHERE c.constraint_type = 'R' 
                    AND a.table_name = :tbl
                    AND c.r_owner = :sma", new
            {
                sma = schema,
                tbl = table
            });
        }

        public IEnumerable<string> GetIdentityFields(IDbConnection connection, string schema, string table)
        {
            return new List<string>();
        }

        public IEnumerable<string> GetPrimaryKeyFields(IDbConnection connection, string schema, string table)
        {
            return connection.Query<string>(@"
                    SELECT cols.column_name
                    FROM all_constraints cons, all_cons_columns cols
                    WHERE cols.table_name = :tbl
                    AND cons.constraint_type = 'P'
                    AND cons.constraint_name = cols.constraint_name
                    AND cons.owner = :sch
                    ORDER BY cols.position",
                new
                {
                    sch = schema,
                    tbl = table
                });
        }

        public IEnumerable<TableName> GetTableNames(IDbConnection connection)
        {
            return connection.Query("SELECT username, table_name FROM user_tables, user_users ORDER BY table_name")
                .Select(x => new TableName
                {
                    Schema = x.USERNAME,
                    Table = x.TABLE_NAME
                });
        }

        private static Dictionary<string, string> SqlTypeToFieldTypeMap =
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                { "bigint", "Int64" },
                { "bit", "Boolean" },
                { "blob sub_type 1", "String" },
                { "char", "String" },
                { "character varying", "String" },
                { "character", "String" },
                { "date", "DateTime" },
                { "datetime", "DateTime" },
                { "datetime2", "DateTime" },
                { "datetimeoffset", "DateTimeOffset" },
                { "decimal", "Decimal" },
                { "double", "Double" },
                { "doubleprecision", "Double" },
                { "float", "Double" },
                { "guid", "Guid" },
                { "int", "Int32" },
                { "int4", "Int32" },
                { "int8", "Int64" },
                { "integer", "Int32" },
                { "money", "Decimal" },
                { "nchar", "String" },
                { "ntext", "String" },
                { "numeric", "Decimal" },
                { "number", "Decimal" },
                { "nvarchar", "String" },
                { "real", "Single" },
                { "rowversion", "ByteArray" },
                { "smalldatetime", "DateTime" },
                { "smallint", "Int16" },
                { "text", "String" },
                { "time", "TimeSpan" },
                { "timestamp", "DateTime" },
                { "timestamp without time zone", "DateTime" },
                { "timestamp with time zone", "DateTimeOffset" },
                { "tinyint", "Int16" },
                { "uniqueidentifier", "Guid" },
                { "varbinary", "Stream" },
                { "varchar", "String" },
                { "varchar2", "String" }
            };

        public string SqlTypeNameToFieldType(string sqlTypeName, int size, out string dataType)
        {
            dataType = null;
            string fieldType;
            sqlTypeName = sqlTypeName.ToLowerInvariant();

            if (sqlTypeName == "blob")
            {
                if (size == 0 || size > 256)
                    return "Stream";

                dataType = "byte[]";
                return "ByteArray";
            }
            else if (sqlTypeName == "timestamp" || sqlTypeName == "rowversion")
            {
                dataType = "byte[]";
                return "ByteArray";
            }
            else if (SqlTypeToFieldTypeMap.TryGetValue(sqlTypeName, out fieldType))
                return fieldType;
            else
                return SchemaHelper.SqlTypeNameToFieldType(sqlTypeName, size, out dataType);
        }
    }
}