﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Serenity.Data.Schema
{
    public class MySqlSchemaProvider : ISchemaProvider
    {
        public string DefaultSchema { get { return null; } }

        public IEnumerable<FieldInfo> GetFieldInfos(IDbConnection connection, string schema, string table)
        {
            return connection.Query(string.Format("SHOW FULL COLUMNS FROM `{0}`", table))
                .OrderBy(x => Convert.ToInt32(x.ORDINAL_POSITION))
                .Select(src =>
                {
                    var fi = new FieldInfo();
                    fi.FieldName = src.Field;
                    fi.IsNullable = ((string)src.Null) != "NO";
                    var dataType = (string)src.Type;
                    var dx = dataType.IndexOf('(');
                    if (dx >= 0)
                    {
                        var dxend = dataType.IndexOf(')', dx);
                        var strlen = dataType.Substring(dx + 1, dxend - dx - 1);
                        dataType = dataType.Substring(0, dx);
                        var lower = dataType.ToLowerInvariant();
                        if (lower == "char" || lower == "varchar")
                            fi.Size = int.Parse(strlen);
                        else if (lower == "real" || lower == "decimal")
                        {
                            var strparts = strlen.Split(',');
                            fi.Size = int.Parse(strparts[0]);
                            fi.Scale = int.Parse(strparts[1]);
                        }
                    }
                    fi.DataType = dataType;
                    fi.IsPrimaryKey = (string)src.Key == "PRI";
                    fi.IsIdentity = (string)src.Extra == "auto_increment";
                    return fi;
                });
        }

        public IEnumerable<ForeignKeyInfo> GetForeignKeys(IDbConnection connection, string schema, string table)
        {
            return connection.Query<ForeignKeyInfo>(@"
                SELECT 
                    i.CONSTRAINT_NAME FKName,
                    k.COLUMN_NAME FKColumn, 
                    CASE WHEN k.REFERENCED_TABLE_SCHEMA = Database() THEN NULL 
                        ELSE k.REFERENCED_TABLE_SCHEMA END PKSchema, 
                    k.REFERENCED_TABLE_NAME PKTable,
                    k.REFERENCED_COLUMN_NAME PKColumn
                FROM information_schema.TABLE_CONSTRAINTS i
                LEFT JOIN information_schema.KEY_COLUMN_USAGE k ON i.CONSTRAINT_NAME = k.CONSTRAINT_NAME
                WHERE i.CONSTRAINT_TYPE = 'FOREIGN KEY'
                AND i.TABLE_SCHEMA = Database()
                AND i.TABLE_NAME = @tbl", new
            {
                tbl = table
            });
        }

        public IEnumerable<string> GetIdentityFields(IDbConnection connection, string schema, string table)
        {
            return connection.Query<string>(@"
                    SELECT COLUMN_NAME FROM information_schema.COLUMNS 
                    WHERE TABLE_SCHEMA = Database()
                    AND table_name = @tbl
                    AND EXTRA = 'auto_increment'", 
                new
                {
                    tbl = table
                });
        }

        public IEnumerable<string> GetPrimaryKeyFields(IDbConnection connection, string schema, string table)
        {
            return connection.Query<string>(@"
                    SELECT COLUMN_NAME  
                    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc  
                    INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS ku  
                    USING(constraint_name,table_schema,table_name)
                    WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'  
                    AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME  
                    AND ku.TABLE_SCHEMA = Database()  
                    AND ku.TABLE_NAME = @tbl  
                    ORDER BY ku.ORDINAL_POSITION",
                new
                {
                    tbl = table
                });
        }

        public IEnumerable<TableName> GetTableNames(IDbConnection connection)
        {
            return connection.Query(
                    "SELECT TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES " +
                    "WHERE TABLE_SCHEMA = Database() " +
                    "ORDER BY TABLE_SCHEMA, TABLE_NAME")
                .Select(x => new TableName
                {
                    Table = x.TABLE_NAME,
                    IsView = x.TABLE_TYPE == "VIEW"
                });
        }

        public string SqlTypeNameToFieldType(string sqlTypeName, int size, out string dataType)
        {
            return SchemaHelper.SqlTypeNameToFieldType(sqlTypeName, size, out dataType);
        }
    }
}