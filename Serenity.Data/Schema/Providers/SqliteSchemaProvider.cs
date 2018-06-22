﻿using Serenity.Data;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System;

namespace Serenity.Data.Schema
{
    public class SqliteSchemaProvider : ISchemaProvider
    {
        public string DefaultSchema { get { return null; } }

        public IEnumerable<FieldInfo> GetFieldInfos(IDbConnection connection, string schema, string table)
        {
            return connection.Query("PRAGMA table_info([" + table + "])")
                .Select(x => new FieldInfo
                {
                    FieldName = x.name,
                    DataType = x.type,
                    IsNullable = Convert.ToInt32(x.notnull) != 1,
                    IsPrimaryKey = Convert.ToInt32(x.pk) == 1
                });
        }

        public IEnumerable<ForeignKeyInfo> GetForeignKeys(IDbConnection connection, string schema, string table)
        {
            return connection.Query("PRAGMA foreign_key_list([" + table + "])")
                .Select(x => new ForeignKeyInfo
                {
                    FKName = x.id.ToString(),
                    FKColumn = x.from,
                    PKTable = x.table,
                    PKColumn = x.to
                });
        }

        public IEnumerable<string> GetIdentityFields(IDbConnection connection, string schema, string table)
        {
            var fields = connection.Query("PRAGMA table_info([" + table + "])")
                .Where(x => (int)x.pk > 0);

            if (fields.Count() == 1 &&
                (string)fields.First().type == "INTEGER")
            {
                return new List<string> { (string)fields.First().name };
            };

            return new List<string> { "ROWID" };
        }

        public IEnumerable<string> GetPrimaryKeyFields(IDbConnection connection, string schema, string table)
        {
            return connection.Query("PRAGMA table_info([" + table + "])")
                .Where(x => (int)x.pk > 0)
                .OrderBy(x => (int)x.pk)
                .Select(x => (string)x.name);
        }

        public IEnumerable<TableName> GetTableNames(IDbConnection connection)
        {
            return connection.Query(
                    "SELECT name, type FROM sqlite_master WHERE type='table' or type='view' " +
                    "ORDER BY name")
                .Select(x => new TableName
                {
                    Table = x.name,
                    IsView = x.type == "view"
                });
        }

        public string SqlTypeNameToFieldType(string sqlTypeName, int size, out string dataType)
        {
            return SchemaHelper.SqlTypeNameToFieldType(sqlTypeName, size, out dataType);
        }
    }
}