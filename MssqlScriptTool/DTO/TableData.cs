using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace MssqlScriptTool.DTO;

public class TableData
{
    private const string DropTableScript =
        "/****** Object:  Table {0}    Script Date: {1} ******/\n" +
        "IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'{0}') AND type in (N'U'))\n" +
        "DROP TABLE {0}\n" +
        "GO";


    private const string CreateTableScript =
        "/****** Object:  Table {0}    Script Date: {1} ******/\n" +
        "SET ANSI_NULLS ON\n" +
        "GO\n" +
        "SET QUOTED_IDENTIFIER ON\n" +
        "GO\n" +
        "CREATE TABLE {0}(\n";

    private const string InsertScript = "INSERT {0} ({1}) VALUES ({2})";

    private const string IdentityInsert = "SET IDENTITY_INSERT {0} {1}";

    private const string ConstraintPrimary = "PRIMARY KEY CLUSTERED";

    private const string ConstraintUnique = "UNIQUE NONCLUSTERED";

    private readonly DateTime _creationTime;

    public TableData(string tableName, List<ColumnData> columns)
    {
        TableName = tableName;
        TableSchemeAndName = columns.First().TableSchemeAndName;
        Columns = columns;
        _creationTime = DateTime.Now;
    }

    public string TableName { get; set; }
    public string TableSchemeAndName { get; set; }
    public List<ColumnData> Columns { get; set; }

    /// <summary>
    /// Drop文を生成して返す
    /// </summary>
    /// <returns></returns>
    public string GetDropScript()
    {
        return string.Format(DropTableScript, TableSchemeAndName, $"{_creationTime:yyyy/MM/dd HH:mm:ss}");
    }

    /// <summary>
    /// Create文を生成して返す
    /// </summary>
    /// <returns></returns>
    public string GetCreateScript()
    {
        var builder = new StringBuilder();
        builder.Append(string.Format(CreateTableScript, TableSchemeAndName, $"{_creationTime:yyyy/MM/dd HH:mm:ss}"));
        
        var hasPrimaryKey = HasPrimaryKey();
        var hasUnique = HasUnique();
        
        // 各列の定義を追加
        Columns.ForEach(c => builder.Append(c.GetCreateColumnScript(hasPrimaryKey || hasUnique, Equals(c, Columns.Last()))));

        // 主キーがあれば主キー制約を追加
        if (HasPrimaryKey())
        {
            builder.Append(GetPrimaryKeyScript());
        }

        // ユニーク制約があれば追加
        if (HasUnique())
        {
            builder.Append(GetUniqueScript());
        }

        builder.Append("GO");

        return builder.ToString();
    }

    /// <summary>
    /// 主キーをもつ列が存在するかを判定する
    /// </summary>
    /// <returns></returns>
    private bool HasPrimaryKey()
    {
        return Columns.Any(c => c.PrimaryKeyOrdinal is not null);
    }

    /// <summary>
    /// 主キー制約スクリプトを生成して返す
    /// </summary>
    /// <returns></returns>
    private string GetPrimaryKeyScript()
    {
        var primaryKeyList = Columns
            .Where(c => c.PrimaryKeyOrdinal is not null)
            .OrderBy(c => c.PrimaryKeyOrdinal)
            .Select(c => c.ColumnName).ToList();

        return GetConstraintScript(primaryKeyList, ConstraintPrimary);
    }

    /// <summary>
    /// ユニーク制約されている列が存在するかを判定する
    /// </summary>
    /// <returns></returns>
    private bool HasUnique()
    {
        return Columns.Any(c => c.IsUnique);
    }

    /// <summary>
    /// ユニーク制約スクリプトを生成して返す
    /// </summary>
    /// <returns></returns>
    private string GetUniqueScript()
    {
        var uniqueList = Columns.Where(c => c.IsUnique).Select(c => c.ColumnName).ToList();
        
        return GetConstraintScript(uniqueList, ConstraintUnique);
    }

    /// <summary>
    /// 制約スクリプトを生成して返す
    /// </summary>
    /// <param name="constraintColumnList"></param>
    /// <param name="constraintType"></param>
    /// <returns></returns>
    private string GetConstraintScript(List<string> constraintColumnList, string constraintType)
    {
        var builder = new StringBuilder();
        var typeStr = constraintType == ConstraintPrimary ? "PK" : "UK";
        builder.Append($" CONSTRAINT [{typeStr}_{TableName}] {constraintType}\n");
        builder.Append("(\n");
        constraintColumnList.ForEach(c =>
        {
            builder.Append($"\t[{c}] ASC");  // とりあえず一旦昇順
            builder.Append(c != constraintColumnList.Last() ? ",\n" : "\n");  // 最後はカンマをつけない
        });
        builder.Append(
            ")WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]\n");
        builder.Append(") ON [PRIMARY]\n");

        return builder.ToString();
    }

    /// <summary>
    /// SqlDataReader から Insert文を生成する
    /// </summary>
    /// <param name="reader"></param>
    /// <returns></returns>
    public string GetInsertScripts(SqlDataReader reader)
    {
        string[] columnNameArray = Columns.Select(c => c.ColumnName).ToArray();
        // DataTableとcolumnNameArray の列順が一致するか確認。一致しなければ例外が出る
        CheckSameColumnOrder(reader, columnNameArray);

        Dictionary<string, Func<object?, string>> funcTypeFuncDict = GetTypeFuncDict();
        var insertScriptList = new List<string>();

        if (HasIdentitySet())
        {
            insertScriptList.Add(string.Format(IdentityInsert, TableSchemeAndName, "ON"));
            insertScriptList.Add("");
        }

        // 1行ずつInsert文を作成
        while (reader.Read())
        {
            var builder = new StringBuilder();
            // 各列のデータの処理
            for (var i = 0; i < reader.FieldCount; i++)
            {
                var column = Columns[i];
                if (column.DataType == SqlDbType.DateTime2)
                {
                    // DateTime2 の場合は ミリ秒のフォーマットを digits によって変える(TODO: DateTime2 以外の対応)
                    var digits = column.Digits ?? 7;  // Digits が null なら、デフォルトの 7 とする。
                    var format = $"yyyy-MM-ddTHH:mm:ss.{string.Join("", Enumerable.Range(0, digits).Select(_ => "f"))}";
                    var datetimeStr = reader.IsDBNull(i) ? null : reader.GetDateTime(i).ToString(format);
                    builder.Append(funcTypeFuncDict[column.DataType.ToString().ToLower()](datetimeStr));
                }
                else
                {
                    builder.Append(funcTypeFuncDict[column.DataType.ToString().ToLower()](reader[i]));
                }
                
                // 値の最後でなければ , をつける
                if (i != reader.FieldCount - 1)
                {
                    builder.Append(", ");
                }
            }

            // 1行分のInsert文を作成しリストに追加
            insertScriptList.Add(string.Format(InsertScript, TableSchemeAndName,
                string.Join(", ", columnNameArray.Select(name => $"[{name}]")), builder.ToString()));
        }

        if (HasIdentitySet())
        {
            insertScriptList.Add(string.Format(IdentityInsert, TableSchemeAndName, "OFF"));
        }

        insertScriptList.Add("GO");

        return string.Join("\n", insertScriptList);
    }

    /// <summary>
    /// IdentitySet が設定されている列があるかを判定する
    /// </summary>
    /// <returns></returns>
    private bool HasIdentitySet()
    {
        return Columns.Any(c => c.IdentitySet != string.Empty);
    }

    /// <summary>
    /// SqlDataReader と columnNameArray の列順が一致するか確認する
    /// </summary>
    /// <param name="reader"></param>
    /// <param name="columnNameArray"></param>
    /// <exception cref="Exception"></exception>
    private static void CheckSameColumnOrder(IDataReader reader, string[] columnNameArray)
    {
        var schemaTable = reader.GetSchemaTable();
        if (schemaTable == null)
        {
            throw new Exception($"SqlDataReader オブジェクトから列情報を取得することができません。");
        }

        // 列の順番が一致するか確認
        foreach (var (schemeRow, columnName) in schemaTable.Rows.Cast<DataRow>().Zip(columnNameArray))
        {
            if (schemeRow["ColumnName"].ToString() != columnName)
            {
                throw new Exception(
                    $"Insert文を作成するオブジェクトとSqlDataReaderの列順が一致しません。オブジェクトの列準 [{string.Join(",", columnNameArray)}] " +
                    $"SqlDataReaderの列順[{string.Join(",", schemaTable.Rows.Cast<DataRow>().Select(row => row["ColumnName"]))}]");
            }
        }
    }

    /// <summary>
    /// SQL Server のデータ型と データの値に対して行う処理の辞書を生成して返す
    /// </summary>
    /// <returns></returns>
    private Dictionary<string, Func<object?, string>> GetTypeFuncDict()
    {
        // Insert文を作成するときにクォートで囲む必要があるデータ型
        var needQuoteArray = new[]
        {
            SqlDbType.Date.ToString().ToLower(),
            SqlDbType.Time.ToString().ToLower(),
            SqlDbType.DateTimeOffset.ToString().ToLower(),
            SqlDbType.DateTime.ToString().ToLower(),
            SqlDbType.DateTime2.ToString().ToLower(),
            SqlDbType.SmallDateTime.ToString().ToLower(),
            SqlDbType.Char.ToString().ToLower(),
            SqlDbType.VarChar.ToString().ToLower(),
            SqlDbType.Text.ToString().ToLower(),
            SqlDbType.NChar.ToString().ToLower(),
            SqlDbType.NVarChar.ToString().ToLower(),
            SqlDbType.NText.ToString().ToLower(),
        };

        // シングルクォートで文字列を囲む処理
        Func<object?, string> getQuotedString = obj => obj is DBNull ? "NULL" : $"N'{obj}'";
        // シングルクォートで文字列を囲まずそのまま返す処理
        Func<object?, string> getNotQuotedString = obj => obj is DBNull ? "NULL" : $"{obj}";
        // DateTime2 の場合
        Func<object?, string> getDateTime2String = obj => obj is DBNull ? "NULL" : $"CAST(N'{obj}' AS DateTime2)";
        // BIT の場合
        Func<object?, string> getBitString = obj => obj is DBNull ? "NULL" : obj?.ToString() == "True" ? "1" : "0";
        // SqlServerDataType によってシングルクォートで囲むかどうかを決める
        var typeFuncDict = new Dictionary<string, Func<object?, string>>();
        foreach (var dataType in Columns.Select(column => column.DataType.ToString().ToLower())
                     .Where(dataType => !typeFuncDict.ContainsKey(dataType)))
        {
            if (dataType == SqlDbType.DateTime2.ToString().ToLower())
            {
                typeFuncDict[dataType] = getDateTime2String;
            }
            else if (dataType == SqlDbType.Bit.ToString().ToLower())
            {
                typeFuncDict[dataType] = getBitString;
            }
            else if (needQuoteArray.Contains(dataType))
            {
                typeFuncDict[dataType] = getQuotedString;
            }
            else
            {
                typeFuncDict[dataType] = getNotQuotedString;
            }
        }

        return typeFuncDict;
    }
}