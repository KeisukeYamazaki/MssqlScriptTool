using System.Data;
using System.Data.SqlClient;
using System.Text;
using MssqlScriptTool.DTO;
using NLog;

namespace MssqlScriptTool;

public class DbOperations
{
    private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

    public static async Task GetDdlAsync(Options　options)
    {
        var connectionString = GetConnectionString(options);
        var targetTableType = GeTargetTableType(options);

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        try
        {
            List<TableData> allTableDataList = await GetAllTableDataListAsync(connection);
            var scripts = new Scripts(options.DatabaseName);
            // Drop, Create, Insert 各スクリプト作成
            await scripts.SetScriptsAsync(options, connection, targetTableType, allTableDataList);
            
            // 出力
            if (options.OutputFilePath != string.Empty)
            {
                OutputFile(options.OutputFilePath, scripts.ToString());
            }
            else
            {
                Console.WriteLine(scripts.ToString());
            }
        }
        finally
        {
            await connection.CloseAsync();
        }
    }

    /// <summary>
    /// 接続文字列を取得する
    /// </summary>
    /// <param name="options"></param>
    /// <returns></returns>
    private static string GetConnectionString(Options options)
    {
        return new SqlConnectionStringBuilder
        {
            DataSource = options.ServerName,
            InitialCatalog = options.DatabaseName,
            UserID = options.User,
            Password = options.Password,
            ConnectTimeout = 5,
            Encrypt = options.IsEncrypted
        }.ToString();
    }

    /// <summary>
    /// 処理を行うテーブルの指定方法を返す
    /// </summary>
    /// <param name="options"></param>
    /// <returns></returns>
    /// <exception cref="ArgumentException"></exception>
    private static TargetTableType GeTargetTableType(Options options)
    {
        if (options.IncludeObjects.Count == 0 && options.ExcludeObjects.Count == 0)
        {
            // 包含・除外どちらも指定なし
            return TargetTableType.All;
        }
        else if(options.IncludeObjects.Count > 0 && options.ExcludeObjects.Count == 0)
        {
            // 包含は指定あり
            return TargetTableType.Include;
        }
        else if (options.IncludeObjects.Count == 0 && options.ExcludeObjects.Count > 0)
        {
            // 除外は指定あり
            return TargetTableType.Exclude;
        }
        else
        {
            throw new Exception($"[{nameof(TargetTableType)}]を設定することができません。 " +
                                $"[{nameof(options.IncludeObjects)}]の要素数：[{options.IncludeObjects.Count}] " +
                                $"[{nameof(options.ExcludeObjects)}]の要素数：[{options.ExcludeObjects.Count}]");
        }
    }

    /// <summary>
    /// SQLの結果を SqlDataReader で取得して返す
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="sql"></param>
    /// <returns></returns>
    public static async Task<SqlDataReader> GetSqlDataReaderAsync(SqlConnection connection, string sql)
    {
        // データを格納する DataTable を生成
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return await command.ExecuteReaderAsync();
    }

    /// <summary>
    /// DB内のデータベースの定義データを取得して返す
    /// </summary>
    /// <param name="connection"></param>
    /// <returns></returns>
    private static async Task<List<TableData>> GetAllTableDataListAsync(SqlConnection connection)
    {
        var sql =
        #region TableInfoSQL
            @"SELECT
                schemas.name AS TABLE_SCHEMA,
                tables.name AS TABLE_NAME,
                columns.COLUMN_ID,
                columns.name AS COLUMN_NAME,
                types.name AS DATA_TYPE,
                CASE
                    WHEN types.name in ('varchar', 'nvarchar', 'varbinary') and columns.max_length = -1 THEN
                        'MAX'
                    WHEN types.name in ('decimal', 'numeric') THEN
                        CONVERT(NVARCHAR(10), columns.precision) + ', ' + CONVERT(NVARCHAR(10), columns.scale)
                    WHEN types.name in ('binary', 'char', 'varbinary', 'varchar') THEN
                        CONVERT(NVARCHAR(10),columns.max_length)
                    WHEN types.name in ('nchar', 'nvarchar') THEN
                        CONVERT(NVARCHAR(10),(columns.max_length / 2))
                    WHEN types.name in ('datetime2', 'datetimeoffset', 'time') THEN
                        CONVERT(NVARCHAR(10),columns.scale)
                    ELSE
                        ''
                    END AS DIGITS,
                CASE 
                    WHEN columns.is_nullable = 1 THEN
                        'YES'
                    ELSE
                        'NO'
                    END AS IS_NULLABLE,
                'IDENTITY(' + CONVERT(NVARCHAR(10), identity_columns.seed_value) + ',' + CONVERT(NVARCHAR(10), identity_columns.increment_value) + ')' AS IDENTITY_SET,
                primary_keys.key_ordinal as PRIMARY_KEY_ORDINAL,
                CASE
                    WHEN unique_keys.key_ordinal is null THEN
                        CONVERT(bit, 'FALSE')
                    ELSE	
                        CONVERT(bit, 'TRUE')
                    END AS IS_UNIQUE,
                CASE 
                    WHEN left(default_constraints.definition, 2) = '((' AND RIGHT(default_constraints.definition, 2) = '))' THEN
                        SUBSTRING(default_constraints.definition, 3, LEN(default_constraints.definition) - 4)
                    WHEN left(default_constraints.definition, 1) = '(' AND RIGHT(default_constraints.definition, 1) = ')' THEN
                        SUBSTRING(default_constraints.definition, 2, LEN(default_constraints.definition) - 2)
                    ELSE
                        NULL
                    END AS COLUMN_DEFAULT
            FROM
                sys.tables
            INNER JOIN
                sys.schemas
            ON
                tables.schema_id = schemas.schema_id
            LEFT OUTER JOIN
                sys.extended_properties AS table_descriptions
            ON
                table_descriptions.class = 1
            AND
                tables.object_id = table_descriptions.major_id
            AND
                table_descriptions.minor_id = 0
            INNER JOIN
                sys.columns
            ON
                sys.tables.object_id = sys.columns.object_id
            INNER JOIN
                sys.types
            ON
                columns.user_type_id = types.user_type_id
            LEFT OUTER JOIN
                sys.identity_columns
            ON
                columns.object_id = identity_columns.object_id
            AND
                columns.column_id = identity_columns.column_id
            LEFT OUTER JOIN
                sys.default_constraints
            ON
                columns.default_object_id = default_constraints.object_id
            LEFT OUTER JOIN
                sys.extended_properties AS column_descriptions
            ON
                column_descriptions.class = 1
            AND
                columns.object_id = column_descriptions.major_id
            AND
                columns.column_id = column_descriptions.minor_id
            LEFT OUTER JOIN
                (
                SELECT
                    index_columns.object_id,
                    index_columns.column_id,
                    index_columns.key_ordinal
                FROM
                    sys.index_columns
                INNER JOIN
                    sys.key_constraints
                ON
                    key_constraints.type = 'PK'
                AND
                    index_columns.object_id = key_constraints.parent_object_id
                AND
                    index_columns.index_id = key_constraints.unique_index_id
                ) AS primary_keys
            ON
                columns.object_id = primary_keys.object_id
            AND
                columns.column_id = primary_keys.column_id
            LEFT OUTER JOIN
                (
                SELECT
                    index_columns.object_id,
                    index_columns.column_id,
                    index_columns.key_ordinal
                FROM
                    sys.index_columns
                INNER JOIN
                    sys.key_constraints
                ON
                    key_constraints.type = 'UQ'
                AND
                    index_columns.object_id = key_constraints.parent_object_id
                AND
                    index_columns.index_id = key_constraints.unique_index_id
                ) AS unique_keys
            ON
                columns.object_id = unique_keys.object_id
            AND
                columns.column_id = unique_keys.column_id";
        #endregion

        // var dt = await GetDataTableAsync(connection, sql);
        await using var reader = await GetSqlDataReaderAsync(connection, sql);
        return GetTableDataListFromSqlDataReader(reader);
    }

    /// <summary>
    /// テーブル情報を保持する SqlDataReader から辞書オブジェクトを生成して返す
    /// </summary>
    /// <param name="reader"></param>
    /// <returns></returns>
    private static List<TableData> GetTableDataListFromSqlDataReader(SqlDataReader reader)
    {
        var tableDataDict = new Dictionary<string, List<ColumnData>>();
        
        while (reader.Read())
        {
            var columnData = new ColumnData(reader);

            // テーブル名のキーがない場合はキーを作成し値を初期化
            if (!tableDataDict.ContainsKey(columnData.TableName))
            {
                tableDataDict[columnData.TableName] = new List<ColumnData>();
            }

            tableDataDict[columnData.TableName].Add(columnData);
        }

        return tableDataDict.Where(kv =>
            {
                // 列がないテーブルはこの時点で除外する
                if (kv.Value.Count == 0)
                {
                    Console.WriteLine($"[{kv.Key}] に定義されている列が存在しないため、処理対象外となります。");
                    return false;
                }
                else
                {
                    return true;
                }
            })
            .Select(kv => new TableData(kv.Key, kv.Value)).ToList();
    }

    /// <summary>
    /// 指定パスに出力内容をファイルとして保存する
    /// </summary>
    /// <param name="outputFilePath"></param>
    /// <param name="content"></param>
    private static void OutputFile(string outputFilePath, string content)
    {
        // ファイルが存在しない場合は作成する
        MakeOutputDirIfNotExist(outputFilePath);

        // ファイル作成
        using var fs = new FileStream(outputFilePath, FileMode.Create);
        fs.Write(Encoding.UTF8.GetBytes(content));
        fs.Close();

        Logger.Info($"[{outputFilePath}] を作成しました。");
    }

    /// <summary>
    /// ディレクトリが存在しない場合はディレクトリを作成する
    /// </summary>
    /// <param name="outputFilePath"></param>
    private static void MakeOutputDirIfNotExist(string outputFilePath)
    {
        var dirPath = Path.GetDirectoryName(outputFilePath);

        if (dirPath is null)
        {
            throw new Exception($"[{outputFilePath}] からディレクトリを取得できませんでした。");
        }

        var outputDir = new DirectoryInfo(dirPath);
        if (!outputDir.Exists)
        {
            outputDir.Create();
            Logger.Info($"[{dirPath}] を作成しました。");
        }
    }
}

public enum TargetTableType
{
    All,
    Include,
    Exclude,
}