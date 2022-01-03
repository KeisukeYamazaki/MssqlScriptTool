using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace MssqlScriptTool.DTO
{
    public class Scripts
    {
        private readonly string _databaseName;

        private const string UseScript = "USE [{0}]\n";

        public Scripts(string databaseName)
        {
            _databaseName　= databaseName;
            DropScripts = new List<string>();
            CreateScripts = new List<string>();
            InsertScripts = new List<string>();
        }

        public List<string> DropScripts { get; set; }
        public List<string> CreateScripts { get; set; }
        public List<string> InsertScripts { get; set; }

        public override string ToString()
        {
            var allList = new List<List<string>>
            {
                DropScripts, CreateScripts, InsertScripts
            };

            var builder = new StringBuilder();
            builder.Append(string.Format(UseScript, _databaseName));
            builder.Append("GO\n");

            foreach (var list in allList.Where(list => list.Count != 0))
            {
                // リストが空でないものを文字列にして追加する
                builder.Append(string.Join("\n", list));
            }

            return builder.ToString();
        }

        /// <summary>
        /// Drop, Create, Insert の各スクリプトを設定する
        /// </summary>
        /// <param name="options"></param>
        /// <param name="connection"></param>
        /// <param name="targetTableType"></param>
        /// <param name="allTableDataList"></param>
        public async Task SetScriptsAsync(Options options, SqlConnection connection, TargetTableType targetTableType,
            List<TableData> allTableDataList)
        {
            string[] targetTableArray = GetTargetTableArray(options, targetTableType, allTableDataList);
            if (options.SchemeData != SchemeData.DataOnly && options.DropCreate != DropCreate.CreateOnly)
            {
                // 各テーブルのDrop文を設定
                SetDropOrCreateScripts(allTableDataList, targetTableArray, SetDropScript);
                DropScripts.Add(""); // 改行用の空白文字を入れる
            }

            if (options.SchemeData != SchemeData.DataOnly && options.DropCreate != DropCreate.DropOnly)
            {
                // 各テーブルのCreate文を設定
                SetDropOrCreateScripts(allTableDataList, targetTableArray, SetCreateScript);
                CreateScripts.Add(""); // 改行用の空白文字を入れる
            }

            if (options.SchemeData != SchemeData.SchemeOnly)
            {
                // 各テーブルのInsert文を設定
                await SetInsertScriptsAsync(connection, allTableDataList, targetTableArray);
            }
        }

        /// <summary>
        /// スクリプト取得対象のテーブルを取得して返す
        /// </summary>
        /// <param name="options"></param>
        /// <param name="targetTableType"></param>
        /// <param name="allTableDataList"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        private static string[] GetTargetTableArray(Options options, TargetTableType targetTableType,
            IEnumerable<TableData> allTableDataList)
        {
            var allTableArray = allTableDataList.Select(t => t.TableName).ToArray();
            return targetTableType switch
            {
                TargetTableType.All     => allTableArray,
                TargetTableType.Exclude => allTableArray.Except(options.ExcludeObjects).ToArray(),
                TargetTableType.Include => options.IncludeObjects.ToArray(),
                _                       => throw new ArgumentOutOfRangeException()
            };
        }

        /// <summary>
        /// Drop, Create いずれかのスクリプトを設定する
        /// </summary>
        /// <param name="allTableDataList"></param>
        /// <param name="targetTableArray"></param>
        /// <param name="action"></param>
        private static void SetDropOrCreateScripts(IEnumerable<TableData> allTableDataList, IReadOnlyCollection<string> targetTableArray,
            Action<TableData> action)
        {
            // 万が一 targetTableArray が null なら コンソール出力して処理を終了する
            if (targetTableArray.Count == 0)
            {
                Console.WriteLine($"対象テーブルが存在しないため [{action.Method.Name}] を実行することができません。");
                return;
            }

            // 各テーブルのDrop or Create 文を設定
            allTableDataList.Where(t => targetTableArray.Contains(t.TableName)).ToList().ForEach(action);
        }

        /// <summary>
        /// Insert スクリプトを設定する
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="allTableDataList"></param>
        /// <param name="targetTableArray"></param>
        private async Task SetInsertScriptsAsync(SqlConnection connection, IEnumerable<TableData> allTableDataList,
            IReadOnlyCollection<string> targetTableArray)
        {
            // 万が一 targetTableArray が null なら コンソール出力して処理を終了する
            if (targetTableArray.Count == 0)
            {
                Console.WriteLine($"対象テーブルが存在しないため Insert文生成を実行することができません。");
                return;
            }

            // 対象テーブルの全データのDataTableを取得し、Insert文を生成する
            foreach (TableData tableData in allTableDataList.Where(t => targetTableArray.Contains(t.TableName)))
            {
                await SetInsertOneTableScriptsAsync(connection, tableData);
            }
        }

        /// <summary>
        /// Drop文を設定する
        /// </summary>
        /// <param name="tableData"></param>
        private void SetDropScript(TableData tableData)
        {
            DropScripts.Add(tableData.GetDropScript());
        }

        /// <summary>
        /// Create文を設定する
        /// </summary>
        /// <param name="tableData"></param>
        private void SetCreateScript(TableData tableData)
        {
            CreateScripts.Add(tableData.GetCreateScript());
        }

        /// <summary>
        /// Insert文を設定する
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="tableData"></param>
        /// <returns></returns>
        private async Task SetInsertOneTableScriptsAsync(SqlConnection connection, TableData tableData)
        {
            var sql = $"SELECT * FROM {tableData.TableSchemeAndName}";
            await using var reader = await DbOperations.GetSqlDataReaderAsync(connection, sql);
            InsertScripts.Add(tableData.GetInsertScripts(reader));
        }
    }
}
