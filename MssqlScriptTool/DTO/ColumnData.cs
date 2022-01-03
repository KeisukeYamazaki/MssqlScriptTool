using System.Data;
using System.Data.SqlClient;
using NLog.Targets;

namespace MssqlScriptTool.DTO;

public class ColumnData
{
    public const string nameTableName         = "TABLE_NAME";
    public const string nameTableScheme       = "TABLE_SCHEMA";
    public const string nameColumnName        = "COLUMN_NAME";
    public const string nameDataType          = "DATA_TYPE";
    public const string nameDigits            = "DIGITS";
    public const string nameIsNullable        = "IS_NULLABLE";
    public const string nameIdentitySet       = "IDENTITY_SET";
    public const string namePrimaryKeyOrdinal = "PRIMARY_KEY_ORDINAL";
    public const string nameIsUnique          = "IS_UNIQUE";
    public const string nameColumnDefault     = "COLUMN_DEFAULT";

    public ColumnData(IDataRecord reader)
    {
        TableScheme = (string) reader[nameTableScheme];
        TableName = (string) reader[nameTableName];
        TableSchemeAndName = $"[{TableScheme}].[{TableName}]";
        ColumnName = (string) reader[nameColumnName];
        DataType = GetDataType((string) reader[nameDataType]);
        Digits = int.TryParse((string) reader[nameDigits], out int digits) ? digits : null;
        IsNullable = (string) reader[nameIsNullable] == "YES";
        IdentitySet = reader[nameIdentitySet] is DBNull ? string.Empty : (string) reader[nameIdentitySet];
        PrimaryKeyOrdinal = reader[namePrimaryKeyOrdinal] is DBNull ? null : (byte) reader[namePrimaryKeyOrdinal];
        IsUnique = (bool) reader[nameIsUnique];
        ColumnDefault = reader[nameColumnDefault] is DBNull ? string.Empty : (string) reader[nameColumnDefault];
    }

    public string TableScheme { get; set; }
    public string TableName { get; set; }
    public string TableSchemeAndName { get; set; }
    public string ColumnName { get; set; }
    public SqlDbType DataType { get; set; }
    public int? Digits { get; set; }
    public bool IsNullable { get; set; }
    public string IdentitySet { get; set; }
    public byte? PrimaryKeyOrdinal { get; set; }
    public bool IsUnique { get; set; }
    public string ColumnDefault { get; set; }

    /// <summary>
    /// 文字列のSQL Server データ型を SqlServerDataType に変換して返す
    /// </summary>
    /// <param name="strDataType"></param>
    /// <returns></returns>
    public static SqlDbType GetDataType(string strDataType)
    {
        // { 文字列のデータ型(小文字): SqlServerDataType } の辞書を作成
        var allTypeDict = Enum.GetNames(typeof(SqlDbType)).ToDictionary(dbType => dbType.ToLower(),
            dbType => (SqlDbType) Enum.Parse(typeof(SqlDbType), dbType));

        return allTypeDict[strDataType];
    }

    /// <summary>
    /// Create文の1列分のSQL文を生成して返す
    /// </summary>
    /// <returns></returns>
    public string GetCreateColumnScript(bool hasPrimaryKeyAndUnique, bool isLast)
    {
        var name = $"[{ColumnName}]";
        var type = $"[{DataType.ToString().ToLower()}]";
        if (Digits is not null)
        {
            type += $"({Digits})";
        }
        var isNullable = IsNullable ? "NULL" : "NOT NULL";

        var script = !string.IsNullOrWhiteSpace(IdentitySet)
            ? $"\t{name} {type} {IdentitySet} {isNullable}"
            : $"\t{name} {type} {isNullable}";

        // 最後の行で、主キー・ユニーク制約のいずれもない場合はカンマをつけずに ")ON [PRIMARY]をつける" 
        return isLast && !hasPrimaryKeyAndUnique ? $"{script}\n) ON [PRIMARY]\n" : $"{script},\n";
    }

    protected bool Equals(ColumnData other)
    {
        return TableScheme == other.TableScheme && 
               TableName == other.TableName && 
               TableSchemeAndName == other.TableSchemeAndName && 
               ColumnName == other.ColumnName && 
               DataType == other.DataType && 
               Digits == other.Digits && 
               IsNullable == other.IsNullable && 
               IdentitySet == other.IdentitySet && 
               PrimaryKeyOrdinal == other.PrimaryKeyOrdinal && 
               IsUnique == other.IsUnique && 
               ColumnDefault == other.ColumnDefault;
    }

    public override bool Equals(object? obj)
    {
        if (ReferenceEquals(null, obj)) return false;
        if (ReferenceEquals(this, obj)) return true;
        if (obj.GetType() != this.GetType()) return false;
        return Equals((ColumnData) obj);
    }

    public override int GetHashCode()
    {
        var hashCode = new HashCode();
        hashCode.Add(TableScheme);
        hashCode.Add(TableName);
        hashCode.Add(TableSchemeAndName);
        hashCode.Add(ColumnName);
        hashCode.Add((int) DataType);
        hashCode.Add(Digits);
        hashCode.Add(IsNullable);
        hashCode.Add(IdentitySet);
        hashCode.Add(PrimaryKeyOrdinal);
        hashCode.Add(IsUnique);
        hashCode.Add(ColumnDefault);
        return hashCode.ToHashCode();
    }
}