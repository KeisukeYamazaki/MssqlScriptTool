using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.CommandLineUtils;
using NLog;

namespace MssqlScriptTool.DTO;

public class Options
{
    public const string optionS                = "-S";
    public const string optionU                = "-U";
    public const string optionP                = "-P";
    public const string optiond                = "-d";
    public const string optionN                = "-N";
    public const string optionf                = "-f";
    public const string optionE                = "-E";
    public const string optionExcludeObjects   = "--exclude-objects";
    public const string optionIncludeObjects   = "--include-objects";
    public const string optionScriptDropCreate = "--script-drop-create";
    public const string optionScriptCreate     = "--script-create";
    public const string optionScriptDrop       = "--script-drop";
    public const string optionSchemeAndData    = "--schema-and-data";
    public const string optionSchemeOnly       = "--schema-only";
    public const string optionDataOnly         = "--data-only";

    public Options(List<CommandOption> options)
    {
        ServerName           = GetValue(options, optionS, true);
        User                 = GetValue(options, optionU, false);
        Password             = GetValue(options, optionP, false);
        DatabaseName         = GetValue(options, optiond, true);
        IsEncrypted          = GetBoolValue(options, optionN);
        OutputFilePath       = GetValue(options, optionf, false);
        UseTrustedConnection = GetBoolValue(options, optionE);
        if (!UseTrustedConnection && (User == string.Empty || Password == string.Empty))
        {
            // Windows認証でない(=SQLServer認証)で User,Passwordが指定されていない場合は例外を出す
            throw new ArgumentException(
                $"「{optionU}(ユーザー名)」・「{optionP}(パスワード)」を指定してください。Windows統合認証の場合は「{optionE}」を指定してください。");
        }


        ExcludeObjects       = GetValues(options, optionExcludeObjects, false);
        IncludeObjects       = GetValues(options, optionIncludeObjects, false);
        if (ExcludeObjects.Count > 0 && IncludeObjects.Count > 0)
        {
            // ExcludeObjects,IncludeObjects どちらも指定された場合は例外を出す
            throw new ArgumentException($"[{optionExcludeObjects}],[{optionIncludeObjects}] を同時に指定することはできません。");
        }

        DropCreate = GetDropCreateValue(options);
        SchemeData = GetSchemeDataValue(options);
    }

    public string ServerName { get; set; }
    public string User { get; set; }
    public string Password { get; set; }
    public string DatabaseName { get; set; }
    public bool IsEncrypted { get; set; }
    public string OutputFilePath { get; set; }
    public bool UseTrustedConnection { get; set; }
    public List<string> ExcludeObjects { get; set; }
    public List<string> IncludeObjects { get; set; }
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public DropCreate DropCreate { get; set; }
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public SchemeData SchemeData { get; set; }

    public static List<CommandOption> GetCommandOptions()
    {
        return new List<CommandOption>
        {
            new CommandOption(template: optionS, optionType: CommandOptionType.SingleValue)
            {
                Description = "接続サーバー名"
            },
            new CommandOption(template: optionU, optionType: CommandOptionType.SingleValue)
            {
                Description = "接続ユーザー名"
            },
            new CommandOption(template: optionP, optionType: CommandOptionType.SingleValue)
            {
                Description = "接続パスワード"
            },
            new CommandOption(template: optiond, optionType: CommandOptionType.SingleValue)
            {
                Description = "データベース名"
            },
            new CommandOption(template: optionN, optionType: CommandOptionType.NoValue)
            {
                Description = "暗号化を設定"
            },
            new CommandOption(template: optionf, optionType: CommandOptionType.SingleValue)
            {
                Description = "出力するファイルパス"
            },
            new CommandOption(template: optionE, optionType: CommandOptionType.NoValue)
            {
                Description = "Windows認証で接続"
            },
            new CommandOption(template: optionExcludeObjects, optionType: CommandOptionType.MultipleValue)
            {
                Description = "除外するオブジェクトを指定"
            },
            new CommandOption(template: optionIncludeObjects, optionType: CommandOptionType.MultipleValue)
            {
                Description = "包含するオブジェクトを指定"
            },
            new CommandOption(template: optionScriptDropCreate, optionType: CommandOptionType.NoValue)
            {
                Description = "drop文とcreate文を作成"
            },
            new CommandOption(template: optionScriptCreate, optionType: CommandOptionType.NoValue)
            {
                Description = "create文を作成"
            },
            new CommandOption(template: optionScriptDrop, optionType: CommandOptionType.NoValue)
            {
                Description = "drop文を作成"
            },
            new CommandOption(template: optionSchemeAndData, optionType: CommandOptionType.NoValue)
            {
                Description = "スキーマとデータを取得"
            },
            new CommandOption(template: optionSchemeOnly, optionType: CommandOptionType.NoValue)
            {
                Description = "データのみ取得"
            },
            new CommandOption(template: optionDataOnly, optionType: CommandOptionType.NoValue)
            {
                Description = "スキーマのみ取得"
            },
        };
    }

    public override string ToString()
    {
        return JsonSerializer.Serialize(this, new JsonSerializerOptions {WriteIndented = true});
    }

    /// <summary>
    /// 1つの項目を指定するパラメータの値を取得する
    /// </summary>
    /// <param name="options"></param>
    /// <param name="templateName"></param>
    /// <param name="required"></param>
    /// <returns></returns>
    /// <exception cref="ArgumentException"></exception>
    public static string GetValue(List<CommandOption> options, string templateName, bool required)
    {
        // パラメータ名が一致するパラメータの値を取得
        foreach (var option in options.Where(option =>
                     option.Template == templateName && option.OptionType == CommandOptionType.SingleValue && option.HasValue()))
        {
            return option.Value();
        }

        // 必須項目なら例外を出し、そうでなければ空の文字列を返す
        if (required)
        {
            throw new ArgumentException($"[{templateName}]が指定されていません。");
        }
        else
        {
            return string.Empty;
        }
    }

    /// <summary>
    /// 複数項目指定するパラメータの値を取得する
    /// </summary>
    /// <param name="options"></param>
    /// <param name="templateName"></param>
    /// <param name="required"></param>
    /// <returns></returns>
    /// <exception cref="ArgumentException"></exception>
    public static List<string> GetValues(List<CommandOption> options, string templateName, bool required)
    {
        // パラメータ名が一致するパラメータの値を取得
        foreach (var option in options.Where(option =>
                     option.Template == templateName && option.OptionType == CommandOptionType.MultipleValue && option.HasValue()))
        {
            return option.Values;
        }

        // 必須項目なら例外を出し、そうでなければ空の文字列を返す
        if (required)
        {
            throw new ArgumentException($"[{templateName}]が指定されていません。");
        }
        else
        {
            return new List<string>();
        }
    }

    /// <summary>
    /// Boolのパラメータ値を取得する
    /// </summary>
    /// <param name="options"></param>
    /// <param name="templateName"></param>
    /// <returns></returns>
    /// <exception cref="Exception"></exception>
    public static bool GetBoolValue(List<CommandOption> options, string templateName)
    {
        // パラメータ名が一致するパラメータの値を取得
        foreach (var option in options.Where(option =>
                     option.Template == templateName && option.OptionType == CommandOptionType.NoValue))
        {
            return option.HasValue();
        }

        throw new Exception($"内部エラー。[{templateName}]は bool値ではない または 存在しないパラメータです。");
    }

    /// <summary>
    /// DropCreate で指定された値を返す
    /// </summary>
    /// <param name="options"></param>
    /// <returns></returns>
    /// <exception cref="ArgumentException"></exception>
    /// <exception cref="ArgumentOutOfRangeException"></exception>
    public static DropCreate GetDropCreateValue(List<CommandOption> options)
    {
        var targets = new[] {optionScriptDropCreate, optionScriptDrop, optionScriptCreate};
        var resultDict = targets.ToDictionary(t => t, t => options.Any(o => o.Template == t && o.HasValue()));

        if (resultDict.Values.All(v => v == false))
        {
            // いずれも指定されていない場合は例外
            throw new ArgumentException($"パラメータ[{string.Join(",", targets)}]はいずれかを指定してください。");
        }
        else if (resultDict.Values.Count(v => v == true) >= 2)
        {
            // 2つ以上指定されていた場合は例外
            var nowParam = resultDict.Where(kv => kv.Value == true).Select(kv => kv.Key);
            throw new ArgumentException($"パラメータ[{string.Join(",", targets)}]はいずれか1つを指定してください。今回の指定[{string.Join(",", nowParam)}]");
        }

        var specified = resultDict.First(kv => kv.Value == true).Key;
        return specified switch
        {
            optionScriptDropCreate => DropCreate.DropAndCreate,
            optionScriptDrop => DropCreate.DropOnly,
            optionScriptCreate => DropCreate.CreateOnly,
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    /// <summary>
    /// DropCreate で指定された値を返す
    /// </summary>
    /// <param name="options"></param>
    /// <returns></returns>
    /// <exception cref="ArgumentException"></exception>
    /// <exception cref="ArgumentOutOfRangeException"></exception>
    public static SchemeData GetSchemeDataValue(List<CommandOption> options)
    {
        var targets = new[] { optionSchemeAndData, optionSchemeOnly, optionDataOnly };
        var resultDict = targets.ToDictionary(t => t, t => options.Any(o => o.Template == t && o.HasValue()));

        if (resultDict.Values.All(v => v == false))
        {
            // いずれも指定されていない場合は例外
            throw new ArgumentException($"パラメータ[{string.Join(",", targets)}]はいずれかを指定してください。");
        }
        else if (resultDict.Values.Count(v => v == true) >= 2)
        {
            // 2つ以上指定されていた場合は例外
            var nowParam = resultDict.Where(kv => kv.Value == true).Select(kv => kv.Key);
            throw new ArgumentException($"パラメータ[{string.Join(",", targets)}]はいずれか1つを指定してください。今回の指定[{string.Join(",", nowParam)}]");
        }

        var specified = resultDict.First(kv => kv.Value == true).Key;
        return specified switch
        {
            optionSchemeAndData => SchemeData.SchemeAndData,
            optionSchemeOnly => SchemeData.SchemeOnly,
            optionDataOnly => SchemeData.DataOnly,
            _ => throw new ArgumentOutOfRangeException()
        };
    }
}

public enum DropCreate
{
    DropAndCreate,
    DropOnly,
    CreateOnly
}

public enum SchemeData
{
    SchemeAndData,
    SchemeOnly,
    DataOnly,
}