namespace MssqlScriptTool.Log;

public class Log
{
    /// <summary>
    /// ログ設定を読み込む
    /// </summary>
    public static void LogInitialize()
    {
        NLog.LogManager.Configuration = new NLog.Config.XmlLoggingConfiguration("Log/nlog.config");
    }
}