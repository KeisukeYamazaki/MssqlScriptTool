using Microsoft.Extensions.CommandLineUtils;
using MssqlScriptTool.DTO;
using NLog;

namespace MssqlScriptTool
{
    public class Program
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        static Task Main(string[] args)
        {
            Log.Log.LogInitialize();
            Logger.Info("処理を開始します。");

            var app = new CommandLineApplication(throwOnUnexpectedArg: false)
            {
                Name = nameof(MssqlScriptTool),
                Description = "ddlの作成"
            };

            app.HelpOption("-h|--help");

            app.Options.AddRange(Options.GetCommandOptions());

            app.OnExecute(async () =>
            {
                try
                {
                    var options = new Options(app.Options);
                    Logger.Debug($"\n{options.ToString()}\n");
                    await DbOperations.GetDdlAsync(options);
                }
                catch (Exception e)
                {
                    Logger.Error($"エラーが発生しました。処理を終了します。エラー内容[{e.ToString()}]");
                }
                
                // app.Options.ForEach(o => Console.WriteLine($"{o.Template}: {o.Value()}"));
                // Console.WriteLine(options.ToString());
                return 0;
            });

            app.Execute(args);
            return Task.CompletedTask;
        }
    }
}

