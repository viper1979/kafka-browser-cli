using McMaster.Extensions.CommandLineUtils;
using Microsoft.Extensions.Logging;
using System;
using System.IO;
using System.Threading.Tasks;

namespace KafkaBrowserCli
{
  [HelpOption("--help")]
  public abstract class KafkaBrowserCmdBase
  {
    private KafkaBrowserClient _kafkaBrowserClient;

    protected ILogger _logger;
    protected IConsole _console;

    [Option(CommandOptionType.SingleValue, ShortName = "", LongName = "output-format", Description = "define the output format you want to receive", ShowInHelpText = true)]
    public string OutputFormat { get; set; } = "json";

    protected virtual Task<int> OnExecute(CommandLineApplication app)
    {
      return Task.FromResult(0);
    }

    protected KafkaBrowserClient KafkaBrowserClient
    {
      get
      {
        if ( _kafkaBrowserClient == null)
        {
          _kafkaBrowserClient = new KafkaBrowserClient();
        }
        return _kafkaBrowserClient;
      }
    }

    protected void OnException(Exception ex)
    {
      OutputError(ex.Message);
      _logger.LogError(ex.Message);
      _logger.LogDebug(ex, ex.Message);
    }

    protected void Output(string data)
    {
      OutputToConsole(data);
    }

    protected void OutputToFile(string data)
    {
      //File.WriteAllText(string.IsN)
    }

    protected void OutputToConsole(string data)
    {
      _console.BackgroundColor = ConsoleColor.Black;
      _console.ForegroundColor = ConsoleColor.White;
      _console.Out.Write(data);
      _console.ResetColor();
    }

    protected void OutputError(string message)
    {
      _console.BackgroundColor = ConsoleColor.Red;
      _console.ForegroundColor = ConsoleColor.White;
      _console.Error.WriteLine(message);
      _console.ResetColor();
    }
  }
}
