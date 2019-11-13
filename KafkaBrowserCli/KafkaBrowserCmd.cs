using McMaster.Extensions.CommandLineUtils;
using Microsoft.Extensions.Logging;
using System;
using System.Reflection;
using System.Threading.Tasks;

namespace KafkaBrowserCli
{
  [Command(Name = "kb", ThrowOnUnexpectedArgument = false, OptionsComparison = StringComparison.InvariantCultureIgnoreCase)]
  [VersionOptionFromMember("--version", MemberName = nameof(GetVersion))]
  [Subcommand(
    typeof(ConfigCmd),
    typeof(ReadTopicCmd),
    typeof(ReadSchemaCmd))]
  public class KafkaBrowserCmd : KafkaBrowserCmdBase
  { 
    public KafkaBrowserCmd(ILogger<KafkaBrowserCmd> logger, IConsole console)
    {
      _logger = logger;
      _console = console;
    }

    protected override Task<int> OnExecute(CommandLineApplication app)
    {
      app.ShowHelp();
      return Task.FromResult(0);
    }

    public static string GetVersion()
    {
      return typeof(KafkaBrowserCmd).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>().InformationalVersion;
    }
  }
}
