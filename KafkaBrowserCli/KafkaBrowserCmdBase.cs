using Confluent.SchemaRegistry;
using McMaster.Extensions.CommandLineUtils;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace KafkaBrowserCli
{
  [HelpOption("--help")]
  public abstract class KafkaBrowserCmdBase
  {
    private KafkaBrowserClient _kafkaBrowserClient;
    private CachedSchemaRegistryClient _schemaRegistryClient;

    protected ILogger _logger;
    protected IConsole _console;

    [Option(CommandOptionType.SingleValue, ShortName = "bs", LongName = "bootstrap-server", Description = "Setting kafka bootstrap server (default: PLAINTEXT://127.0.0.1:9092)", ShowInHelpText = true)]
    public string BootstrapServer { get; set; } = "PLAINTEXT://127.0.0.1:9092";

    [Option(CommandOptionType.SingleValue, ShortName = "sr", LongName = "schema-registry", Description = "Sets the schema-registry url (default: http://127.0.0.1:8081)", ShowInHelpText = true)]
    public string SchemaRegistryUrl { get; set; } = "http://127.0.0.1:8081";

    [Option(CommandOptionType.SingleValue, ShortName = "zk", LongName = "zookeeper", Description = "Sets the zookeeper address (default: 127.0.0.1:2181)", ShowInHelpText = true)]
    public string Zookeeper { get; set; } = "127.0.0.1:2181";

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

    protected CachedSchemaRegistryClient SchemaRegistryClient
    {
      get
      {
        if(_schemaRegistryClient == null)
        {
          _schemaRegistryClient = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = SchemaRegistryUrl });
        }
        return _schemaRegistryClient;
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
      // TODO: implement continous write to file      
    }

    protected void OutputToConsole(string data)
    {
      _console.BackgroundColor = ConsoleColor.Black;
      _console.ForegroundColor = ConsoleColor.White;
      _console.Out.WriteLine(data);
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
