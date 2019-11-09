using McMaster.Extensions.CommandLineUtils;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace KafkaBrowserCli
{
  [Command(Name = "config", Description = "Set default configurations")]
  public class ConfigCmd : KafkaBrowserCmdBase
  {
    public ConfigCmd(ILogger<ConfigCmd> logger,  IConsole console)
    {
      _logger = logger;
      _console = console;
    }
    private KafkaBrowserCmd Parent { get; set; }

    protected override async Task<int> OnExecute(CommandLineApplication app)
    {
      if(string.IsNullOrEmpty(BootstrapServer) ||
         string.IsNullOrEmpty(SchemaRegistryUrl) ||
         string.IsNullOrEmpty(Zookeeper))
      {
        BootstrapServer = Prompt.GetString("Kafka Bootstrap Server:", BootstrapServer);
        SchemaRegistryUrl = Prompt.GetString("Schema Registry Url:", SchemaRegistryUrl);
        Zookeeper = Prompt.GetString("Zookeeper:", Zookeeper);
      }

      try
      {
        var userProfile = new UserProfile()
        {
          BootstrapServer = BootstrapServer,
          SchemaRegistryUrl = SchemaRegistryUrl,
          Zookeeper = Zookeeper
        };

        var profileFolder = "";
        if (!Directory.Exists(profileFolder))
        {
          Directory.CreateDirectory(profileFolder);
        }

        await File.WriteAllTextAsync($"{profileFolder}", JsonConvert.SerializeObject(userProfile, Formatting.Indented), UTF8Encoding.UTF8);

        return 0;
      }
      catch(Exception ex)
      {
        OnException(ex);
        return 1;
      }
    }
  }
}
