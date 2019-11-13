using McMaster.Extensions.CommandLineUtils;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;

namespace KafkaBrowserCli
{
  [Command(Name = "read-schema", Description = "reads a schema from schema-registry")]
  public class ReadSchemaCmd : KafkaBrowserCmdBase
  {
    [Option(CommandOptionType.SingleValue, ShortName = "t", LongName = "topic", Description = "the schema of the topic to read", ValueName = "topic", ShowInHelpText = true)]
    public string Topic { get; set; }

    [Option(CommandOptionType.SingleValue, ShortName = "id", LongName = "schema-id", Description = "the schema id to read", ValueName = "schema-id", ShowInHelpText = true)]
    public int? SchemaId { get; set; }

    [Option(CommandOptionType.NoValue, ShortName = "ks", LongName = "key-schema", Description = "when topic is specified that get the key schema (default is value)", ValueName = "key-schema", ShowInHelpText = true)]
    public bool Key { get; set; }

    public ReadSchemaCmd(ILogger<ReadSchemaCmd> logger, IConsole console)
    {
      _logger = logger;
      _console = console;
    }

    protected override async Task<int> OnExecute(CommandLineApplication app)
    {
      //Topic = "cc_payments";
      //Key = false;

      if(SchemaId.HasValue)
      {
        var schema = await SchemaRegistryClient.GetSchemaAsync(SchemaId.Value);
        
        Output($"SCHM: {schema}");
        return 1;
      }
      else
      {
        if(string.IsNullOrEmpty(Topic))
        {
          return -1;
        }

        var schemaName = Key ? SchemaRegistryClient.ConstructKeySubjectName(Topic) : SchemaRegistryClient.ConstructValueSubjectName(Topic);
        var schema = await SchemaRegistryClient.GetLatestSchemaAsync(schemaName);

        var header = $"Subject: {schema.Subject} | ID: {schema.Id} | Version: {schema.Version}";

        Output($"INFO: {header}");
        Output($"SCHM: {schema.SchemaString}");
        return 1;
      }
    }
  }
}
