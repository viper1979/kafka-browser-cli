using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using KafkaBrowserCli.Extensions;
using McMaster.Extensions.CommandLineUtils;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaBrowserCli
{
  [Command(Name = "read", Description = "reads a kafka topic")]
  public class ReadTopicCmd : KafkaBrowserCmdBase
  {
    [Option(CommandOptionType.SingleValue, ShortName = "t", LongName = "topic", Description = "the topic to read", ValueName = "topic", ShowInHelpText = true)]
    public string Topic { get; set; }

    [Option(CommandOptionType.SingleValue, ShortName = "g", LongName = "group-id", Description = "All clients sharing the same group-id are in the same group.", ValueName = "group id", ShowInHelpText = true)]
    public string GroupId { get; set; } = "kafka-browser-cli";

    [Option(CommandOptionType.SingleValue, ShortName = "b", LongName = "from-beginning", Description = "offset to start reading from", ValueName = "offset", ShowInHelpText = true)]
    public bool FromBeginning { get; set; } = false;

    [Option(CommandOptionType.SingleValue, ShortName = "k", LongName = "key-serializer", Description = "The deserializer for the key", ValueName = "key-serializer", ShowInHelpText = true)]
    public string KeySerializer { get; set; } = "avro";

    [Option(CommandOptionType.SingleValue, ShortName = "v", LongName = "value-serializer", Description = "The deserializer for the value", ValueName = "value-serializer", ShowInHelpText = true)]
    public string ValueSerializer { get; set; } = "avro";

    public ReadTopicCmd(ILogger<ReadTopicCmd> logger, IConsole console)
    {
      _logger = logger;
      _console = console;
    }

    protected override async Task<int> OnExecute(CommandLineApplication app)
    {
      if (string.IsNullOrEmpty(Topic))
      {
        Topic = Prompt.GetString("Topic Name:", Topic);
      }

      var configuration = new ConsumerConfig
      {
        BootstrapServers = BootstrapServer,
        GroupId = GroupId,
        AutoOffsetReset = FromBeginning ? AutoOffsetReset.Earliest : AutoOffsetReset.Latest,
      };

      using (var consumer = GetAvroConsumer(configuration))
      {
        consumer.Subscribe(Topic);

        CancellationTokenSource cts = new CancellationTokenSource();
        _console.CancelKeyPress += (_, e) =>
        {
          e.Cancel = true;
          cts.Cancel();
        };

        try
        {
          while (true)
          {
            try
            {
              var cr = consumer.Consume(cts.Token);
              var keyStrOutput = cr.Key.ToOutputString();
              var valStrOutput = cr.Value.ToOutputString();

              var header = $"Topic: {cr.Topic} | Offset: {cr.Offset} | Partition: {cr.Partition.Value} | Timestamp: {cr.Timestamp.UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss")}";

              Output($"NFO: {header}");
              Output($"KEY: {keyStrOutput}");
              Output($"VAL: {valStrOutput}");
              Output("");
            }
            catch (ConsumeException ex)
            {
              OutputError(ex.Message);
              _logger.LogDebug(ex, ex.Message);
              return 0;
            }
          }
        }
        catch (OperationCanceledException)
        {
          // Ensure the consumer leaves the group cleanly and final offsets are committed.
          consumer.Close();
        }
      }

      return 1;
    }

    private IConsumer<string, string> GetSimpleConsumer(ConsumerConfig configuration)
    {
      return new ConsumerBuilder<string, string>(configuration)
          .SetErrorHandler((_, e) => OutputError($"{e.Reason}"))
          .Build();
    }

    private IConsumer<GenericRecord, GenericRecord> GetAvroConsumer(ConsumerConfig configuration)
    {
      var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { SchemaRegistryUrl = SchemaRegistryUrl });
      return new ConsumerBuilder<GenericRecord, GenericRecord>(configuration)
        .SetKeyDeserializer(new AvroDeserializer<GenericRecord>(schemaRegistry).AsSyncOverAsync())
        .SetValueDeserializer(new AvroDeserializer<GenericRecord>(schemaRegistry).AsSyncOverAsync())
        .SetErrorHandler((_, e) => OutputError($"{e.Reason}"))
        .Build();
    }
  }
}