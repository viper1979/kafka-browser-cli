using Confluent.Kafka;
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

    private SerializationFormat keyFormat = SerializationFormat.NotSet;
    private SerializationFormat valueFormat = SerializationFormat.NotSet;

    public ReadTopicCmd(ILogger<ReadTopicCmd> logger, IConsole console)
    {
      _logger = logger;
      _console = console;
    }

    protected override async Task<int> OnExecute(CommandLineApplication app)
    {
      //Topic = "cc_data";
      //FromBeginning = true;

      if (string.IsNullOrEmpty(Topic))
      {
        Topic = Prompt.GetString("Topic Name:", Topic);
      }

      var configuration = new ConsumerConfig
      {
        BootstrapServers = BootstrapServer,
        GroupId = GroupId,
        AutoOffsetReset = FromBeginning ? AutoOffsetReset.Earliest : AutoOffsetReset.Latest,
        SessionTimeoutMs = 10000,
      };

      using (var consumer = GetSimpleConsumer(configuration))
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

              if (keyFormat == SerializationFormat.NotSet || valueFormat == SerializationFormat.NotSet)
              {
                if (keyFormat == SerializationFormat.NotSet) 
                {
                  keyFormat = SerializationHelper.DetectSerializationFormat(cr.Key, SchemaRegistryUrl);
                }
                if (valueFormat == SerializationFormat.NotSet)
                {
                  valueFormat = SerializationHelper.DetectSerializationFormat(cr.Value, SchemaRegistryUrl);
                }
              }

              var keyAsString = SerializationHelper.Deserialize(cr.Key, true, Topic, keyFormat);
              var valueAsString = SerializationHelper.Deserialize(cr.Value, false, Topic, valueFormat);

              var header = $"Topic: {cr.Topic} | Offset: {cr.Offset} | Partition: {cr.Partition.Value} | Timestamp: {cr.Timestamp.UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss")}";

              Output($"NFO: {header}");
              Output($"KEY: {keyAsString}");
              Output($"VAL: {valueAsString}");
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

    private IConsumer<byte[], byte[]> GetSimpleConsumer(ConsumerConfig configuration)
    {
      return new ConsumerBuilder<byte[], byte[]>(configuration)
          .SetErrorHandler((_, e) => OutputError($"{e.Reason}"))
          .Build();
    }
  }
}