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
        AutoOffsetReset = FromBeginning ? AutoOffsetReset.Earliest : AutoOffsetReset.Latest
      };

      using (var consumer = new ConsumerBuilder<Ignore, string>(configuration).Build())
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
              Output($"{cr.Value} at {cr.TopicPartitionOffset}");
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
  }
}