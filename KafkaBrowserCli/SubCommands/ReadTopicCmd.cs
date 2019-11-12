using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using KafkaBrowserCli.Extensions;
using McMaster.Extensions.CommandLineUtils;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
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
      Topic = "telecom_italia_data";
      FromBeginning = true;

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

              if(keyFormat == SerializationFormat.NotSet || valueFormat == SerializationFormat.NotSet)
              {
                Task avroTask = null;
                if (valueFormat == SerializationFormat.NotSet)
                {
                  avroTask = DetectSerializationFormat(false, cr.Value);
                }

                Task jsonTask = null;
                if (keyFormat == SerializationFormat.NotSet)
                {
                  jsonTask = DetectSerializationFormat(true, cr.Key);
                }

                await Task.WhenAll(jsonTask, avroTask);
              }

              var keyStrOutput = Deserialize(true, cr.Key);
              var valStrOutput = Deserialize(false, cr.Value);

              //var keyStrOutput = ByteArrayToObject(cr.Key);
              //var valStrOutput = ByteArrayToObject(cr.Value);

              //var keyStrOutput = cr.Key.ToOutputString();
              //var valStrOutput = cr.Value.ToOutputString();

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

    private SerializationFormat keyFormat = SerializationFormat.NotSet;
    private SerializationFormat valueFormat = SerializationFormat.NotSet;

    private async Task DetectSerializationFormat(bool isKey, byte[] data)
    {
      var taskJson = data.TryDeserializeJson();
      var taskAvro = data.TryDeserializeAvro(isKey, SchemaRegistryUrl);
      await Task.WhenAll(taskJson, taskAvro);

      if (taskJson.Result)
      {
        if (isKey && keyFormat == SerializationFormat.NotSet)
        {
          keyFormat = SerializationFormat.Json;
        }
        else if (!isKey && valueFormat == SerializationFormat.NotSet)
        {
          valueFormat = SerializationFormat.Json;
        }
        return;
      }

      if (taskAvro.Result)
      {
        if (isKey && keyFormat == SerializationFormat.NotSet)
        {
          keyFormat = SerializationFormat.Avro;
        }
        else if (!isKey && valueFormat == SerializationFormat.NotSet)
        {
          valueFormat = SerializationFormat.Avro;
        }
        return;
      }

      if (isKey) keyFormat = SerializationFormat.Unknown;
      if (!isKey) valueFormat = SerializationFormat.Unknown;
    }

    private string Deserialize(bool isKey, byte[] data)
    {
      if(isKey)
      {
        switch(keyFormat)
        {
          case SerializationFormat.Avro: return data.DeserializeAvro(isKey, SchemaRegistryUrl);
          case SerializationFormat.Json: return data.DeserializeJson();
          case SerializationFormat.NotSet:
          case SerializationFormat.Unknown:
          default:
          {
            return "UNKNWON_KEY_SERIALIZATION_FORMAT";
          }
        }
      }
      else
      {
        switch(valueFormat)
        {
          case SerializationFormat.Avro: return data.DeserializeAvro(isKey, SchemaRegistryUrl);
          case SerializationFormat.Json: return data.DeserializeJson();
          case SerializationFormat.NotSet:
          case SerializationFormat.Unknown:
          default:
          {
            return "UNKNWON_VALUE_SERIALIZATION_FORMAT";
          }
        }
      }
    }

    private IConsumer<byte[], byte[]> GetSimpleConsumer(ConsumerConfig configuration)
    {
      return new ConsumerBuilder<byte[], byte[]>(configuration)
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

  public static class SerializerExtensions
  {
    public static Task<bool> TryDeserializeJson(this byte[] data)
    {
      try
      {
        DeserializeJson(data);
      }
      catch(Exception ex)
      {
        Serilog.Log.Logger.Error(ex, ex.Message);

        return Task.FromResult(false);
      }
      return Task.FromResult(true);
    }

    public static string DeserializeJson(this byte[] data)
    {
      return Newtonsoft.Json.Linq.JObject.Parse(System.Text.Encoding.UTF8.GetString(data)).ToString();
    }

    public static Task<bool> TryDeserializeAvro(this byte[] data, bool isKey, string schemaRegistryUrl)
    {
      try
      {
        DeserializeAvro(data, isKey, schemaRegistryUrl);
      }
      catch (Exception ex)
      {
        Serilog.Log.Logger.Error(ex, ex.Message);

        //AvroKeyDeserializer = null;
        //AvroValueDeserializer = null;
        //SchemaRegistry?.Dispose();
        return Task.FromResult(false);
      }
      return Task.FromResult(true);
    }

    private static CachedSchemaRegistryClient SchemaRegistry;
    private static AvroDeserializer<GenericRecord> AvroKeyDeserializer;
    private static AvroDeserializer<GenericRecord> AvroValueDeserializer;
    public static string DeserializeAvro(this byte[] data, bool isKey, string schemaRegistryUrl)
    {
      if(SchemaRegistry == null)
      {
        SchemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { SchemaRegistryUrl = schemaRegistryUrl });
      }
      if(isKey && AvroKeyDeserializer == null)
      {
        AvroKeyDeserializer = new AvroDeserializer<GenericRecord>(SchemaRegistry);
      }
      if(!isKey && AvroValueDeserializer == null)
      {
        AvroValueDeserializer = new AvroDeserializer<GenericRecord>(SchemaRegistry);
      }

      if (isKey)
      {
        return AvroKeyDeserializer.DeserializeAsync(data, false, SerializationContext.Empty).Result.ToOutputString();
      }
      else
      {
        return AvroValueDeserializer.DeserializeAsync(data, false, SerializationContext.Empty).Result.ToOutputString();
      }
    }
  }
}