using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using KafkaBrowserCli.Extensions;
using System.Text;

namespace KafkaBrowserCli
{
  public static class SerializationHelper
  {
    private static CachedSchemaRegistryClient _cachedSchemaRegistryClient;
    private static AvroDeserializer<GenericRecord> _avroDeserializer;
    private static string _schemaRegistryUrl;

    public static SerializationFormat DetectSerializationFormat(byte[] data, string schemaRegistryUrl = null)
    {
      if(data == null || data.Length == 0)
      {
        return SerializationFormat.Unknown;
      }

      if(CheckJson(data))
      {
        return SerializationFormat.Json;
      }

      if(string.IsNullOrEmpty(schemaRegistryUrl) == false && CheckAvro(data))
      {
        _schemaRegistryUrl = schemaRegistryUrl;
        return SerializationFormat.Avro;
      }

      // TODO: check for more formats

      return SerializationFormat.Unknown;
    }

    public static string Deserialize(byte[] data, bool isKey, string topic, SerializationFormat serializationFormat)
    {
      if(data == null || data.Length == 0)
      {
        return string.Empty;
      }

      switch(serializationFormat)
      {
        case SerializationFormat.Json: return DeserializeJson(data);
        case SerializationFormat.Avro: return DeserializeAvro(data, topic, isKey);
        default:
          return "UNKNOWN_SERIALIZATION_FORMAT";
      }
    }

    private static bool CheckJson(byte[] data)
    {
      if (data[0] == (byte) '{')
      {
        return true;
      }
      return false;
    }

    private static string DeserializeJson(byte[] data)
    {
      return Newtonsoft.Json.Linq.JObject.Parse(Encoding.UTF8.GetString(data)).ToString();
    }

    private static bool CheckAvro(byte[] data)
    {
      if(data[0] == 0)
      {
        return true;
      }
      return false;
    }

    private static string DeserializeAvro(byte[] data, string topic, bool isKey)
    {
      if (_cachedSchemaRegistryClient == null)
      {
        _cachedSchemaRegistryClient = new CachedSchemaRegistryClient(new SchemaRegistryConfig { SchemaRegistryUrl = _schemaRegistryUrl });
      }
      if (_avroDeserializer == null)
      {
        _avroDeserializer = new AvroDeserializer<GenericRecord>(_cachedSchemaRegistryClient);
      }

      var deserializationTask = _avroDeserializer.DeserializeAsync(data, false, new SerializationContext(isKey ? MessageComponentType.Key : MessageComponentType.Value, topic));
      deserializationTask.Wait();

      return deserializationTask.Result.ToOutputString();
    }
  }
}
