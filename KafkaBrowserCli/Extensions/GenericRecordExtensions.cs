using Avro.Generic;

namespace KafkaBrowserCli.Extensions
{
  public static class GenericRecordExtensions
  {
    public static string ToOutputString(this GenericRecord record)
    {
      if (record == null)
      {
        return null;
      }

      System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
      strBuilder.Append("{ ");
      for (int i = 0; i < record.Schema.Fields.Count; i++)
      {
        var field = record.Schema.Fields[i];
        if (record.TryGetValue(field.Name, out var value))
        {
          var strValue = field.Schema.Name == "string" ? "\"" + value + "\"" : value;
          strBuilder.Append($"\"{field.Name}\": {strValue}");
        }
        else
        {
          strBuilder.Append($"\"{field}\": {field.DefaultValue.ToString()}");
        }

        if (i < record.Schema.Fields.Count - 1)
        {
          strBuilder.Append(", ");
        }
      }
      strBuilder.Append(" }");

      return strBuilder.ToString();
    }
  }
}
