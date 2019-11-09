using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaBrowserCli
{
  public class UserProfile
  {
    public string BootstrapServer { get; set; }
    public string SchemaRegistryUrl { get; set; }
    public string Zookeeper { get; set; }
  }
}
