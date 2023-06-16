using Orleans.Serialization;
using Orleans.Streams.Kafka.Utils;

namespace Orleans.Streams.Kafka.Serialization
{
	public struct SerializationContext
	{
		public SerializationManager SerializationManager { get; set; }
		public IExternalStreamDeserializer ExternalStreamDeserializer { get; set; }
	}
}
