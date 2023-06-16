﻿using System;

namespace Orleans.Streams.Kafka.Utils
{
	public class QueueProperties
	{
		public string QueueName { get; }
		public string Namespace { get; }
		public uint PartitionId { get; }
		public uint Hash { get; }
		public bool IsExternal { get; }
		public Type ExternalContractType { get; }

		public QueueProperties(
			string @namespace,
			uint partitionId = 0,
			bool isExternal = false,
			Type externalContractType = null
		)
		{
			Namespace = @namespace;
			PartitionId = partitionId;
			QueueName = $"{@namespace}_{partitionId.ToString()}";
			Hash = JenkinsHash.ComputeHash(@namespace);
			IsExternal = isExternal;
			ExternalContractType = externalContractType;
		}
	}
}