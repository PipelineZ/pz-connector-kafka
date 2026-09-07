using Microsoft.Extensions.Logging;
using Pz.Connector.Kafka;
using Pz.Connectors.Sdk;

return await PzConnectorHost.RunAsync(args, ctx => new KafkaConnector(ctx.LoggerFactory)).ConfigureAwait(false);
