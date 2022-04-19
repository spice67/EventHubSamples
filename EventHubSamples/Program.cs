using Azure.Identity;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using System;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace EventHubSamples
{
    internal class Program
    {
        static string eventHubNamespace;
        static string eventHubName;
        static string ehConnectionStr;
        static string tenantId;
        static string clientId;
        static string clientSecret;
        static string proxyUrl;
        static bool useProxy = false;
        static int timeToInvokeTokenCancellation = 45;
        static bool hasTimedOut = false;
        static async Task Main(string[] args)
        {
            Console.WriteLine("Choose an action:");
            Console.WriteLine("[A] Read events from a partition via default azure credential.");
            Console.WriteLine("[B] Read events from a partition thru connection string.");
            Console.WriteLine("[C] Read events from a partition via client secret.");
            //Console.WriteLine("[D] Authenticate via certificate.");

            eventHubNamespace = ConfigurationManager.AppSettings["eventHubNamespaceFQDN"];
            eventHubName = ConfigurationManager.AppSettings["eventHubName"];
            ehConnectionStr = ConfigurationManager.AppSettings["eventHubConnStr"];
            tenantId = ConfigurationManager.AppSettings["tenantId"];
            clientId = ConfigurationManager.AppSettings["clientId"];
            clientSecret = ConfigurationManager.AppSettings["clientSecret"];
            proxyUrl = ConfigurationManager.AppSettings["proxyUrl"];
            useProxy = Convert.ToBoolean(ConfigurationManager.AppSettings["useProxy"]);

            timeToInvokeTokenCancellation = int.TryParse(ConfigurationManager.AppSettings["timeToInvokeTokenCancellation"], out timeToInvokeTokenCancellation) ? timeToInvokeTokenCancellation : 45;

            Char key = Console.ReadKey(true).KeyChar;
            String keyPressed = key.ToString().ToUpper();

            Console.WriteLine($"Pressed: {keyPressed} option. Pls wait while fetching data...");

            switch (keyPressed)
            {
                case "A":
                    await DoReadEventsFromPartitionsUsingAzDefCredential(GetEventHubConnectionOptions());
                    break;
                case "B":
                    await DoReadEventsFromPartitionsByConnectionString(GetEventHubConnectionOptions());
                    break;
                case "C":
                    await DoReadEventsFromPartitionsUsingClientSecret(GetEventHubConnectionOptions());
                    break;
                default:
                    break;
            }

            if (hasTimedOut)
            {
                Console.WriteLine("Timed out due to cancellation token at approx 45s.");
            }
        }

        private static EventHubConnectionOptions GetEventHubConnectionOptions()
        {
            return new EventHubConnectionOptions()
            {
                TransportType = EventHubsTransportType.AmqpWebSockets,
                Proxy = new WebProxy(proxyUrl, true)
            };
        }

        // using client secret
        private static async Task DoReadEventsFromPartitionsUsingClientSecret(EventHubConnectionOptions eventHubConnectionOptions)
        {
            var consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;

            var credential = new ClientSecretCredential(tenantId, clientId, clientSecret);

            if (useProxy)
            {
                EventHubConsumerClientOptions eventHubConsumerClientOptions = new()
                {
                    ConnectionOptions = eventHubConnectionOptions
                };
            }

            var consumer = new EventHubConsumerClient(consumerGroup, eventHubNamespace, eventHubName, credential);

            try
            {
                CancellationTokenSource cancellationSource = await FetchDataFromPartition(consumer);
            }
            catch (TaskCanceledException)
            {
                // This is expected if the cancellation token is
                // signaled.
                hasTimedOut = true;
            }
            finally
            {
                await consumer.CloseAsync();
            }
        }

        //using connection string and Shared Access Key
        private static async Task DoReadEventsFromPartitionsByConnectionString(EventHubConnectionOptions eventHubConnectionOptions)
        {
            var consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;

            var consumer = new EventHubConsumerClient(consumerGroup, ehConnectionStr);

            if (useProxy)
            {
                EventHubConsumerClientOptions eventHubConsumerClientOptions = new()
                {
                    ConnectionOptions = eventHubConnectionOptions
                };
            }

            try
            {
                CancellationTokenSource cts = await FetchDataFromPartition(consumer);
            }
            catch (TaskCanceledException)
            {
                hasTimedOut = true; 
            }
            finally
            {
                await consumer.CloseAsync();
            }
        }

        // Read events from a partition using Az Default Cred
        private static async Task DoReadEventsFromPartitionsUsingAzDefCredential(EventHubConnectionOptions eventHubConnectionOptions)
        {
            var consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;

            var credential = new DefaultAzureCredential(new DefaultAzureCredentialOptions
            {
                ExcludeAzureCliCredential = true,
                ExcludeAzurePowerShellCredential = true,
                ExcludeEnvironmentCredential = true,
                ExcludeInteractiveBrowserCredential = true,
                ExcludeManagedIdentityCredential = true,
                ExcludeSharedTokenCacheCredential = true,
                ExcludeVisualStudioCodeCredential = true,
                ExcludeVisualStudioCredential = false
            });

            if (useProxy)
            {
                EventHubConsumerClientOptions eventHubConsumerClientOptions = new()
                {
                    ConnectionOptions = eventHubConnectionOptions
                };
            }

            var consumer = new EventHubConsumerClient(consumerGroup, eventHubNamespace, eventHubName, credential);

            try
            {
                CancellationTokenSource cancellationSource = await FetchDataFromPartition(consumer);
            }
            catch (TaskCanceledException)
            {
                // This is expected if the cancellation token is
                // signaled.
                hasTimedOut = true;
            }
            finally
            {
                await consumer.CloseAsync();
            }
        }

        private static async Task<CancellationTokenSource> FetchDataFromPartition(EventHubConsumerClient consumer)
        {
            CancellationTokenSource cancellationSource = new CancellationTokenSource();
            cancellationSource.CancelAfter(TimeSpan.FromSeconds(timeToInvokeTokenCancellation));

            string firstPartition = (await consumer.GetPartitionIdsAsync(cancellationSource.Token)).First();
            EventPosition startingPosition = EventPosition.Earliest;

            var options = new ReadEventOptions
            {
                TrackLastEnqueuedEventProperties = true,
                OwnerLevel = 0
            };

            await foreach (PartitionEvent partitionEvent in consumer.ReadEventsFromPartitionAsync(
                firstPartition,
                startingPosition,
                options,
                cancellationSource.Token))
            {
                LastEnqueuedEventProperties properties =
                    partitionEvent.Partition.ReadLastEnqueuedEventProperties();

                Console.WriteLine($"Partition: { partitionEvent.Partition.PartitionId }");
                Console.WriteLine($"\tThe last sequence number is: { properties.SequenceNumber }");
                Console.WriteLine($"\tThe last offset is: { properties.Offset }");
                Console.WriteLine($"\tThe last enqueued time is: { properties.EnqueuedTime }, in UTC.");
                Console.WriteLine($"\tThe information was updated at: { properties.LastReceivedTime }, in UTC.");
            }

            return cancellationSource;
        }
    }
}
