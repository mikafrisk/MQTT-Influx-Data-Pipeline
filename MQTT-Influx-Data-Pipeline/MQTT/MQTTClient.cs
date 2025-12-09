using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Writes;
using MQTTnet;
using System.Buffers;
using System.Text;

namespace MQTT_Influx_Data_Pipeline.MQTT;

public class MQTTClient : IMQTTClient
{
    private readonly IMqttClient _mqttClient;
    private readonly MqttClientOptions _mqttOptions;
    private readonly MqttClientFactory _factory;
    private readonly IInfluxDBClient _influxDBClient;
    private readonly IWriteApiAsync _writeApi;

    public string? DefaultPublishTopic  { get; set; }

    public MQTTClient(InfluxDBClient influxDBClient, string brokerTcp, int brokerPort)
    {
        _factory = new MqttClientFactory();
        _mqttClient = _factory.CreateMqttClient();

        _mqttOptions = new MqttClientOptionsBuilder()
            .WithTcpServer(brokerTcp, brokerPort)
            .WithClientId(string.Concat("dotnet-mqtt-client-", Guid.NewGuid().ToString("N").AsSpan(0, 8)))
            .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V311)
            .Build();

        _influxDBClient = influxDBClient;
        _writeApi = _influxDBClient.GetWriteApiAsync();
    }

    public async Task ConnectAsync()
    {
        await _mqttClient.ConnectAsync(_mqttOptions, CancellationToken.None);
    }

    public async Task DisconnectAsync()
    {
        await _mqttClient.DisconnectAsync();
    }

    public async Task SubscribeAsync(string topic)
    {
        var mqttSubscribeOptions = _factory.CreateSubscribeOptionsBuilder()
        .WithTopicFilter(topic) // Subscribe to "test/topic"
        .Build();

        await _mqttClient.SubscribeAsync(mqttSubscribeOptions, CancellationToken.None);
    }

    public void SetApplicationMessageReceivedHandlerToStoreIntoInflux(string influxBucket, string influxOrg)
    {
        _mqttClient.ApplicationMessageReceivedAsync += async e =>
        {
            var topic = e.ApplicationMessage.Topic;
            var payloadSequence = e.ApplicationMessage.Payload;
            string payload = payloadSequence.IsEmpty
                ? string.Empty
                : Encoding.UTF8.GetString(payloadSequence.ToArray());

            Console.WriteLine($"Received on '{topic}': {payload}");

            // Try to parse the payload as a double
            if (double.TryParse(payload, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var value))
            {
                var point = PointData
                    .Measurement("mqtt_data")
                    .Tag("topic", topic)
                    .Field("value", value)
                    .Timestamp(DateTime.UtcNow, WritePrecision.Ns);

                // Write to InfluxDB
                await _writeApi.WritePointAsync(point, influxBucket, influxOrg);
            }
            else
            {
                Console.WriteLine("Payload is not a valid number, not writing to InfluxDB.");
            }
        };
    }

    public async Task PublishMessage(string? topic, string? payload)
    {
        topic ??= DefaultPublishTopic;

        if (topic == null)
        {
            Console.WriteLine("No topic specified for publishing message.");
            return;
        }

        var message = new MqttApplicationMessageBuilder()
                    .WithTopic(topic)
                    .WithPayload(payload)
                    .Build();

        if (_mqttClient.IsConnected)
        {
            await _mqttClient.PublishAsync(message);
            Console.WriteLine($"Published: {payload}");
        }
    }

    public void Dispose()
    {
        _mqttClient?.Dispose();
        _influxDBClient?.Dispose();
    }
}
