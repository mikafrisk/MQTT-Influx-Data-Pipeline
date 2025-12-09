using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Writes;
using MQTTnet;
using System.Buffers;
using System.Text;

namespace MQTT_Influx_Data_Pipeline.MQTT
{
    public class MQTTClient
    {

        const string MQTT_BROKER_TCP = "127.0.0.1";
        const int MQTT_BROKER_PORT = 1883;
        // Jos haluat WebSocket-yhteyden:
        // const string MQTT_WS_URI = "ws://localhost:9001";
        const string TOPIC = "test/topic";

        const string INFLUX_URL = "http://localhost:8086";
        const string INFLUX_TOKEN = "HmR9GF2MozEk31D5v0wyd4ctfRlIajeXYNBC4V7rOJTwu96WWWMnCjGyL_ZglWbZBuaKHyz0YyYJ_E-BNa_nwg==";
        const string INFLUX_ORG = "myorg";
        const string INFLUX_BUCKET = "mybucket";
        private readonly IMqttClient _mqttClient;
        private readonly MqttClientOptions _mqttOptions;
        private readonly MqttClientFactory _factory;
        private readonly IInfluxDBClient _influxDBClient;
        private readonly IWriteApiAsync _writeApi;

        public MQTTClient(InfluxDBClient influxDBClient)
        {
            _factory = new MqttClientFactory();
            _mqttClient = _factory.CreateMqttClient();

            _mqttOptions = new MqttClientOptionsBuilder()
                .WithTcpServer(MQTT_BROKER_TCP, MQTT_BROKER_PORT)
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

        public async Task SubscribeAsync(string topic)
        {
            var mqttSubscribeOptions = _factory.CreateSubscribeOptionsBuilder()
            .WithTopicFilter(topic) // Subscribe to "test/topic"
            .Build();

            await _mqttClient.SubscribeAsync(mqttSubscribeOptions, CancellationToken.None);
        }

        public void SetApplicationMessageReceivedHandlerToStoreIntoInflux(Func<MqttApplicationMessageReceivedEventArgs, Task>? handler)
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
                    await _writeApi.WritePointAsync(point, INFLUX_BUCKET, INFLUX_ORG);
                }
                else
                {
                    Console.WriteLine("Payload is not a valid number, not writing to InfluxDB.");
                }
            };
        }
    }
}
