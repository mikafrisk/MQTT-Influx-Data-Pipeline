using MQTTnet;

namespace MQTT_Influx_Data_Pipeline.MQTT;

public interface IMQTTClient
{
    public Task ConnectAsync();
    public Task DisconnectAsync();
    public Task SubscribeAsync(string topic);
    public void SetApplicationMessageReceivedHandlerToStoreIntoInflux(string influxBucket, string influxOrg);
    public Task PublishMessage(string? topic, string? payload);
    public void Dispose();
}
