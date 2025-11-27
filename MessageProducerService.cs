using Confluent.Kafka;
using KafkaMessaging.GenericProcessor.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace KafkaMessaging.GenericProcessor.Producer;

/// <summary>
/// Unified message producer service that can produce any message type
/// </summary>
public interface IMessageProducerService
{
    Task<MessageProduceResult> ProduceAsync<TMessage>(
        TMessage message,
        string? key = null,
        CancellationToken cancellationToken = default) where TMessage : class;

    Task<MessageProduceResult> ProduceToTopicAsync<TMessage>(
        string topic,
        TMessage message,
        string? key = null,
        CancellationToken cancellationToken = default) where TMessage : class;
}

/// <summary>
/// Unified message producer implementation using a single Kafka producer
/// </summary>
public class MessageProducerService : IMessageProducerService, IDisposable
{
    private readonly ILogger<MessageProducerService> _logger;
    private readonly IProducer<string, string> _producer;
    private readonly KafkaOptions _options;
    private readonly Dictionary<Type, string> _topicMappings;

    public MessageProducerService(
        ILogger<MessageProducerService> logger,
        IOptions<KafkaOptions> options)
    {
        _logger = logger;
        _options = options.Value;

        var config = new ProducerConfig
        {
            BootstrapServers = _options.Producer.BootstrapServers,
            Acks = Enum.Parse<Acks>(_options.Producer.Acks, true),
            EnableIdempotence = _options.Producer.EnableIdempotence,
            MaxInFlight = _options.Producer.MaxInFlight,
            MessageSendMaxRetries = _options.Producer.MessageSendMaxRetries,
            RetryBackoffMs = _options.Producer.RetryBackoffMs,
            RequestTimeoutMs = _options.Producer.RequestTimeoutMs,
            CompressionType = Enum.Parse<CompressionType>(_options.Producer.CompressionType, true),
            LingerMs = _options.Producer.LingerMs,
            BatchSize = _options.Producer.BatchSize
        };

        // Optional security settings
        if (!string.IsNullOrEmpty(_options.Producer.SecurityProtocol))
            config.SecurityProtocol = Enum.Parse<SecurityProtocol>(_options.Producer.SecurityProtocol, true);

        if (!string.IsNullOrEmpty(_options.Producer.SaslMechanism))
            config.SaslMechanism = Enum.Parse<SaslMechanism>(_options.Producer.SaslMechanism, true);

        if (!string.IsNullOrEmpty(_options.Producer.SaslUsername))
            config.SaslUsername = _options.Producer.SaslUsername;

        if (!string.IsNullOrEmpty(_options.Producer.SaslPassword))
            config.SaslPassword = _options.Producer.SaslPassword;

        _producer = new ProducerBuilder<string, string>(config)
            .SetErrorHandler((_, error) =>
            {
                _logger.LogError(
                    "Kafka Producer Error - Code: {Code}, Reason: {Reason}, IsFatal: {IsFatal}",
                    error.Code, error.Reason, error.IsFatal);
            })
            .SetLogHandler((_, log) =>
            {
                var logLevel = log.Level switch
                {
                    SyslogLevel.Emergency or SyslogLevel.Alert or SyslogLevel.Critical => LogLevel.Critical,
                    SyslogLevel.Error => LogLevel.Error,
                    SyslogLevel.Warning => LogLevel.Warning,
                    SyslogLevel.Notice or SyslogLevel.Info => LogLevel.Information,
                    _ => LogLevel.Debug
                };
                _logger.Log(logLevel, "[{Name}] {Message}", log.Name, log.Message);
            })
            .Build();

        // Initialize topic mappings based on message types
        _topicMappings = InitializeTopicMappings();
    }

    /// <summary>
    /// Produce a message using automatic topic resolution based on message type
    /// </summary>
    public async Task<MessageProduceResult> ProduceAsync<TMessage>(
        TMessage message,
        string? key = null,
        CancellationToken cancellationToken = default) where TMessage : class
    {
        var messageType = typeof(TMessage);

        if (!_topicMappings.TryGetValue(messageType, out var topic))
        {
            throw new InvalidOperationException(
                $"No topic mapping found for message type {messageType.Name}. " +
                $"Please use ProduceToTopicAsync or register the topic mapping.");
        }

        return await ProduceToTopicAsync(topic, message, key, cancellationToken);
    }

    /// <summary>
    /// Produce a message to a specific topic
    /// </summary>
    public async Task<MessageProduceResult> ProduceToTopicAsync<TMessage>(
        string topic,
        TMessage message,
        string? key = null,
        CancellationToken cancellationToken = default) where TMessage : class
    {
        try
        {
            // Serialize message to JSON
            var messageJson = JsonSerializer.Serialize(message);

            // Generate key if not provided
            var messageKey = key ?? Guid.NewGuid().ToString();

            var kafkaMessage = new Message<string, string>
            {
                Key = messageKey,
                Value = messageJson,
                Timestamp = Timestamp.Default
            };

            // Produce message
            var result = await _producer.ProduceAsync(topic, kafkaMessage, cancellationToken);

            _logger.LogInformation(
                "Message produced - Type: {MessageType}, Topic: {Topic}, Partition: {Partition}, Offset: {Offset}",
                typeof(TMessage).Name,
                result.Topic,
                result.Partition.Value,
                result.Offset.Value);

            return new MessageProduceResult
            {
                Topic = result.Topic,
                Partition = result.Partition.Value,
                Offset = result.Offset.Value,
                Timestamp = result.Timestamp.UtcDateTime,
                Key = messageKey,
                MessageType = typeof(TMessage).Name
            };
        }
        catch (ProduceException<string, string> ex)
        {
            _logger.LogError(ex,
                "Failed to produce message - Type: {MessageType}, Topic: {Topic}, Error: {Error}",
                typeof(TMessage).Name, topic, ex.Error.Reason);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Unexpected error producing message - Type: {MessageType}, Topic: {Topic}",
                typeof(TMessage).Name, topic);
            throw;
        }
    }

    /// <summary>
    /// Initialize topic mappings based on configured topics and known message types
    /// </summary>
    private Dictionary<Type, string> InitializeTopicMappings()
    {
        var mappings = new Dictionary<Type, string>();

        // Get the assembly containing message types
        var messageTypes = AppDomain.CurrentDomain.GetAssemblies()
            .SelectMany(a => a.GetTypes())
            .Where(t => t.Namespace?.Contains("Models") == true && t.IsClass && !t.IsAbstract)
            .ToList();

        // Map known message types to topics
        foreach (var type in messageTypes)
        {
            var topic = type.Name switch
            {
                "OrderMessage" => _options.Topics.Orders,
                "PaymentMessage" => _options.Topics.Payments,
                "NotificationMessage" => _options.Topics.Notifications,
                "InventoryUpdateMessage" => _options.Topics.InventoryUpdates,
                "ShippingMessage" => _options.Topics.Shipping,
                _ => null
            };

            if (!string.IsNullOrEmpty(topic))
            {
                mappings[type] = topic;
                _logger.LogDebug("Mapped {MessageType} to topic {Topic}", type.Name, topic);
            }
        }

        return mappings;
    }

    public void Dispose()
    {
        try
        {
            _producer?.Flush(TimeSpan.FromSeconds(10));
            _producer?.Dispose();
            _logger.LogInformation("Message producer service disposed successfully");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error disposing message producer service");
        }
    }
}

/// <summary>
/// Result of message production
/// </summary>
public class MessageProduceResult
{
    public string Topic { get; set; } = string.Empty;
    public int Partition { get; set; }
    public long Offset { get; set; }
    public DateTime Timestamp { get; set; }
    public string Key { get; set; } = string.Empty;
    public string MessageType { get; set; } = string.Empty;
}
