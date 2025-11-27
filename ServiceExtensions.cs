using Confluent.Kafka;
using KafkaMessaging.GenericProcessor.Configuration;
using KafkaMessaging.GenericProcessor.Handlers;
using KafkaMessaging.GenericProcessor.Models;
using KafkaMessaging.GenericProcessor.Producer;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KafkaMessaging.GenericProcessor;

public static class GenericProcessorServiceExtensions
{
    /// <summary>
    /// Register generic multi-message Kafka consumer with automatic handler resolution using Options pattern
    /// </summary>
    public static IServiceCollection AddGenericKafkaConsumer(
        this IServiceCollection services,
        IConfiguration configuration,
        Action<TopicRegistrationBuilder> configureTopics)
    {
        // Register Kafka options using Options pattern
        services.Configure<KafkaOptions>(configuration.GetSection(KafkaOptions.SectionName));

        // Validate options on startup
        services.AddOptions<KafkaOptions>()
            .Bind(configuration.GetSection(KafkaOptions.SectionName))
            .ValidateDataAnnotations()
            .ValidateOnStart();

        // Register all message handlers
        services.AddSingleton<IMessageHandler<OrderMessage>, OrderMessageHandler>();
        services.AddSingleton<IMessageHandler<PaymentMessage>, PaymentMessageHandler>();
        services.AddSingleton<IMessageHandler<NotificationMessage>, NotificationMessageHandler>();
        services.AddSingleton<IMessageHandler<InventoryUpdateMessage>, InventoryUpdateMessageHandler>();
        services.AddSingleton<IMessageHandler<ShippingMessage>, ShippingMessageHandler>();

        // Register the consumer
        services.AddSingleton<IGenericMultiMessageConsumer>(sp =>
        {
            var logger = sp.GetRequiredService<ILogger<GenericMultiMessageConsumer>>();
            var kafkaOptions = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;
            
            var config = CreateConsumerConfig(kafkaOptions.Consumer);

            var consumer = new GenericMultiMessageConsumer(logger, config, sp);

            // Configure topics using the builder
            var builder = new TopicRegistrationBuilder(consumer, kafkaOptions);
            configureTopics(builder);

            return consumer;
        });

        // Register the background service
        services.AddHostedService<GenericMultiMessageConsumerBackgroundService>();

        return services;
    }

    /// <summary>
    /// Register generic Kafka consumer with custom options configuration
    /// </summary>
    public static IServiceCollection AddGenericKafkaConsumer(
        this IServiceCollection services,
        Action<KafkaOptions> configureOptions,
        Action<TopicRegistrationBuilder> configureTopics)
    {
        // Register and configure options
        services.Configure(configureOptions);

        services.AddOptions<KafkaOptions>()
            .ValidateDataAnnotations()
            .ValidateOnStart();

        // Register all message handlers
        services.AddSingleton<IMessageHandler<OrderMessage>, OrderMessageHandler>();
        services.AddSingleton<IMessageHandler<PaymentMessage>, PaymentMessageHandler>();
        services.AddSingleton<IMessageHandler<NotificationMessage>, NotificationMessageHandler>();
        services.AddSingleton<IMessageHandler<InventoryUpdateMessage>, InventoryUpdateMessageHandler>();
        services.AddSingleton<IMessageHandler<ShippingMessage>, ShippingMessageHandler>();

        // Register the consumer
        services.AddSingleton<IGenericMultiMessageConsumer>(sp =>
        {
            var logger = sp.GetRequiredService<ILogger<GenericMultiMessageConsumer>>();
            var kafkaOptions = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;
            
            var config = CreateConsumerConfig(kafkaOptions.Consumer);

            var consumer = new GenericMultiMessageConsumer(logger, config, sp);

            // Configure topics using the builder
            var builder = new TopicRegistrationBuilder(consumer, kafkaOptions);
            configureTopics(builder);

            return consumer;
        });

        // Register the background service
        services.AddHostedService<GenericMultiMessageConsumerBackgroundService>();

        return services;
    }

    /// <summary>
    /// Register Kafka producers for all message types
    /// </summary>
    public static IServiceCollection AddKafkaProducers(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        // Register Kafka options if not already registered
        services.Configure<KafkaOptions>(configuration.GetSection(KafkaOptions.SectionName));

        // Register producers for each message type
        services.AddSingleton<IKafkaProducer<string, OrderMessage>>(sp =>
        {
            var logger = sp.GetRequiredService<ILogger<KafkaProducer<string, OrderMessage>>>();
            var options = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;
            var config = CreateProducerConfig(options.Producer);
            var valueSerializer = new JsonSerializer<OrderMessage>();
            
            return new KafkaProducer<string, OrderMessage>(logger, config, null, valueSerializer);
        });

        services.AddSingleton<IKafkaProducer<string, PaymentMessage>>(sp =>
        {
            var logger = sp.GetRequiredService<ILogger<KafkaProducer<string, PaymentMessage>>>();
            var options = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;
            var config = CreateProducerConfig(options.Producer);
            var valueSerializer = new JsonSerializer<PaymentMessage>();
            
            return new KafkaProducer<string, PaymentMessage>(logger, config, null, valueSerializer);
        });

        services.AddSingleton<IKafkaProducer<string, NotificationMessage>>(sp =>
        {
            var logger = sp.GetRequiredService<ILogger<KafkaProducer<string, NotificationMessage>>>();
            var options = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;
            var config = CreateProducerConfig(options.Producer);
            var valueSerializer = new JsonSerializer<NotificationMessage>();
            
            return new KafkaProducer<string, NotificationMessage>(logger, config, null, valueSerializer);
        });

        services.AddSingleton<IKafkaProducer<string, InventoryUpdateMessage>>(sp =>
        {
            var logger = sp.GetRequiredService<ILogger<KafkaProducer<string, InventoryUpdateMessage>>>();
            var options = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;
            var config = CreateProducerConfig(options.Producer);
            var valueSerializer = new JsonSerializer<InventoryUpdateMessage>();
            
            return new KafkaProducer<string, InventoryUpdateMessage>(logger, config, null, valueSerializer);
        });

        services.AddSingleton<IKafkaProducer<string, ShippingMessage>>(sp =>
        {
            var logger = sp.GetRequiredService<ILogger<KafkaProducer<string, ShippingMessage>>>();
            var options = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;
            var config = CreateProducerConfig(options.Producer);
            var valueSerializer = new JsonSerializer<ShippingMessage>();
            
            return new KafkaProducer<string, ShippingMessage>(logger, config, null, valueSerializer);
        });

        return services;
    }

    /// <summary>
    /// Register unified message producer service (Recommended)
    /// Single service that can produce any message type
    /// </summary>
    public static IServiceCollection AddMessageProducerService(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        // Register Kafka options if not already registered
        services.Configure<KafkaOptions>(configuration.GetSection(KafkaOptions.SectionName));

        // Register the unified producer service
        services.AddSingleton<IMessageProducerService, MessageProducerService>();

        return services;
    }

    /// <summary>
    /// Register a specific message handler
    /// Use this if you want to register handlers individually
    /// </summary>
    public static IServiceCollection AddMessageHandler<TMessage, THandler>(
        this IServiceCollection services)
        where TMessage : class
        where THandler : class, IMessageHandler<TMessage>
    {
        services.AddSingleton<IMessageHandler<TMessage>, THandler>();
        return services;
    }

    /// <summary>
    /// Create Confluent.Kafka ConsumerConfig from options
    /// </summary>
    private static ConsumerConfig CreateConsumerConfig(ConsumerOptions options)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            GroupId = options.GroupId,
            AutoOffsetReset = Enum.Parse<AutoOffsetReset>(options.AutoOffsetReset, true),
            EnableAutoCommit = options.EnableAutoCommit,
            EnableAutoOffsetStore = options.EnableAutoOffsetStore,
            MaxPollIntervalMs = options.MaxPollIntervalMs,
            SessionTimeoutMs = options.SessionTimeoutMs,
            AllowAutoCreateTopics = options.AllowAutoCreateTopics
        };

        // Optional settings
        if (options.FetchMinBytes.HasValue)
            config.FetchMinBytes = options.FetchMinBytes.Value;

        if (options.FetchMaxBytes.HasValue)
            config.FetchMaxBytes = options.FetchMaxBytes.Value;

        if (!string.IsNullOrEmpty(options.SecurityProtocol))
            config.SecurityProtocol = Enum.Parse<SecurityProtocol>(options.SecurityProtocol, true);

        if (!string.IsNullOrEmpty(options.SaslMechanism))
            config.SaslMechanism = Enum.Parse<SaslMechanism>(options.SaslMechanism, true);

        if (!string.IsNullOrEmpty(options.SaslUsername))
            config.SaslUsername = options.SaslUsername;

        if (!string.IsNullOrEmpty(options.SaslPassword))
            config.SaslPassword = options.SaslPassword;

        return config;
    }

    /// <summary>
    /// Create Confluent.Kafka ProducerConfig from options
    /// </summary>
    private static ProducerConfig CreateProducerConfig(ProducerOptions options)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,
            Acks = Enum.Parse<Acks>(options.Acks, true),
            EnableIdempotence = options.EnableIdempotence,
            MaxInFlight = options.MaxInFlight,
            MessageSendMaxRetries = options.MessageSendMaxRetries,
            RetryBackoffMs = options.RetryBackoffMs,
            RequestTimeoutMs = options.RequestTimeoutMs,
            CompressionType = Enum.Parse<CompressionType>(options.CompressionType, true),
            LingerMs = options.LingerMs,
            BatchSize = options.BatchSize
        };

        // Optional settings
        if (!string.IsNullOrEmpty(options.SecurityProtocol))
            config.SecurityProtocol = Enum.Parse<SecurityProtocol>(options.SecurityProtocol, true);

        if (!string.IsNullOrEmpty(options.SaslMechanism))
            config.SaslMechanism = Enum.Parse<SaslMechanism>(options.SaslMechanism, true);

        if (!string.IsNullOrEmpty(options.SaslUsername))
            config.SaslUsername = options.SaslUsername;

        if (!string.IsNullOrEmpty(options.SaslPassword))
            config.SaslPassword = options.SaslPassword;

        if (!string.IsNullOrEmpty(options.TransactionalId))
            config.TransactionalId = options.TransactionalId;

        return config;
    }
}

/// <summary>
/// Builder for registering topics with their message types
/// </summary>
public class TopicRegistrationBuilder
{
    private readonly IGenericMultiMessageConsumer _consumer;
    private readonly KafkaOptions _options;

    public TopicRegistrationBuilder(
        IGenericMultiMessageConsumer consumer,
        KafkaOptions options)
    {
        _consumer = consumer;
        _options = options;
    }

    /// <summary>
    /// Register Orders topic
    /// </summary>
    public TopicRegistrationBuilder AddOrdersTopic<TMessage>() where TMessage : class
    {
        _consumer.RegisterTopic<TMessage>(_options.Topics.Orders);
        return this;
    }

    /// <summary>
    /// Register Payments topic
    /// </summary>
    public TopicRegistrationBuilder AddPaymentsTopic<TMessage>() where TMessage : class
    {
        _consumer.RegisterTopic<TMessage>(_options.Topics.Payments);
        return this;
    }

    /// <summary>
    /// Register Notifications topic
    /// </summary>
    public TopicRegistrationBuilder AddNotificationsTopic<TMessage>() where TMessage : class
    {
        _consumer.RegisterTopic<TMessage>(_options.Topics.Notifications);
        return this;
    }

    /// <summary>
    /// Register Inventory Updates topic
    /// </summary>
    public TopicRegistrationBuilder AddInventoryUpdatesTopic<TMessage>() where TMessage : class
    {
        _consumer.RegisterTopic<TMessage>(_options.Topics.InventoryUpdates);
        return this;
    }

    /// <summary>
    /// Register Shipping topic
    /// </summary>
    public TopicRegistrationBuilder AddShippingTopic<TMessage>() where TMessage : class
    {
        _consumer.RegisterTopic<TMessage>(_options.Topics.Shipping);
        return this;
    }

    /// <summary>
    /// Register a custom topic by name
    /// </summary>
    public TopicRegistrationBuilder AddCustomTopic<TMessage>(string topicKey) where TMessage : class
    {
        if (_options.Topics.Custom.TryGetValue(topicKey, out var topicName))
        {
            _consumer.RegisterTopic<TMessage>(topicName);
        }
        else
        {
            throw new InvalidOperationException($"Custom topic '{topicKey}' not found in configuration");
        }
        return this;
    }

    /// <summary>
    /// Register a topic with explicit topic name
    /// </summary>
    public TopicRegistrationBuilder AddTopicWithName<TMessage>(string topicName) where TMessage : class
    {
        _consumer.RegisterTopic<TMessage>(topicName);
        return this;
    }
}
