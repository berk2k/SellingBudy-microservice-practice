using EventBus.Base.Abstraction;
using EventBus.Base.SubManagers;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace EventBus.Base.Event
{
    public abstract class BaseEventBus : IEventBus
    {
        public readonly IServiceProvider ServiceProvider;
        public readonly IEventBusSubscriptionManager SubsManager;

        private EventBusConfig? eventBusConfig;

        public BaseEventBus(IServiceProvider serviceProvider, EventBusConfig config)
        {
            eventBusConfig = config;
            ServiceProvider = serviceProvider;
            SubsManager = new InMemoryEventBusSubscriptionManager(ProcessEventName);
        }

        public virtual string ProcessEventName(string eventName)
        {
            if (eventBusConfig is not null)
            {
                if (eventBusConfig.DeleteEventPrefix)
                {
                    eventName = eventName.TrimStart(eventBusConfig.EventNamePrefix.ToArray());
                }

                if (eventBusConfig.DeleteEventSuffix)
                {
                    eventName = eventName.TrimEnd(eventBusConfig.EventNameSuffix.ToArray());
                }
            }

            return eventName;
        }

        public virtual string GetSubName(string eventName) => $"{eventBusConfig?.SubscriberClientAppName}.{ProcessEventName(eventName)}";

        public virtual void Dispose()
        {
            eventBusConfig = null;
        }

        public async Task<bool> ProcessEvent(string eventName, string message)
        {
            eventName = ProcessEventName(eventName);

            bool processed = false;

            if (SubsManager.HasSubscriptionsForEvent(eventName))
            {
                using (IServiceScope scope = ServiceProvider.CreateScope())
                {
                    foreach (SubscriptionInfo subscription in SubsManager.GetHandlersForEvent(eventName))
                    {
                        object? handler = ServiceProvider.GetService(subscription.HandlerType);
                        if (handler is not null)
                        {
                            Type? eventType = SubsManager.GetEventTypeByName($"{eventBusConfig?.EventNamePrefix}{eventName}{eventBusConfig?.EventNameSuffix}");
                            if (eventType is not null)
                            {
                                object? integrationEvent = JsonConvert.DeserializeObject(message, eventType);

                                if (integrationEvent is not null)
                                {
                                    MethodInfo? handleMethod = typeof(IIntegrationEventHandler<>).MakeGenericType(eventType).GetMethod("Handle");
                                    if (handleMethod is not null)
                                    {
                                        await (Task)handleMethod.Invoke(handler, new object[] { integrationEvent });
                                    }
                                }
                            }
                        }
                    }
                }

                processed = true;
            }

            return processed;
        }

        public abstract void Publish(IntegrationEvent @event);

        public abstract void Subscribe<T, TH>()
            where T : IntegrationEvent
            where TH : IIntegrationEventHandler<T>;

        public abstract void UnSubscribe<T, TH>()
            where T : IntegrationEvent
            where TH : IIntegrationEventHandler<T>;
    }
}
