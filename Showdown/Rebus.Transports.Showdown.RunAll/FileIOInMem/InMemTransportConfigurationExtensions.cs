using System;
using Rebus.Config;

namespace Rebus.Transport.InMem2
{
    /// <summary>
    /// Configuration extensions for the in-mem transport
    /// </summary>
    public static class FileIOInMemTransportConfigurationExtensions
    {
        /// <summary>
        /// Configures Rebus to use in-mem message queues, delivering/receiving from the specified <see cref="FileIOInMemNetwork"/>
        /// </summary>
        public static void UseFileIOInMemoryTransport(this StandardConfigurer<ITransport> configurer, FileIOInMemNetwork network, string inputQueueName)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (network == null) throw new ArgumentNullException(nameof(network));
            if (inputQueueName == null) throw new ArgumentNullException(nameof(inputQueueName));

            configurer.OtherService<FileIOInMemTransport>()
                .Register(context => new FileIOInMemTransport(network, inputQueueName));

            configurer.OtherService<ITransportInspector>()
                .Register(context => context.Get<FileIOInMemTransport>());

            configurer.Register(context => context.Get<FileIOInMemTransport>());
        }

        /// <summary>
        /// Configures Rebus to use in-mem message queues, configuring this instance to be a one-way client
        /// </summary>
        public static void UseFileIOInMemoryTransportAsOneWayClient(this StandardConfigurer<ITransport> configurer, FileIOInMemNetwork network)
        {
            if (configurer == null) throw new ArgumentNullException(nameof(configurer));
            if (network == null) throw new ArgumentNullException(nameof(network));

            configurer.Register(c => new FileIOInMemTransport(network, null));

            OneWayClientBackdoor.ConfigureOneWayClient(configurer);
        }
    }
}