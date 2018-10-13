using Rebus.Messages;

namespace Rebus.Transport.InMem2
{
    /// <summary>
    /// Extensions that make it nice to work with <see cref="FileIOInMemTransportMessage"/> and <see cref="TransportMessage"/>
    /// </summary>
    public static class FileIOInMemTransportMessageExtensions
    {
        /// <summary>
        /// Returns a new <see cref="FileIOInMemTransportMessage"/> containing the headers and the body data of the <see cref="TransportMessage"/>
        /// </summary>
        public static FileIOInMemTransportMessage ToInMemTransportMessage(this TransportMessage transportMessage)
        {
            return new FileIOInMemTransportMessage(transportMessage);
        }
    }
}