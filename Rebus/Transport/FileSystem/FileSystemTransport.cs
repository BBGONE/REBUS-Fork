using Newtonsoft.Json;
using Rebus.Bus;
using Rebus.Messages;
using Rebus.Time;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
#pragma warning disable 1998

namespace Rebus.Transport.FileSystem
{
    /// <summary>
    /// Transport implementation that uses the file system to send/receive messages.
    /// </summary>
    public class FileSystemTransport : ITransport, IInitializable, ITransportInspector
    {
        static readonly JsonSerializerSettings SuperSecretSerializerSettings = new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.None };
        static readonly Encoding FavoriteEncoding = Encoding.UTF8;
   
        readonly string _inputQueue;
        readonly FileNameGenerator _fileNameGenerator;
        readonly QueueRegister _queueRegister;
        readonly FileQueue _fileQueue;

        /// <summary>
        /// Constructs the file system transport to create "queues" as subdirectories of the specified base directory.
        /// While it is apparent that <seealso cref="_baseDirectory"/> must be a valid directory name, please note that 
        /// <seealso cref="_inputQueue"/> must not contain any invalid path either.
        /// </summary>
        public FileSystemTransport(string baseDirectory, string inputQueue)
        {
            if (inputQueue == null) return;
            FileSystemHelper.EnsureQueueNameIsValid(inputQueue);
            _inputQueue = inputQueue;

            _queueRegister = new QueueRegister(baseDirectory);
            _fileQueue = new FileQueue(inputQueue, _queueRegister);
            _fileNameGenerator = new FileNameGenerator();
        }

        /// <summary>
        /// Creates a "queue" (i.e. a directory) with the given name
        /// </summary>
        public void CreateQueue(string address)
        {
            _queueRegister.EnsureQueueInitialized(address);
        }

        /// <summary>
        /// Sends the specified message to the logical queue specified by <paramref name="destinationQueueName"/> by writing
        /// a JSON serialied text to a file in the corresponding directory. The actual write operation is delayed until
        /// the commit phase of the queue transaction unless we're non-transactional, in which case it is written immediately.
        /// </summary>
        public async Task Send(string destinationQueueName, TransportMessage message, ITransactionContext context)
        {
            var destinationDirectory = _queueRegister.EnsureQueueInitialized(destinationQueueName);

            var serializedMessage = Serialize(message);
            var fileName = _fileNameGenerator.GetNextFileName();
            string tempFileName = $"t{fileName.Substring(1)}";
            string tempFilePath = Path.Combine(destinationDirectory, tempFileName);

            context.OnCommitted(async () =>
            {
                // new file in async mode
                // write the file with the temporary name prefix (so it could not be read while it is written)
                using (var stream = new FileStream(tempFilePath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 1024 * 8, true))
                {
                    var bytes = FavoriteEncoding.GetBytes(serializedMessage);
                    await stream.WriteAsync(bytes, 0, bytes.Length);
                }
                
                // rename the file after the write is completed
                FileSystemHelper.RenameFile(tempFilePath, fileName, out var _);
            });
        }

        /// <summary>
        /// Checks if the message is still valid to be received
        /// </summary>
        /// <param name="fullPath"></param>
        /// <param name="receivedTransportMessage"></param>
        /// <returns></returns>
        private static bool _CheckIsValid(string fullPath, TransportMessage receivedTransportMessage)
        {
            bool isValid = true;
            if (receivedTransportMessage.Headers.TryGetValue(Headers.TimeToBeReceived, out var timeToBeReceived))
            {
                var maxAge = TimeSpan.Parse(timeToBeReceived);
                DateTime sendTimeUtc = FileSystemHelper.GetSendDate(fullPath);
                DateTime nowUtc = RebusTime.Now.UtcDateTime;

                var messageAge = nowUtc - sendTimeUtc;

                if (messageAge > maxAge)
                {
                    isValid = false;
                }
            }
            return isValid;
        }

        /// <summary>
        /// Receives the next message from the logical input queue by loading the next file from the corresponding directory,
        /// deserializing it, deleting it when the transaction is committed.
        /// </summary>
        public async Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            TransportMessage receivedTransportMessage = null;
            string fullPath = null;
            bool loopAgain = false;
            do
            {
                loopAgain = false;
                receivedTransportMessage = null;
                fullPath = await _fileQueue.Dequeue(cancellationToken);
                if (!string.IsNullOrEmpty(fullPath))
                {
                    var jsonText = await ReadAllText(fullPath);
                    receivedTransportMessage = Deserialize(jsonText);
                    if (!_CheckIsValid(fullPath, receivedTransportMessage))
                    {
                        File.Delete(fullPath);
                        loopAgain = true;
                    }
                }
            } while (loopAgain);


            if (receivedTransportMessage != null)
            {
                context.OnCompleted(async () => {
                    File.Delete(fullPath);
                });
                context.OnAborted(async () => {
                    FileSystemHelper.RenameToError(fullPath, out var _);
                });
            }
            return receivedTransportMessage;
        }

        static async Task<string> ReadAllText(string fullPath)
        {
            using (var stream = new FileStream(fullPath, FileMode.Open, FileAccess.Read, FileShare.None, 1024 * 8, true))
            using (var reader = new StreamReader(stream, FavoriteEncoding, false, 1024 * 8, true))
            {
                return await reader.ReadToEndAsync();
                
            }
        }

        /// <summary>
        /// Gets the logical input queue name which for this transport correponds to a subdirectory of the specified base directory.
        /// For other transports, this is a global "address", but for this transport the address space is confined to the base directory.
        /// Therefore, the global address is the same as the input queue name.
        /// </summary>
        public string Address => _inputQueue;

        /// <summary>
        /// Ensures that the "queue" is initialized (i.e. that the corresponding subdirectory exists).
        /// </summary>
        public void Initialize()
        {
            if (_inputQueue == null) return;

            _queueRegister.EnsureQueueInitialized(_inputQueue);
        }

        /// <summary>
        /// Gets the number of messages waiting in this "queue"
        /// </summary>
        public async Task<Dictionary<string, object>> GetProperties(CancellationToken cancellationToken)
        {
            var count = GetCount(cancellationToken);

            return new Dictionary<string, object>
            {
                {TransportInspectorPropertyKeys.QueueLength, count.ToString()}
            };
        }

        int GetCount(CancellationToken cancellationToken)
        {
            var directoryPath = _queueRegister.EnsureQueueInitialized(_inputQueue);
            var files = Directory.EnumerateFiles(directoryPath, "b*.json");
            return files.Aggregate(0, (counter, _) => {
                cancellationToken.ThrowIfCancellationRequested();
                return ++counter;
            });
        }

        static string Serialize(TransportMessage message)
        {
            return JsonConvert.SerializeObject(message, SuperSecretSerializerSettings);
        }

        static TransportMessage Deserialize(string serialiedMessage)
        {
            return JsonConvert.DeserializeObject<TransportMessage>(serialiedMessage, SuperSecretSerializerSettings);
        }
    }
}