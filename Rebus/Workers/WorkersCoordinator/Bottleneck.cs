using System;
using System.Threading;
using System.Threading.Tasks;

namespace Rebus.TasksCoordinator
{
    public class Bottleneck
    {
        readonly SemaphoreSlim _semaphore;

        public Bottleneck(int maxParallelOperationsToAllow) => _semaphore = new SemaphoreSlim(maxParallelOperationsToAllow);

        public async Task<IDisposable> EnterAsync(CancellationToken cancellationToken) 
        {
            await _semaphore.WaitAsync(cancellationToken);

            return new Releaser(_semaphore);
        }

        public IDisposable Enter(CancellationToken cancellationToken)
        {
            _semaphore.Wait(100, cancellationToken);

            return new Releaser(_semaphore);
        }

        class Releaser : IDisposable
        {
            readonly SemaphoreSlim _semaphore;

            public Releaser(SemaphoreSlim semaphore) => _semaphore = semaphore;

            public void Dispose() => _semaphore.Release();
        }
    }
}