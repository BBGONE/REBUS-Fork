using System.Threading;
using System.Threading.Tasks;

namespace TasksCoordinator.Interface
{
    public interface ITaskCoordinator
    {
        bool Start();
        Task Stop();
        bool IsPaused { get; set; }
        int MaxWorkersCount { get; set; }
        int FreeReadersAvailable { get; }
        int TasksCount { get; }
        CancellationToken Token { get; }
    }
}
