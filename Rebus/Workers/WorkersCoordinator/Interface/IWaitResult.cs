using System;

namespace Rebus.TasksCoordinator.Interface
{
    public interface IWaitResult: IDisposable
    {
        bool Result { get; }
    }
}
