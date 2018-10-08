namespace Rebus.Workers
{
    /// <summary>
    /// Factory responsible for creating a "worker"
    /// </summary>
    public interface IWorkersCoordinatorFactory
    {
        IWorkersCoordinator CreateWorkerCoordinator(string name, int desiredNumberOfWorkers);
    }
}