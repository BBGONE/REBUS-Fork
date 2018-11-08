## Rebus - TaskCoordinator is <a href="https://github.com/rebus-org/Rebus" target="_blank"><b>the Rebus - Simple and lean service bus implementation for .NET (https://github.com/rebus-org/Rebus)</b></a> with the worker threads part replaced 
to use <a href="https://github.com/BBGONE/TaskCoordinator" target="_blank"><b>the TaskCoordinator (https://github.com/BBGONE/TaskCoordinator)</b></a> which manages jobs execution using TPL Tasks instead
of plain threads. 
<br/>
Also it uses only one task when idle to monitor the queue for new messages instead of all worker threads as in the original Rebus implementation.
<br/>
The main reason to create this patch was to relieve the stress from the queue by constant polling it by multiple workers.
It has the built-in autoscaling ability.
<br/>
Also in the original Rebus the FileSystemTransport is very unoptimized.
I have made the optimizations so it perfoms at 10 times of the original. 
The FileSystemTransport now supports receiving messages using several buses from the same queue
and it supports defered sends now.
<br/>
<br/>
In the original Rebus implementation there's the ParallelOperationsManager which caps the overall parallelism 
(message processing as well, not only reading from the queue), 
and transports use AsyncBottleneck which caps overall access to the transport (not only the current queue).
<br/>
These caps are too broad. In my patch i introduced MaxReadParallelism instead of MaxParallelism (it was removed).
The NumberOfWorkers is the maximum number of running TPL tasks, not plain threads.
<br/> 
The bus starts its work by creating only one TPL task which waits for the messages in the queue.
When the TPL task receives the message it starts a new TPL task which in its turn waits for the messages.
The first TPL tasks processes the message and tries to receive the next one. If there are no messages then
the tasks ends its work and is removed. The total number of the tasks is capped by the NumberOfWorkers,
but when no messages in the queue, only one task is remained to wait for the messages.
<br/>
For example (tested on InMemoryTransport), in my implementation when idle (no messages) - no matter how many workers configured, it is 9,1 queue reads per second.
<br/> 
In the original Rebus implementation with 1 worker and 10 MaxParallelism, it is 51 queue reads per second. 
With 1 worker and 20 MaxParallelism, it is 102 queue reads per second.
With 20 workers and 20 MaxParallelism, it is 133 queue reads per second.
<br/> 
<br/>
P.S.: 
For testing purposes i modified the <b>Rebus.Transports.Showdown</b> sample to run it with the patched Rebus. 
You need to update the sql connection string in the Rebus.Transports.Showdown.RunAll project.
