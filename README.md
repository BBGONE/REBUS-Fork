## Rebus - TaskCoordinator is <a href="https://github.com/rebus-org/Rebus" target="_blank"><b>the Rebus (https://github.com/rebus-org/Rebus)</b></a> with the worker threads part replaced 
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
Also it is better to cap the read parallelism (threads reading messages from the queue concurrently).
<br/>
In the original Rebus implementation there's the ParallelOperationsManager which caps the overall parallelism 
(message processing as well, not only reading from the queue), 
and transports use AsyncBottleneck which caps overall access to the transport (not only the current queue).
<br/>
These caps are too broad. In my patch i introduced MaxReadParallelism instead of MaxParallelism (it was removed).
The MaxParallelism is the number of Workers (and they are really the TPL tasks, not plain threads).
<br> 
The bus starts its work by creating only one TPL task which waits for the messages in the queue.
When the TPL task receives the message it starts a new TPL task which in its turn waits for the messages.
The first TPL tasks processes the message and tries to receive the next one. If there are no messages then
the tasks ends its work and is removed. The total number of the tasks is capped by the MaxParallelism,
but when no messages in the queue, only one task is remained to wait for the messages.
<br> 
So in this implementation it is enough to operate with MaxReadParallelism (4 by default), 
and the number of the workers (which is really, is the maximum number of tasks that can be launched concurrently).
<br/>
<br/>
P.S.: 
For demo purposes i modified the <b>Rebus.Transports.Showdown</b> sample to run it with the patched Rebus. You need to update the sql connection string in
the Rebus.Transports.Showdown.SqlServer projects.
<br/>
If you want to see the real performance, then <b>don't run in Visual Studio (or run without debug)</b> because the results are dramatically different
from what when you will try to run with debugging. 
For example, on my comp sending to the queue is 2500 msgs/ sec, but when run in Visual Studio it was 690 msgs/ sec.
<br/>And the recieve results were also very different.
<br/>You can compare it with the original Rebus performance if you reference the original nuget Rebus5 package in the projects instead of the patched Rebus in the solution.
<br/><b>One thing to mention</b>: the tests in the Rebus usually use InMemoryTransport but it's a faulty approach because it uses the synchronous queue which is almost devoid of latency
and read contention. For more real world results use transports which are async in nature (like the Sql Server Transport) and have a read contention to the same queue
(this can be, File IO, Network IO, any real world queue).