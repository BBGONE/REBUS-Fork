## Rebus - TaskCoordinator is <a href="https://github.com/rebus-org/Rebus" target="_blank"><b>the Rebus (https://github.com/rebus-org/Rebus)</b></a> with the worker threads part replaced 
to use <a href="https://github.com/BBGONE/TaskCoordinator" target="_blank"><b>the TaskCoordinator (https://github.com/BBGONE/TaskCoordinator)</b></a> which manages jobs execution using TPL Tasks instead
of plain threads. 
<br/>
Also it uses only one task when idle to monitor the queue for new messages instead of all worker threads as in the original Rebus implementation.
<br/>
The main reason to create this patch was to relieve the stress from the queue by constant polling it by multiple workers.
It has the built-in autoscaling ability.
<br/>
Also it is better to cap the read parallelism (threads reading from the queue concurrently).
<br/>
In the original Rebus implementation there's
the ParallelOperationsManager which caps overall parallelism (message processing as well, not only reading from the queue), also
transports use AsyncBottleneck which caps overall access to the transport (not only the current queue).
<br/>
These caps are too broad. In my patch i introduced MaxReadParallelism instead of MaxParallelism (it was removed).
The MaxParallelism is the number of Workers (and they are really the TPL tasks, not plain threads).
<br> 
So it is enough to operate with MaxReadParallelism (4 by default), and the number of the workers (which is really, just the maximum number of tasks that can be launched).
<br/>
In the original Rebus implementation the number of workers is mostly redundant, because 1 is enough. 
It spawns new task in the loop and does not wait for its completion, and that task becomes truly async on
the async recieve. After the transport message have been recieved its processing goes to the thread pool thread.
So, what the purpose of the number of workers? The main role plays the MaxParallelism.
<br/>
I tried in the showdown sample project (with original Rebus 5) simple tests:
<br/>
performance on my comp: the send is constant 2300 msgs/ sec
<br/>
<br/>
The Receive:
<br/>
1) o.SetMaxParallelism(5) and MaxNumberOfWorkers: 5 (measure the receive performance - i have 51 msgs/ sec)
<br/>
2) o.SetMaxParallelism(20) and MaxNumberOfWorkers: 1 (measure the receive performance - i have 738 msgs/ sec)
<br/>
3) o.SetMaxParallelism(20) and MaxNumberOfWorkers: 5 (measure the receive performance - i have 568 msgs/ sec)
<br/>
4) o.SetMaxParallelism(20) and MaxNumberOfWorkers: 20 (measure the receive performance - i have 688 msgs/ sec)
<br/>
<br/>
For comparison: in the Rebus with WorkersCoordinator with 10 workers - receives at  2380 msgs/sec
<br/>
in the Rebus with WorkersCoordinator with 5 workers - receives at  2300 msgs/sec
<br/>
<br/>
P.S.: 
For demo purposes i modified the <b>Rebus.Transports.Showdown</b> sample to run it with the patched Rebus. You need to update the sql connection string in
the Rebus.Transports.Showdown.SqlServer projects.
<br/>
If you want to see the real performance, then <b>don't run in Visual Studio</b> because the results are dramatically different
from what when you will try to run the compiled exe file. 
For example, on my comp sending to the queue is 2500 msgs/ sec, but when run in Visual Studio it was 690 msgs/ sec.
<br/>And the recieve results were also very different.
<br/>You can compare it with the original Rebus performance if you reference the original nuget Rebus5 package in the projects instead of the patched Rebus in the solution.