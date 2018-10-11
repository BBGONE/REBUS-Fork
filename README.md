## Rebus - TaskCoordinator is <a href="https://github.com/rebus-org/Rebus" target="_blank"><b>the Rebus (https://github.com/rebus-org/Rebus)</b></a> with the worker threads part replaced 
to use <a href="https://github.com/BBGONE/TaskCoordinator" target="_blank"><b>the TaskCoordinator (https://github.com/BBGONE/TaskCoordinator)</b></a> which manages jobs execution using TPL Tasks instead
of plain threads. 
<br/>
Also it uses only one task when idle to monitor the queue for new messages instead of all worker threads as in the original Rebus implementation.
<br/>
P.S.: 
For demo purposes i modified the <b>Rebus.Transports.Showdown</b> sample to run it with the patched Rebus. You need to update the sql connection string in
the Rebus.Transports.Showdown.SqlServer projects. If you want to see the real performance, then don't run in Visual Studio because the results are dramatically different
when you will try to run the compiled exe file. For example, on my comp sending to the queue is 2500 msgs/ sec, but when run in Visual Studio it was 690 msgs/ sec.
<br/>And the read results are also different.
<br/>You can compare it with the original Rebus performance if you reference the original nuget Rebus5 package in the projects instead of the patched Rebus in the solution.
<br/>On My comp it was difficult to squeeze the read performance from the original Rebus, the best results were at (2 or 3) workers and 20 MaxParallelism, it was 750 msgs/sec 
(and could drop to 50 msg/ sec with other settings - for example, 5 workers and 5 MaxParallelism - it was 50 - 110 msgs/sec),
with the Rebus-TaskCoordinator at 10 maximum workers (the tasks really) it was 1000 - 1500 msgs/sec. 
And setting other values to the maximum workers did not change the performance dramatically.
