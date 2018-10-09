## Rebus - TaskCoordinator is <a href="https://github.com/rebus-org/Rebus" target="_blank"><b>the Rebus (https://github.com/rebus-org/Rebus)</b></a> with the worker threads part replaced 
to use <a href="https://github.com/BBGONE/TaskCoordinator" target="_blank"><b>the TaskCoordinator (https://github.com/BBGONE/TaskCoordinator)</b></a> which manages jobs execution using TPL Tasks instead
of plain threads. 
<br/>
Also it uses only one task when idle to monitor the queue for new messages instead of all worker threads as in the original Rebus implementation.
<br/>
P.S.: 
For demo purposes i modified the <b>Rebus.Transports.Showdown</b> sample to run it with the patched Rebus. You need to update the sql connection string in
the Rebus.Transports.Showdown.SqlServer projects.
