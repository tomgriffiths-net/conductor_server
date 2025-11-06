# conductor_server
This is a package for PHP-CLI that hosts a server for handing out jobs for other computers to complete.

# Settings
- **jobsFile**: Specifies where to store the jobs information, expects string, default of "conductor\\jobs.json".

# Functions
- **start(string $ip="0.0.0.0", int $port=52000):bool**: Starts the conductor server with the specified ip and port, returns false on failure, blocks on success.
- **numberOfJobs():int**: Returns the number of pending jobs or 0 on failure.
- **numberOfTotalJobs():int**: Returns the number of jobs the server is aware of or 0 on failure.
- **getJobTimes(int $jobScope=5):array|false**: Gets the estimates for how long things will take on success or false on failure.
- **filterJobs(string|false $expression, int $offset=0, bool $recentFirst=false):array|false**: Returns the counts of filtered jobs using a boolean expression or false on failure.