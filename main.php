<?php
class conductor_server{
    public static function init(){
        settings::set("jobsFile", "conductor\\jobs.json", false);
    }

    /**
     * Calculates average job time and estimates, time left and jobs speed.
     * @param int $jobScope How many jobs to include in the average.
     * @return array An array containing a success boolean, job average time and speed/time left estimations, error message on failure.
     */
    public static function getJobTimes(int $jobScope=5):array{
        $filter = self::filterJobs();
        if(!$filter['success']){
            mklog(2, "Failed to get jobs");
            return [
                'success' => false,
                'error' => "Failed to get jobs"
            ];
        }

        $jobTimes = [];
        foreach($filter['jobs'] as $job){
            if(!isset($job['request_time']) || !is_int($job['request_time']) || !isset($job['completion_time']) || !is_int($job['completion_time'])){
                continue;
            }

            $timeTaken = $job['completion_time'] - $job['request_time'];

            $jobTimes[] = $timeTaken;
            if(count($jobTimes) > $jobScope){
                array_shift($jobTimes);
            }
        }

        if(empty($jobTimes)){
            return [
                'success' => false,
                'error' => "Failed to determine average job time"
            ];
        }

        $averageJobTime = round(math::average($jobTimes));
        $timeLeftEstimate = null;
        $finishTimeEstimate = "Unknown";
        $jobsPerDayEstimate = 0;

        if($filter['totals']['processing'] > 0){
            // Time for all pending jobs to complete add Time for currently processing jobs (on average, halfway done)
            $timeLeftEstimate = round((($averageJobTime * $filter['totals']['pending']) / $filter['totals']['processing']) + ($averageJobTime / 2));
            $finishTimeEstimate = date("Y-m-d H:i", time() + $timeLeftEstimate) . ":00";
            $jobsPerDayEstimate = round(86400 / ($averageJobTime / $filter['totals']['processing']));
        }

        return [
            'success' => true,
            'average_job_time' => $averageJobTime,
            'time_left_estimate' => $timeLeftEstimate,
            'finish_time_estimate' => $finishTimeEstimate,
            'jobs_per_day_estimate' => $jobsPerDayEstimate
        ];
    }
    /**
     * Filters jobs.
     * @param null|string $expression An optional filter expression. Available variables: count, taken, pending, processing, completed, successful, failed.
     * @param int $offset An optional start offset.
     * @param bool $recentFirst Weather to search recent jobs first.
     * @return array The jobs information.
     */
    public static function filterJobs(?string $expression=null, int $offset=0, bool $recentFirst=false):array{
        $response = ['success'=>false];

        if(is_string($expression) && !preg_match('/^(?=.+)[a-zA-Z_$()!][a-zA-Z0-9_\[\].\s$(),"\'&|!<>=+-]*$/', $expression)){
            $response['error'] = "Invalid filter expression";
            return $response;
        }

        $jobs = self::loadJobs();
        if(!is_array($jobs)){
            mklog(2, "Failed to read jobs file");
            $response['error'] = "Could not read jobs file";
            return $response;
        }

        $filtered = [];
        $matches = 0;
        $totals = [
            'totaljobs' => count($jobs),
            'taken' => 0,
            'pending' => 0,
            'processing' => 0,
            'completed' => 0,
            'successful' => 0,
            'failed' => 0
        ];

        if($recentFirst){
            $jobs = array_reverse($jobs);
        }

        foreach($jobs as $jobnum => $job){
            $jobnum++; //Remove starting at 0
            try{
                $count = count($filtered);
                $taken = ($job['completed'] !== false || $job['requested'] !== false || $job['return'] !== null);
                $pending = !$taken;
                $processing = ($taken && !$job['completed']);
                $completed = ($job['completed'] && $job['requested'] && $job['return'] !== null);
                $successful = ($job['completed'] && $job['return'] && (!isset($job['finish_function']) || $job['finish_function_return']));
                $failed = ($job['completed'] && !$successful);

                if($taken){$totals['taken']++;}
                if($pending){$totals['pending']++;}
                if($processing){$totals['processing']++;}
                if($completed){$totals['completed']++;}
                if($successful){$totals['successful']++;}
                if($failed){$totals['failed']++;}

                if(is_string($expression)){
                    if(eval('return (' . $expression . ');')){
                        $matches++;
                        if(($count + 1) > $offset){
                            $filtered[] = $job;
                        }
                    }
                }
                else{
                    $filtered[] = $job;
                }
            }
            catch(\Error){
                continue;
            }
        }

        $response['success'] = true;
        $response['jobs'] = $filtered;
        $response['matches'] = $matches;
        $response['totals'] = $totals;
        return $response;
    }

    /**
     * Gets the next available job.
     * @param string $name The name of the requester.
     * @param array $installedPackages An array of installed packages and their versions.
     * @return array The response with a success and job key if successful, the job array contains all the job information.
     */
    public static function getJob(array $installedPackages):array{
        $response = ['success'=>false];

        foreach($installedPackages as $packageName => $packageVersion){
            if(!pkgmgr::validatePackageId($packageName)){
                $response['error'] = "ability package id is invalid: " . $packageName;
                return $response;
            }
            if(!is_int($packageVersion) || $packageVersion < 1){
                $response['error'] = "ability package version is invalid: " . $packageName . ":" . $packageVersion;
                return $response;
            }
        }

        $jobs = self::loadJobs();
        if(!is_array($jobs)){
            mklog(2, "Failed to read jobs file");
            $response['error'] = "Could not read jobs file";
            return $response;
        }

        $name = communicator::getLastReceivedName();

        $availableJob = null;
        foreach($jobs as $job){
            if($job['completed'] !== false || $job['requested'] !== false || $job['return'] !== null){
                continue;
            }

            if(is_string($job['target'])){
                if($job['target'] !== $name){
                    continue;
                }
            }

            $requirementsMet = true;
            foreach($job['requirements'] as $requirement => $requirementVersion){
                if(($installedPackages[$requirement] ?? 0) < $requirementVersion){
                    $requirementsMet = false;
                    continue;
                }
            }
            if(!$requirementsMet){
                continue;
            }
            
            $availableJob = $job;
            break;
        }

        

        if(!is_array($availableJob)){
            $response['message'] = "No jobs available";
            $response['success'] = true;
            return $response;
        }
        
        $response['job'] = $availableJob;
        $requestedId = $response['job']['id'];
        foreach($jobs as $jobindex => $job){
            if($job['id'] === $requestedId){
                $jobs[$jobindex]['requested'] = true;
                $jobs[$jobindex]['request_time'] = time();
                break;
            }
        }

        if(!self::saveJobs($jobs)){
            mklog(2, "Failed to save jobs file");
            return [
                'success'=> false,
                'error' => "Failed to save updated job information"
            ];
        }

        mklog(1, "Sent job " . $response['job']['id'] . " to " . $name);

        $response['success'] = true;
        return $response;
    }
    /**
     * Marks a requested job as finished.
     * @param string $jobId The job id to set to finished.
     * @param mixed $jobReturn The return of the job action.
     * @param bool $errorCompletingJob Weather there was an error completing the job.
     * @return array An array containing a success key and an error key for error messages on failure.
     */
    public static function finishJob(string $jobId, mixed $jobReturn, bool $errorCompletingJob):array{
        $response = ['success'=>false];

        $jobs = self::loadJobs();
        if(!is_array($jobs)){
            mklog(2, "Failed to read jobs file");
            $response['error'] = "Could not read jobs file";
            return $response;
        }

        foreach($jobs as &$job){
            if($job['id'] !== $jobId){
                continue;
            }

            if($job['requested'] !== true){
                $response['error'] = "Job has not been requested yet";
                return $response;
            }
            if($job['completed'] !== false){
                $response['error'] = "Job already completed";
                return $response;
            }

            $job['return'] = $jobReturn;
            $job['error_completing'] = $errorCompletingJob;
            $job['completed'] = true;
            $job['completion_time'] = time();

            if(isset($job['finish_function'])){
                if(is_string($job['finish_function']) && !empty($job['finish_function'])){
                    $finishReturn = null;
                    $finishError = false;
                    try{
                        mklog(1, "Running finish function for job " . $job['id']);
                        $finishReturn = eval('return ' . $job['finish_function'] . ';');
                    }
                    catch(Throwable $throwable){
                        $finishError = true;
                        $finishReturn = null;
                        mklog(2, "Error running finish function for job " . $job['id'] . ": " . explode("\n",$throwable)[0] . "\n");
                    }

                    $job['finish_function_return'] = $finishReturn;
                    $job['finish_function_error'] = $finishError;
                }
                else{
                    mklog(2, "Finish function for job " . $job['id'] . " is set but not valid");
                }
            }

            if(!self::saveJobs($jobs)){
                mklog(2, "Failed to save jobs file");
                return [
                    'success'=> false,
                    'error' => "Failed to save updated job information"
                ];
            }

            mklog(1, "Job " . $jobId . " completed");

            $response['success'] = true;
            return $response;
        }
        unset($job);

        return [
            'success'=> false,
            'error' => "Job not found"
        ];
    }
    /**
     * Creates a job.
     * @param string $function The function string to be run.
     * @param array $requirements A list of required packages and package versions.
     * @param null|string $finishFunction An optional finish function the server runs when a job is completed.
     * @param null|string $target An optional target machine to send the job to.
     * @return array An array containing success boolean and a job id on success or error message on failure.
     */
    public static function addJob(string $function, array $requirements, ?string $finishFunction, ?string $target):array{
        $response = ['success'=>false];

        $job = [
            'id' => (string) floor(microtime(true)*1000),
            'completed' => false,
            'requested' => false,
            'requirements' => [],
            'target' => null,
            'action' => null,
            'return' => null,
            'error_completing' => null,
            'finish_function' => null,
            'finish_function_return' => null,
            'finish_function_error' => null
        ];
        
        if(empty(trim($function))){
            $response['error'] = "Function is empty";
            return $response;
        }
        $job['action'] = $function;

        if(is_string($finishFunction)){
            if(empty(trim($function))){
                $response['error'] = "Finish function is set but is empty";
                return $response;
            }
            $job['finish_function'] = $finishFunction;
        }

        foreach($requirements as $requirement => $version){
            if(!is_string($requirement) || empty(trim($requirement)) || !is_int($version) || $version < 1){
                $response['error'] = "Invalid requirements data";
                return $response;
            }
            if(!pkgmgr::validatePackageId($requirement)){
                $response['error'] = "Requirement package id is invalid: " . $requirement;
                return $response;
            }
        }
        $job['requirements'] = $requirements;

        if(is_string($target)){
            if(empty(trim($target))){
                $response['error'] = "Invalid target";
                return $response;
            }
            $job['target'] = $target;
        }

        $jobs = self::loadJobs();
        if(!is_array($jobs)){
            mklog(2, "Failed to read jobs file");
            $response['error'] = "Could not read jobs file";
            return $response;
        }
        
        $jobs[] = $job;

        if(!self::saveJobs($jobs)){
            mklog(2, "Failed to save jobs file");
            return [
                'success'=> false,
                'error' => "Failed to save updated job information"
            ];
        }

        mklog(1, "Job " . $job['id'] . " created");

        $response['job_id'] = $job['id'];
        $response['success'] = true;
        return $response;
    }

    private static function loadJobs():?array{
        $jobsFile = self::jobsFile();
        $json = json::readFile($jobsFile,false);
        if(is_array($json)){
            return $json;
        }
        return null;
    }
    private static function saveJobs(array $jobs):bool{
        $jobsFile = self::jobsFile();
        return json::writeFile($jobsFile,$jobs,true);
    }
    private static function jobsFile():string{
        $setting = settings::read("jobsFile");
        if(is_string($setting) && !empty($setting)){
            return $setting;
        }
        return "conductor\\jobs.json";
    }

    public static function communicatorServerActions():array{
        return [
            "getJob" => [
                "function" => "conductor_server::getJob",
                "args" => [
                    "--0"
                ]
            ],
            "finishJob" => [
                "function" => "conductor_server::finishJob",
                "args" => [
                    "--0",
                    "--1",
                    "--2"
                ]
            ],
            "addJob" => [
                "function" => "conductor_server::addJob",
                "args" => [
                    "--0",
                    "--1",
                    "--2",
                    "--3"
                ]
            ],
            "filterJobs" => [
                "function" => "conductor_server::filterJobs",
                "args" => [
                    "--0",
                    "--1",
                    "--2"
                ],
                "defArgs" => [
                    0 => null,
                    1 => 0,
                    2 => false,
                ]
            ],
            "getJobTimes" => [
                "function" => "conductor_server::getJobTimes",
                "args" => [
                    "--0"
                ],
                "defArgs" => [
                    0 => 5
                ]
            ],
        ];
    }
}