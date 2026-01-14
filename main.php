<?php
class conductor_server{
    public static function init(){
        settings::set("jobsFile", "conductor\\jobs.json", false);
    }

    public static function start(string $ip="0.0.0.0", int $port=52000):bool{
        //Start Socket Server
        if(!extension_ensure('sockets')){
            mklog(2, 'Extension sockets needed to run conductor server');
            return false;
        }

        if(network::ping($ip,$port,1)){
            mklog('warning',"Unable to listen on $ip:$port as it is already in use",false);
            return false;
        }

        $socket = communicator::createServer($ip,$port,false,$socketError,$socketErrorString);
        if(!$socket){
            mklog('warning',"Unable to listen on $ip:$port, $socketErrorString",false);
            return false;
        }
        echo "Listening on $ip:$port\n";
        exec('title Conductor Server ' . $port);

        stream_set_timeout($socket,5);

        while(true){
            $break = false;
            $clientSocket = communicator::acceptConnection($socket,5);
            if($clientSocket){

                $jobs = self::loadJobs();

                $startTime = time::millistamp();
                $tempconid = date("Y-m-d H:i:s");
                //echo "$tempconid: Received connection\n";

                $data = communicator::receive($clientSocket);
                $data = json_decode(base64_decode($data),true);
                $response = array("success" => false);

                $required = array("type","payload","name","password");
                foreach($required as $require){
                    if(!isset($data[$require])){
                        $response['error'] = "Missing data: " . $require;
                        echo "$tempconid: Missing data: " . $require . "\n";
                        goto respond;
                    }
                }

                if(!is_string($data['name']) || empty($data['name'])){
                    $response['error'] = "Invalid name";
                }

                if(!communicator::verifyPassword($data['password'])){
                    $response['error'] = "Incorrect password";
                    mklog("warning","Communicator: Incorrect passowrd submitted",false);
                    goto respond;
                }

                /////
                if($data["type"] === "requestJob"){
                    if(!self::getJob($data['name'] ,$data['payload'], $jobs, $response)){
                        if(!isset($response['error'])){
                            $response['error'] = "Unknown error";
                        }
                        mklog("warning","Unable to get job info: " . $response['error'],false);
                    }
                    else{
                        if(isset($response['job']['id'])){
                            echo "$tempconid: Sent job " . $response['job']['id'] . " to " . $data['name'] . "\n";
                        }
                    }
                }
                elseif($data["type"] === "updateJob"){
                    if(!self::updateJob($data['payload'], $jobs, $response)){
                        if(!isset($response['error'])){
                            $response['error'] = "Unknown error";
                        }
                        mklog("warning","Unable to update job: " . $response['error'],false);
                    }
                    else{
                        echo "$tempconid: " . $data['name'] . " updated job " . $data['payload']['id'] . "\n";
                    }
                }
                elseif($data["type"] === "addJob"){
                    if(!self::addJob($data['payload'], $jobs, $response)){
                        if(!isset($response['error'])){
                            $response['error'] = "Unknown error";
                        }
                        mklog("warning","Unable to add job: " . $response['error'],false);
                    }
                    else{
                        echo "$tempconid: " . $data['name'] . " created job " . $response['job_id'] . "\n";
                    }
                }
                elseif($data["type"] === "listJobs"){
                    $response['jobs'] = $jobs;
                    $response['success'] = true;
                    echo "$tempconid: " . $data['name'] . " listed all jobs\n";
                }
                elseif($data["type"] === "stop"){
                    $break = true;
                    $response['success'] = true;
                    $response['message'] = "Closing conductor server";
                    mklog('general','Conductor closed by request',false);
                }
                else{
                    $response['error'] = "Action does not exist";
                }

                /////

                respond:

                $response = base64_encode(json_encode($response));
                communicator::send($clientSocket,$response);

                $timeTaken = (time::millistamp() - $startTime)/1000;
                //echo $connid . ": Closing connection (" . $timeTaken . "s)\n";
                communicator::close($clientSocket);

                if(!self::saveJobs($jobs)){
                    mklog('error',"Unable to save jobs data",false);
                    break;
                }
                unset($jobs);

                if($timeTaken > 2){
                    echo "$tempconid: Warning: Last request took longer than 2 seconds\n";
                }
            }
            
            if($break){
                break;
            }
        }
        @communicator::close($socket);

        return $break;
    }
    public static function numberOfJobs():int{
        $jobNumbers = self::filterJobs(false);
        if(!is_array($jobNumbers)){
            return 0;
        }
        return $jobNumbers['pending'];
    }
    public static function numberOfTotalJobs():int{
        $jobs = self::loadJobs();
        if(!is_array($jobs)){
            return 0;
        }
        return count($jobs);
    }
    public static function getJobTimes(int $jobScope=5):array|false{
        $jobs = self::loadJobs();
        if(!is_array($jobs) || !array_is_list($jobs)){
            return false;
        }

        $jobTimes = [];
        foreach($jobs as $jobIndex => $job){
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
            return false;
        }

        $averageJobTime = round(math::average($jobTimes));
        $timeLeftEstimate = null;
        $finishTimeEstimate = "Unknown";
        $jobsPerDayEstimate = 0;

        $jobNumbers = self::filterJobs(false);

        if($jobNumbers['processing'] > 0){
            // Time for all pending jobs to complete add Time for currently processing jobs (on average, halfway done)
            $timeLeftEstimate = round((($averageJobTime * $jobNumbers['pending']) / $jobNumbers['processing']) + ($averageJobTime / 2));
            $finishTimeEstimate = date("Y-m-d H:i", time() + $timeLeftEstimate) . ":00";
            $jobsPerDayEstimate = round(86400 / ($averageJobTime / $jobNumbers['processing']));
        }

        return [
            'average_job_time' => $averageJobTime,
            'time_left_estimate' => $timeLeftEstimate,
            'finish_time_estimate' => $finishTimeEstimate,
            'jobs_per_day_estimate' => $jobsPerDayEstimate
        ];
    }
    public static function filterJobs(string|false $expression, int $offset=0, bool $recentFirst=false):array|false{
        if(is_string($expression) && !preg_match('/^(?=.+)[a-zA-Z_$()!][a-zA-Z0-9_\[\].\s$(),"\'&|!<>=+-]*$/', $expression)){
            return false;
        }

        $jobs = self::loadJobs();

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
            }
            catch(\Error){
                continue;
            }
        }

        if(is_string($expression)){
            return [
                'jobs' => $filtered,
                'matches' => $matches,
                'totals' => $totals
            ];
        }
        else{
            return $totals;
        }
    }

    private static function getJob(string $name, $data, &$jobs, &$response):bool{
        if(!is_array($data)){
            $response['error'] = "Data is not an array";
            return false;
        }
        if(!isset($data['abilities'])){
            $response['error'] = "abilities list not present";
            return false;
        }
        if(!is_array($data['abilities'])){
            $response['error'] = "abilities list is not an array";
            return false;
        }
        if(count($data['abilities']) < 1){
            $response['error'] = "abilities list is empty";
            return false;
        }
        foreach($data['abilities'] as $ability => $abilityVersion){
            if(!pkgmgr::validatePackageId($ability)){
                $response['error'] = "ability package id is invalid: " . $ability;
                return false;
            }
            if(!is_int($abilityVersion)){
                $response['error'] = "ability package version is not an integer: " . $abilityVersion;
                return false;
            }
            if($abilityVersion < 1){
                $response['error'] = "ability package version is invalid: " . $abilityVersion;
                return false;
            }
        }

        $availableJob = null;
        foreach($jobs as $job){
            if($job['completed'] !== false || $job['requested'] !== false || $job['return'] !== null){
                continue;
            }

            if(isset($job['target'])){
                if($job['target'] !== $name){
                    continue;
                }
            }

            foreach($job['requirements'] as $requirement => $requirementVersion){
                if(!isset($data['abilities'][$requirement])){
                    continue;
                }
                if($data['abilities'][$requirement] < $requirementVersion){
                    continue;
                }
            }
            
            $availableJob = $job;
            break;
        }

        if($availableJob === null){
            $response['message'] = "No jobs available";
        }
        else{
            $response['job'] = $availableJob;

            $requestedId = $response['job']['id'];
            foreach($jobs as $jobindex => $job){
                if($job['id'] === $requestedId){
                    $jobs[$jobindex]['requested'] = true;
                    $jobs[$jobindex]['request_time'] = time();
                    break;
                }
            }
        }

        $response['success'] = true;
        return true;
    }
    private static function updateJob($jobData, &$jobs, &$response):bool{
        if(!is_array($jobData)){
            $response['error'] = "jobdata is not an array";
            return false;
        }

        if(!isset($jobData['id'])){
            $response['error'] = "job id not set";
            return false;
        }
        if(!is_string($jobData['id'])){
            $response['error'] = "job id not a string";
            return false;
        }

        if(!isset($jobData['return'])){
            $response['error'] = "return not set";
            return false;
        }

        if(!isset($jobData['error_completing'])){
            $response['error'] = "error_completing not set";
            return false;
        }
        if(!is_bool($jobData['error_completing'])){
            $response['error'] = "error_completing not a boolean";
            return false;
        }

        $foundJob = false;
        foreach($jobs as $jobindex => $job){
            if($job['id'] === $jobData['id']){
                if($job['requested'] !== true){
                    $response['error'] = "Job " . $jobData['id'] . " has not been requested";
                    return false;
                }
                if($job['completed'] !== false){
                    $response['error'] = "Job already completed";
                    return false;
                }

                $jobs[$jobindex]['return'] = $jobData['return'];
                $jobs[$jobindex]['error_completing'] = $jobData['error_completing'];
                $jobs[$jobindex]['completed'] = true;
                $jobs[$jobindex]['completion_time'] = time();

                // finish function
                if(isset($jobs[$jobindex]['finish_function'])){
                    if(is_string($jobs[$jobindex]['finish_function'])){
                        $return = null;
                        $error = false;
                        try{
                            echo "Executing finish function: " . $jobs[$jobindex]['finish_function'] . "\n";
                            $return = eval('return ' . $jobs[$jobindex]['finish_function'] . ';');
                        }
                        catch(Throwable $throwable){
                            $error = true;
                            $return = null;
                            mklog("warning", "Error running finish function: " . $jobs[$jobindex]['finish_function'] . ": " . explode("\n",$throwable)[0] . "\n", false);
                        }

                        $jobs[$jobindex]['finish_function_return'] = $return;
                        $jobs[$jobindex]['finish_function_error'] = $error;
                    }
                    else{
                        echo "Finish function is not a string\n";
                    }
                }
                /////

                $foundJob = true;
                break;
            }
        }

        if(!$foundJob){
            $response['error'] = "Job " . $jobData['id'] . " not found";
            return false;
        }

        $response['success'] = true;
        return true;
    }
    private static function addJob($jobData, &$jobs, &$response):bool{
        if(!is_array($jobData)){
            $response['error'] = "Job data is not an array";
            return false;
        }
        $required = array("requirements","action_type","action");
        foreach($required as $require){
            if(!isset($jobData[$require])){
                $response['error'] = $require . " data not present";
                return false;
            }
        }

        $job['id'] = (string) time::millistamp();

        if(!is_array($jobData['requirements'])){
            $response['error'] = "requirements data is not an array";
            return false;
        }
        foreach($jobData['requirements'] as $requirement => $version){
            if(!pkgmgr::validatePackageId($requirement)){
                $response['error'] = "requirement package id is invalid: " . $requirement;
                return false;
            }
            if(!is_int($version) || $version < 1){
                $response['error'] = "requirement package version for " . $requirement . " is invalid: " . $version;
                return false;
            }
        }
        $job['requirements'] = $jobData['requirements'];

        if(isset($jobData['target'])){
            if(is_string($jobData['target'])){
                $job['target'] = $jobData['target'];
            }
        }

        if(!is_string($jobData['action_type'])){
            $response['error'] = "action_type data is not a string";
            return false;
        }
        $types = array("function_string");
        if(!in_array($jobData['action_type'],$types)){
            $response['error'] = "action_type is not a recognised type";
            return false;
        }
        $job['action_type'] = $jobData['action_type'];

        if(!is_string($jobData['action'])){
            $response['error'] = "action is not a string";
            return false;
        }
        $job['action'] = $jobData['action'];

        $job['return'] = null;
        $job['completed'] = false;
        $job['requested'] = false;
        $job['error_completing'] = false;
        $job['request_time'] = null;
        $job['completion_time'] = null;

        if(isset($jobData['finish_function'])){
            if(is_string($jobData['finish_function'])){
                $job['finish_function'] = $jobData['finish_function'];
                $job['finish_function_return'] = null;
                $job['finish_function_error'] = false;
            }
        }

        if(!is_array($jobs)){
            $response['error'] = "main jobs list is corrupted";
            return false;
        }
        $jobs[] = $job;

        $response['job_id'] = $job['id'];
        $response['success'] = true;
        return true;
    }
    private static function loadJobs():array{
        $jobsFile = self::jobsFile();
        $json = json::readFile($jobsFile,false);
        if(is_array($json)){
            return $json;
        }
        return array();
    }
    private static function saveJobs(array $jobs):bool{
        $jobsFile = self::jobsFile();
        return json::writeFile($jobsFile,$jobs,true);
    }
    private static function jobsFile():string{
        $setting = settings::read("jobsFile");
        if(is_string($setting)){
            return $setting;
        }
        return "conductor\\jobs.json";
    }
}