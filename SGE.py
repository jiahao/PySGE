#!/usr/bin/env python

"""
TODO Add support for SGE logs
$SGE_ROOT/$SGE_CELL/spool/qmaster/messages
$SGE_ROOT/$SGE_CELL/spool/qmaster/messages

execd spool logs
$SGE_ROOT/$SGE_CELL/spool/<node>/messages

panic location
/tmp in lieu of $SGE_ROOT
"""

import pwd, os, subprocess

from qsubopts import *

class _JobData:
    """
    Internal helper class to manage job data from qstat
    """
    def __init__(self, qstat_job_line):
        #The format of the line goes like
        #job-ID  prior   name       user         state submit/start at     queue                          slots ja-task-ID 

        tokens = qstat_job_line.split()

        assert len(tokens) >= 8, 'Not a valid qstat line: '+qstat_job_line
        #Job line must have at least 8 tokens
        self.job_ID = int(tokens[0])
        self.priority = float(tokens[1])
        self.name = tokens[2]
        self.user = tokens[3]
        self.state = tokens[4]
        self.time = ' '.join(tokens[5:7])

        if '@' in qstat_job_line:
            #Has queue assigned, e.g. core2.q@q2.caspian.mit.edu
            self.queue = tokens[7]
            self.slots = tokens[8]
            if len(tokens) == 9:
                self.ja_task_ID = None
            elif len(tokens) == 10:
                self.ja_task_ID = tokens[-1]
            else:
                raise ValueError, 'Could not parse line: '+qstat_job_line
        else:
            #No queue assigned
            self.slots = tokens[7]
            if len(tokens) == 8:
                self.ja_task_ID = None
            elif len(tokens) == 9:
                self.ja_task_ID = tokens[-1]
            else:
                raise ValueError, 'Could not parse line: '+qstat_job_line

        #Convert array indices ja_task_ID into python list format
        ja_task_ID = []
        if self.ja_task_ID is not None:
            for blob in self.ja_task_ID.split(','):
                #Parse data of form '193-349:1' or '1934'
                x = blob.split(':')
                if len(x) == 1:
                    ja_task_ID += x
                else:
                    subjobs, step = x
                    begin, end = subjobs.split('-')
                    ja_task_ID += range(int(begin),int(end)+1, int(step))

        self._ja_tasklist = ja_task_ID
    
    def __repr__(self):
        repr = ['{']
        for key, value in self.__dict__.items():
            if key[0] != '_':
                repr.append(key+'='+str(value))
        repr.append('}')
        return '\n'.join(repr)



class JobList:
    """
    Internal helper class to manage job lists
    """

    def __init__(self, qstat_output = None):
        self._joblist = []
        for line in qstat_output.split('\n')[2:-1]:
            self._joblist.append(_JobData(line))

    def __iter__(self):
        for job in self._joblist:
            yield job

    def __repr__(self):
        return '\n'.join([str(job) for job in self._joblist])



class SGE:
    """External system call handler for Sun Grid Engine environment."""

    def __init__(self, q = '', path = '', ):
        if q == '':
            #@TODO By default, submit to all available queues

            self.cmd_qconf = os.path.join(path, 'qconf')

            try:
                qlist = _exec(self.cmd_qconf+' -sql')
            except IOError:
                print "Error querying queue configuration"
                raise IOError

            self.q = qlist.replace('\n',',')[:-1]
            #print "Sun Grid Engine handler initialized"
            #print "Queues detected:",self.q

        else:
            self.q = q

        self.cmd_qsub = os.path.join(path, 'qsub')
        self.cmd_qstat = os.path.join(path, 'qstat')
        


    def wait(self, jobid, interval = 10, name = None, verbose = True, pbar = None, pbar_mode = None):
        """Waits for job running on SGE Grid Engine environment to finish.

        If you are just waiting for one job, this becomes a dumb substitute for the -sync y option
        which can be specified to qsub.

        Inputs:

        jobid - 
        cmd - The command line to run SGE's qstat. (Default "qstat")
        interval - How often to check , in seconds. (Default, 10)

        """
        dowait = True
        waited = False
        while dowait:
            p = subprocess.Popen(self.cmd_qstat, shell = True, stdout = subprocess.PIPE)
            pout, _ = p.communicate()

            if pbar is not None:
                verbose = False
                if pbar_mode == 'quantum':
                    pbar.update(getstarfleet(getouts()))
                elif pbar_mode == 'mdrun':
                    pbar.update(getstep())

            dowait = False
            for line in pout.split('\n'):
                t = line.split()
                if len(t) >= 5: #Find a line with useful info
                    #Job must be queued, running or being transferred

                    if t[0] == str(jobid) and re.search(t[4], 'qwrt'):
                        dowait = True
                        break
                    if t[0] == str(jobid) and re.search(t[4], 'acuE'): #Job or host in error state
                        print "Warning: job", jobid, "in error state"
    
            if dowait:
                time.sleep(interval)
                waited = True
                if verbose:
                    if name == None:
                        print "\r%s, waiting for jobid %i to finish" % (time.ctime(), int(jobid)),
                    else:
                        print "\r%s, waiting for job '%s' (jobid %i) to finish" % (time.ctime(), name, int(jobid)),
        if waited and verbose:
            print



    def submit(self, job, Verbose = True, array = False, useenvironment = True, usecwd = True, name = None, stdin = None, stdout = None, stderr = None, joinstdouterr = True, nproc = 1, lammpi = True):
        """Submits a job to SGE

        Returns jobid as a number"""
        if Verbose: print "Submitting job:    \x1b[1;91m%-50s\x1b[0m Stdout: %s Stderr: %s" % (job,stdout,stderr)

        #Parameters to qsub specified as the header of the job specified on STDIN
        lamstring = lammpi and " -pe lammpi %d" % nproc or ""
        qsuboptslist = ['-cwd -V ', lamstring]

        if name   != None: qsuboptslist.append('-N '+name)
        if stdin  != None: qsuboptslist.append('-i '+stdin)
        if stdout != None: qsuboptslist.append('-o '+stdout)
        if stderr != None: qsuboptslist.append('-e '+stderr)
        if joinstdouterr:  qsuboptslist.append('-j')
        if usecwd:         qsuboptslist.append('-cwd')
        if useenvironment: qsuboptslist.append('-V')
        if array != False:
            try:
                n = int(array[0])
            except IndexError:
                n = int(array)
            try:
                m = int(array[1])
            except IndexError:
                m = None
            try:
                s = int(array[2])
            except IndexError:
                s = None
            if m == s == None:
                qsuboptslist.append('-t %d'       %  n)
            elif s == None:
                qsuboptslist.append('-t %d-%d'    % (n, m))
            else:
                qsuboptslist.append('-t %d-%d:%d' % (n, m, s)) 

        qsubopts = ' '.join(qsuboptslist)

        pout = _exec(self.cmd_qsub, stdin = qsubopts + '\n' + job, print_command = False)

##         p = subprocess.Popen(self.cmd_qsub, shell = True, stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
##         pout, _ = p.communicate(qsubopts + '\n' + job)

        try:
            #Next to last line should be "Your job 1389 (name) has been submitted"
            #parse for job id
            jobid = int(pout.split('\n')[-2].split()[2])
            return jobid
        except (ValueError, IndexError, AttributeError), e:
            print "Error submitting SGE job"
            print "Job submitted was"
            print qsubopts
            print job
            print "Output was"
            print pout
            raise e     



    def getuserjobs(self, user = pwd.getpwuid(os.getuid())[0]):
        """Returns a list of SGE jobids run by a specific user

        Inputs
            user - SGE user to poll (Default = '', i.e. current user)
            qstat - path to qstat binary (Default = 'qstat')
        """

        p = subprocess.Popen(self.cmd_qstat + " -u " + user, shell = True, stdout = subprocess.PIPE)
        qstat_output, _ = p.communicate()
        joblist = JobList(qstat_output)
        return [job for job in joblist if job.user == user]

    def run_job(self, command, name = 'default', logfnm = 'default.log', wait = True):
        jobid = self.submit(command, name = name, stdout = logfnm, stderr = logfnm)
        if wait:
            self.wait(jobid, verbose = True)



def _exec(command, print_to_screen = False, logfnm = None, stdin = None, print_command = False, **kwargs):
    """
    Runs command line using subprocess, optionally returning stdout
    """
    print_to_file = (logfnm != None)
    if print_to_file:
        f = open(logfnm,'a')
    if print_command:
        print "Executing process: \x1b[1;92m%-50s\x1b[0m Logfile: %s" % (command, logfnm)
        if print_to_file:
            print >> f, "Executing process: %s" % command
    if stdin == None:
        p = subprocess.Popen(command, shell=True, stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
        Output, _ = p.communicate()
    else:
        p = subprocess.Popen(command, shell=True, stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
        Output, _ = p.communicate(stdin)
    if print_to_screen:
        print Output
    if logfnm != None:
        f.write(Output)
        f.close()
    return Output
 
