#!/usr/bin/env python



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
        


    def wait(self, jobid, interval = 1, name = None, verbose = True, pbar = None, pbar_mode = None):
        """Waits for job running on SGE Grid Engine environment to finish.

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

            if pbar != None:
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
                        print "\r%s, waiting for jobid %i to finish" % (time.ctime(),int(jobid)),
                    else:
                        print "\r%s, waiting for job '%s' (jobid %i) to finish" % (time.ctime(),name,int(jobid)),
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
             if m == s == None: qsuboptslist.append('-t %d'       %  n)
             elif s == None:    qsuboptslist.append('-t %d-%d'    % (n, m))
             else:              qsuboptslist.append('-t %d-%d:%d' % (n, m, s)) 

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



    def getuserjobs(self, user = os.getlogin()):
        """Returns a list of SGE jobids run by a specific user

        Inputs
            user - SGE user to poll (Default = '', i.e. current user)
            qstat - path to qstat binary (Default = 'qstat')
        """

        p = subprocess.Popen(self.cmd_qstat + " -u " + user, shell = True, stdout = subprocess.PIPE)
        fout, _ = p.communicate()
        foutlist = fout.split('\n')

        joblist = []
        if len(foutlist) > 2:
            for l in foutlist[2:]:
                t = l.split()
                if len(t) > 0:
                    job = l.split()[0]
                    joblist.append(job)
    
        return joblist

    def run_job(self, command, name = 'default', logfnm = 'default.log', wait = True):
        jobid = self.submit(command, name = name, stdout = logfnm, stderr = logfnm)
        if wait:
            self.wait(jobid, verbose = True)
    
