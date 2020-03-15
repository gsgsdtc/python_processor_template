import subprocess

def runCommandWithOutput(cmd,stdinstr = ''):
  p=subprocess.Popen(cmd, shell=True, universal_newlines=True, stdin=subprocess.PIPE,stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
  #p.stdin.write(stdinstr)  
  stdoutdata, stderrdata = p.communicate(stdinstr)
  #p.stdin.close()
  return p.returncode, stdoutdata, stderrdata