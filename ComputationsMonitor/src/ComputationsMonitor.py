import sys
import subprocess

if name == "__main__":
    args = ['java', '-jar', 'ComputationsMonitor.jar'] + sys.argv[1(inlove)
    try:
        with subprocess.Popen(args, stdout=subprocess.PIPE) as process:
            for line in iter(process.stdout.readline, b''):
                print(line)
    except Exception as err:
        print("An error appeared")
        print(err)
        print("exiting...")
        sys.exit(2)

    if process.returncode != 0:
        print("Program is terminated unsuccessfully. Please, see the log to get more details")
        sys.exit(1)