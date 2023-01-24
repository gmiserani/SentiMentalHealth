import os
import subprocess


SUBREDDITS = ['AnsiedadeDepressao'] 
DATADIR = os.getcwd() + "/data/"
print(DATADIR)
if "data" not in os.listdir():
    os.makedirs(os.getcwd() + "/data/")
assert "src" in os.listdir() 

for subreddit in SUBREDDITS:
    command = f"python3 scraper.py --subreddit {subreddit}"
    print(command)
    proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    while proc.poll() is None:
        print(proc.stdout.readline()) #give output from your execution/your own message
    commandResult = proc.wait() #catch return code
    if commandResult != 0:
        print(f'Error code: {commandResult}')
    else:
        print('Data successfully crawled.\n')  