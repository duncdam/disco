from prefect import task, flow
from prefect_shell import ShellOperation


def create_commmand():
    commands = ["stage-0", "stage-1", "stage-2", "stage-3", "disco"]
    return commands

@flow(name="download")
def run_download():
   ShellOperation(commands = ["chmod a+x ./scripts/download.sh ", "./scripts/download.sh"]).run()


@flow(name="stage-0")
def run_stage_0():
  sh= ShellOperation(commands = [f"clj -M:stage-0"]).run()
  return sh

@flow(name="stage-1")
def run_stage_1():
  sh= ShellOperation(commands = [f"clj -M:stage-1"]).run()   
  return sh

@flow(name="stage-2")
def run_stage_2():
  sh= ShellOperation(commands = [f"clj -M:stage-2"]).run()
  return sh

@flow(name="stage-3")
def run_stage_3():
  sh= ShellOperation(commands = [f"clj -M:stage-3"]).run()
  return sh

@flow(name="disco")
def run_disco():
  sh= ShellOperation(commands = [f"clj -M:disco"]).run()
  return sh

@flow(name="disco-pipeline")
def run_pipeline():
   run_download()
   run_stage_0()
   run_stage_1()
   run_stage_2()
   run_stage_3()
   run_disco()
    
         
if __name__ == "__main__":
    run_pipeline()