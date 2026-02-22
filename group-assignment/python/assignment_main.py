"""The main entry point for the group assignment"""

# Copyright 2025 Tampere University
# This notebook and software was developed for a Tampere University course COMP.CS.320.
# This source code is licensed under the MIT license. See LICENSE in the exercise repository root directory.
# Author(s): Ville Heikkilä (ville.heikkila@tuni.fi)

import sys


# This is the main entry point for running the assignment.
# The tasks should be done in the files basic_tasks.py, advanced_task2.py, etc
# And those are the files that should be submitted to Moodle.

def main():
    def try_to_tun(task_name: str) -> None:
        task = __import__(task_name)
        if task.tasks_implemented:
            print(f"Running task {task_name}")
            task.run()
        else:
            print(f"Task {task_name} is not implemented yet")

    # use command line arguments to select which tasks to run
    # if no arguments are given, run all tasks
    if len(sys.argv) <= 1:
        try_to_tun("basic_tasks")
        try_to_tun("advanced_task2")
        try_to_tun("advanced_task3")
        try_to_tun("advanced_task4")
    else:
        for arg in sys.argv[1:]:
            if arg == "basic":
                try_to_tun("basic_tasks")
            elif arg == "at2":
                try_to_tun("advanced_task2")
            elif arg == "at3":
                try_to_tun("advanced_task3")
            elif arg == "at4":
                try_to_tun("advanced_task4")
            else:
                print(f"Unknown task: {arg}")


if __name__ == "__main__":
    main()
