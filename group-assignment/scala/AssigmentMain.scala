// Copyright 2025 Tampere University
// This notebook and software was developed for a Tampere University course COMP.CS.320.
// This source code is licensed under the MIT license. See LICENSE in the exercise repository root directory.
// Author(s): Ville Heikkilä (ville.heikkila@tuni.fi)

// The example solutions for the group assignment
package dip25.assignment

// This is the main entry point for running the assignment.
// The tasks should be done in the files BasicTasks.scala, AdvancedTask2.scala, etc
// And those are the files that should be submitted to Moodle.

object AssignmentMain extends App {
    def tryToRun(task: TaskTrait): Unit = {
        val taskName: String = task.getClass.getSimpleName.split("\\$")(0)
        if (task.tasksImplemented) {
            println(s"Running task ${taskName}")
            task.run()
        }
        else {
            println(s"Task ${taskName} is not implemented yet")
        }
    }

    // use command line arguments to select which tasks to run
    // if no arguments are given, run all tasks
    if (args.length == 0) {
        tryToRun(BasicTasks)
        tryToRun(AdvancedTask2)
        tryToRun(AdvancedTask3)
        tryToRun(AdvancedTask4)
    }
    else {
        args.foreach(
            arg => {
                arg.toLowerCase match {
                    case "basic" => tryToRun(BasicTasks)
                    case "at2" => tryToRun(AdvancedTask2)
                    case "at3" => tryToRun(AdvancedTask3)
                    case "at4" => tryToRun(AdvancedTask4)
                    case _ => println(s"Unknown task: ${arg}")
                }
            }
        )
    }
}
