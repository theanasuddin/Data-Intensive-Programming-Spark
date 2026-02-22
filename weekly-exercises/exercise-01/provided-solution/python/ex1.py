"""The example solutions for exercise 1."""

# Copyright 2025 Tampere University
# This notebook and software was developed for a Tampere University course COMP.CS.320.
# This source code is licensed under the MIT license. See LICENSE in the exercise repository root directory.
# Author(s): Ville Heikkilä (ville.heikkila@tuni.fi)



def main():
    # COMP.CS.320 Data-Intensive Programming, Exercise 1
    #
    # This exercise is mostly intended as an introduction to the Azure Databricks notebook system, but it can be done locally as well.
    # There are some basic programming tasks that can be done in either Scala or Python.
    # The Spark related tasks start next week in exercise 2.
    #
    # This is the Python version intended for local development.
    #
    # Each task is separated by the printTaskLine() function. Add your solutions to replace the question marks.
    # There is test code and example output following most of the tasks that involve producing code.
    # Uncomment the code in order to run the tests.
    #
    # At the end of the file, there is a question regarding the use of AI or other collaboration when working the tasks.
    # Please remember to answer the AI question. And finally, don't forget to submit your solutions to Moodle.



    printTaskLine(1)
    # Task 1 - Read tutorial
    #
    # Read the "Basics of using Databricks notebooks" tutorial notebook:
    # https://adb-204444790738407.7.azuredatabricks.net/editor/notebooks/2157421122333722
    # (a copy can be found in the ex1 folder of the exercise repository)
    #
    # Instructions for testing the tutorial in the Databricks environment:
    # The notebook is in a read-only folder, so to make edits clone the tutorial notebook to your own workspace and
    # run at least the first couple of code examples.
    # - File -> Clone...
    # - Modify the notebook name if you want
    # - Select your workspace folder (Workspace/Users/YOUR_TUNI_EMAIL) using Browse
    # - Clone
    # - Now a clone of the notebook is in your folder and you can edit the cells.
    #   "tuni-cs320-f2025-dbx" cluster should be available for you to run and test the notebook.
    #
    # To get a point from this task, replace "???" with "Task 1 is done" (or something similar)
    # to the following (after you have read the tutorial).

    # Task 1 is done



    printTaskLine(2)
    # Task 2 - Basic functions
    #
    # Part 1:
    #
    # - Write a simple function `mySum` that takes two integers as parameters and returns their sum.
    #
    # Part 2:
    #
    # - Write a function `myTripleAvg` that takes three integers as parameters
    #   and returns their average rounded to the nearest integer.

    def mySum(x: int, y: int) -> int:
        return x + y

    def myTripleAvg(a: int, b: int, c: int) -> int:
        avg: float = (a + b + c) / 3.0
        return round(avg)


    # Code to test your functions:
    print(f"- mySum(20, 21) = {mySum(20, 21)}")
    print(f"- mySum(-2, 17) = {mySum(-2, 17)}")
    print(f"- myTripleAvg(20, 21, 23) = {myTripleAvg(20, 21, 23)}")
    print(f"- myTripleAvg(20, 21, 24) = {myTripleAvg(20, 21, 24)}")
    print(f"- myTripleAvg(12, -9, -3) = {myTripleAvg(12, -9, -3)}")

    # Example output:
    # ==============
    # - mySum(20, 21) = 41
    # - mySum(-2, 17) = 15
    # - myTripleAvg(20, 21, 23) = 21
    # - myTripleAvg(20, 21, 24) = 22
    # - myTripleAvg(12, -9, -3) = 0



    printTaskLine(3)
    # Task 3 - Fibonacci numbers
    #
    # The Fibonacci numbers, F_n, are defined such that each number is the sum of the two preceding numbers.
    # The first two Fibonacci numbers are: F_0 = 0 and F_1 = 1.
    #
    # Write a **recursive** function, `fibonacci`, that takes in the index and returns the Fibonacci number.
    # (no need for any optimized solution here)

    def fibonacci(n: int) -> int:
        if n < 1:
            return 0
        if n == 1:
            return 1
        return fibonacci(n-1) + fibonacci(n-2)


    print(f"- fibonacci(1) = {fibonacci(1)}")
    print(f"- fibonacci(5) = {fibonacci(5)}")
    print(f"- fibonacci(9) = {fibonacci(9)}")
    print(f"- fibonacci(12) = {fibonacci(12)}")
    print(f"- fibonacci(17) = {fibonacci(17)}")

    # Example output:
    # ==============
    # - fibonacci(1) = 1
    # - fibonacci(5) = 5
    # - fibonacci(9) = 34
    # - fibonacci(12) = 144
    # - fibonacci(17) = 1597



    printTaskLine(4)
    # Task 4 - Map and reduce
    #
    # - `map` function can be used to transform the elements of a list.
    # - `reduce` function can be used to combine the elements of a list into a single value.
    #
    # Part 1:
    #
    # - Using the `myList`as a starting point, use function `map` to calculate the cube of each element
    # - and then use the `reduce` function to calculate the sum of the cubes.
    #
    # Part 2:
    #
    # - Using functions `map` and `reduce`, find the smallest value for `f(x)=x^2-34*x+325`
    #   when the input values `x` are the values from `myList`.

    from functools import reduce

    myList: list[int] = [2, 3, 5, 7, 11, 13, 17, 19, 21, 23, 29]

    cubeSum: int = reduce(
        lambda x, y: x + y,
        map(
            lambda x: x ** 3,
            myList
        )
    )

    # or with a single line:
    cubeSum2: int = reduce(mySum, map(lambda x: x ** 3, myList))

    def f(x: int):
        return x*x - 34*x + 325

    smallestFuncValue: int = reduce(
        min,
        map(f, myList)
    )

    # or more explicitly:
    smallestFuncValue2: int = reduce(
        lambda x, y: x if x < y else y,
        map(lambda x: f(x), myList)
    )


    print(f"Sum of cubes:                        {cubeSum}")
    print(f"Smallest value of f(x)=x^2-34*x+325:    {smallestFuncValue}")

    # Example output:
    # ==============
    # Sum of cubes:                        61620
    # Smallest value of f(x)=x^2-34*x+325:    36



    printTaskLine(5)
    # Task 5 - Grouping and aggregating
    #
    # You are given a list of tuples representing sensor readings (`temperatureReadings`).
    # Each tuple contains a sensor name and a temperature reading.
    # You are also given a function `average` that returns the average value of the given list.
    #
    # Part 1:
    #
    # - Using the given readings and the `average` function, calculate the average temperature reading for each sensor
    #   and assign it as a dictionary to `averageTemperatures`.
    #   Any working solution is accepted, but you are encouraged to try to solve this using `groupby` function from itertools.
    #
    # Part 2:
    #
    # - Find the value for the highest average temperature.

    from functools import reduce
    from itertools import groupby  # itertools.groupby requires the list to be sorted

    temperatureReadings: list[tuple[str, float]] = [
        ("sensorA", 19.4), ("sensorB", 19.0), ("sensorA", 20.1), ("sensorC", 18.6),
        ("sensorC", 18.7), ("sensorB", 20.5), ("sensorC", 18.3), ("sensorA", 18.3),
        ("sensorA", 21.0), ("sensorB", 21.1), ("sensorC", 19.2), ("sensorB", 19.8)
    ]

    def average(values: list[float]) -> float:
        value_sum, value_count = reduce(
            lambda pair, elem: (pair[0] + elem, pair[1] + 1),
            values,
            (0.0, 0)
        )
        return value_sum / value_count

    # A map for the average temperatures for each sensor
    averageTemperatures: dict[str, float] = {
        name: average(map(lambda x: x[1], readingList))
        for name, readingList in groupby(sorted(temperatureReadings, key=lambda x: x[0]), key=lambda x: x[0])
    }
    # - groupby (in Python) works on sorted lists, so temperatureReadings is first sorted by the sensor name
    # - then the list is grouped by the sensor name
    # - the readingList is a list of tuples with the sensor name and the temperature
    # - for the average function the list of temperatures is extracted from the readingList
    # - then the average temperature is calculated for each sensor

    highestAverageTemperature: float = max(averageTemperatures.values())
    # - get the list of the dictionary values (without keys) as list with .values()
    # - find the highest temperature with the built-in function max


    # A "traditional" solution with mutable variables and a for loop could be done as well
    # but that solution is left to the reader as an additional exercise.
    # And the "functional" solution is closer to what would be naturally done using Spark later on.
    # Though the equivalent Scala code for this looks a lot cleaner and more readable than the Python code given here.


    print(f"type(averageTemperatures): {type(averageTemperatures)} - type(averageTemperatures['sensorA']): {type(averageTemperatures['sensorA'])}")
    print()
    print("Average temperatures per sensor:")
    for name, avg in averageTemperatures.items():
        print(f"{name}: {avg}")
    print(f"The highest average temperature: {highestAverageTemperature}")

    # Example output:
    # ==============
    # type(averageTemperatures): <class 'dict'> - type(averageTemperatures['sensorA']): <class 'float'>
    #
    # Average temperatures per sensor:
    # sensorA: 19.7
    # sensorB: 20.1
    # sensorC: 18.7
    # The highest average temperature: 20.1



    printTaskLine(6)
    # Task 6 - Code explanation
    #
    # Look at the `average` function that was given in task 5.
    #
    # If you are not used to functional programming, it might look quite strange and somewhat different than
    # the way you would "traditionally" write a function that calculates the average from a list of numbers
    # (when doing so without using any built-in functionalities of the programming language).
    #
    # - Explain in your own words how the `average` function works.
    # - Are there any benefits to this implementation compared to a straightforward implementation that
    #   separately calculates the sum and the count of the elements?
    # - What happens with this implementation if the input is an empty list?
    #   Given your solution to task 5 can this cause issues?

    # How the `average` function works:
    # - The function uses `reduce` function to iterate through the list and calculate the sum of the elements
    #   and the element count at the same time.
    #     - (0.0, 0)-tuple is used as an initial value. These tuples are referred to in the logic as `pair`
    #       and they represent the current element sum and the current count for the number of elements.
    #     - Each element (referred as `elem`) in the list is handled one-by-one, and a new tuple (`pair`) is created
    #       with a new sum and count that includes the current element.
    #     - The Python `reduce` is equivalent to Scala `foldLeft` when given the third parameter as the initial value.
    #       This allows the returned type to not have to match the types of the elements in the list.
    # - The returned average value is then calculated in the "usual" way, by dividing the element sum by the element count.
    #
    # Benefits of this approach?
    # - If the sum and the count are calculated separately, the list needs to be gone through twice,
    #   once for counting the elements and another time for counting the element sum. With the given implementation,
    #   the list only needs to be gone through once, which can save time when dealing with very large lists.
    #     - Depending on the implementation of the list, getting the element count might not require fully going through
    #       the list. But in general case, getting the count is a linear operation in regard to the number of elements.
    #
    # What happens with an empty list?
    # - If the input is an empty list, the average value calculation involves dividing by zero at the last line of the function.
    #   The Python implementation throws `ZeroDivisionError` in this case.
    # - With the given solution to task 5, the `average` function will never be called with an empty list.
    #   The used grouping guarantees that each group will have at least one appearance.



    printTaskLine(7)
    # Task 7 - Approximation for seventh root
    #
    # Write a function, `seventhRoot`, that returns an approximate value for the seventh root of the input.
    # Use the Newton's method, https://en.wikipedia.org/wiki/Newton%27s_method, with the initial guess of 1.
    # For the seventh root this Newton's method translates to:
    #
    # y_0 = 1
    # y_{n+1} = 1/7 * (6*y_n + x / y_n^6)
    #
    # where x is the input value and y_n is the guess for the cube root after n iterations.
    # For large enough k => y_k = x^(1/7)
    #
    # Example steps when x=128:
    #
    # y_0 = 1
    # y_1 = 1/7 * (6*1 + 128 / 1^6) = 19.143
    # y_2 = 1/7 * (6*19.143 + 128 / 19.143^6) = 16.408
    # y_3 = 1/7 * (6*16.408 + 128 / 16.408^6) = 14.064
    # ...
    #
    # You will have to decide yourself on what is the condition for stopping the iterations.
    # (you can add parameters to the function if you think it is necessary)
    #
    # Note, if your code is running for hundreds or thousands of iterations, you are either
    # doing something wrong or trying to calculate too precise values.
    # The example output has been calculated using less than 100 iterations in each case.

    # these could be adjusted if necessary
    MaxIterations: int = 1000
    MaxPrecision: float = 1.0E-7  # we should get at least the first 7 most significant digits correct

    def seventhRoot(x: float) -> float:
        def rootIter(guess: float, iterations: int) -> float:
            # stop the iterations if the fifth power of the guess is close enough to the input x
            # or the maximum number of iterations has been calculated
            if (
                abs(guess ** 7 - x) / abs(x) < MaxPrecision or
                iterations >= MaxIterations
            ):
                return guess
            else:
                return rootIter((6*guess + x / (guess**6)) / 7, iterations + 1)

        if x == 0:  # special case for x=0 to avoid division by zero
            return 0.0

        # start the iterations
        return rootIter(1.0, 0)


    print(f"Seventh root of 128:       {seventhRoot(128)}")
    print(f"Seventh root of 78125:     {seventhRoot(78125)}")
    print(f"Seventh root of 10^7:     {seventhRoot(1e7)}")
    print(f"Seventh root of 10^(-7):   {seventhRoot(1e-7)}")
    print(f"Seventh root of -2187:    {seventhRoot(-2187)}")
    print(f"Seventh root of 0:         {seventhRoot(0)}")

    # Example output:
    # (the exact values are not important, but the results should be close enough)
    # ==============
    #
    # Seventh root of 128:       2.0000000000000067
    # Seventh root of 78125:     5.000000000023987
    # Seventh root of 10^7:     10.000000000033177
    # Seventh root of 10^(-7):   0.10000000000010757
    # Seventh root of -2187:    -3.0000000000751648
    # Seventh root of 0:         0.0



    printTaskLine(8)
    # Task 8 - General programming background question
    #
    # For the final task of the first exercise, just answer the following questions:
    #
    # 1. What king of programming background do you have?
    #   (for example, how many courses have you taken or how many years of experience do you have)
    # 2. How did you find the use of the Databricks environment and the notebooks?
    #   - If you did not try the Databricks environment and did everything locally, explain why.
    # 3. How did you find these programming tasks? (easy, hard, interesting, annoying, ...)
    #
    # There are no correct answers to this task.
    # The answers might give the course staff a better understanding on the student's level of the programming skills.
    # Short answers are accepted, but you are free to write any constructive comments you can think of.

    # 1. No correct answer. Some programming skills are expected from everyone.
    # 2. No correct answer. Any suggestions to make working with the environment better are welcome.
    # 3. No correct answer.



    printAIQuestionTaskLine()
    # Use of AI and collaboration
    #
    # Using AI and collaborating with other students is allowed when doing the weekly exercises.
    # However, the AI use and collaboration should be documented.
    #
    # - Did you use AI tools while doing this exercise?
    #   - Did they help? And how did they help?
    # - Did you work with other students to complete the tasks?
    #   - Only extensive collaboration is expected to be reported. If you only got help
    #     for a couple of the tasks, you don't need to report it here.

    # - If you got significant help from an AI tool (either from the Assistant in Databricks or from another tool), it is expected that you mention it here.
    # - Also, if you worked together with other students for a significant portion of the exercise, it is expected that you mention it here.
    #
    # Using AI and collaboration is allowed and will not affect the grading (when it is reported).
    # But, it can appear suspicious if, for example, there are several identical submissions without any mention of collaboration or AI usage.



# Helper function to separate the task outputs from each other
def printTaskLine(taskNumber: int) -> None:
    print(f"======\nTask {taskNumber}\n======")

def printAIQuestionTaskLine() -> None:
    print("======\nAI and collaboration\n======")



if __name__ == "__main__":
    main()
