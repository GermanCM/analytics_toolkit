# Analytics_toolkit
---

This is a repository to allocate internal utilities for several steps in an analytics pipeline, like data extraction, exploratory analysis, visualizations, data processing, etc.

This includes a battery of tests with which the developed modules should be tested, both from an operational and business-logical perspective.

The main idea of this repository could be something as follows:


![Alt text](/readme_files/toolkit_repo_schema.png "schema")


These helpers functions are divided into importable packages with modules, having several classes with helper functions.
The approach is object-oriented-programming, meaning that we can use it from a "main" module or from a notebook seamlessly and reuse it as much as possible. We can also use the concept of snippets, in case a code is a little standalone helper.
The main goal is to make it useful and easy to use, so class and methods definitions are important; it is proposed to follow some standard to enable a quick and easy preview as follows:

![Alt text](/readme_files/class_functions_definitions.gif "definitions preview")

<br>

Another important point is to make sure that we test the helpers, for which we can make use of some extensions in IDEs like VSCode via test frameworks like pytest:

![Alt text](/readme_files/testing_custom_helpers.gif "schema")

![Alt text](/readme_files/passed_tests_pytest.png)


In addition to the purely code oriented functions, we can include tools focused on data discovery, visualizations... which, although not being part of the automatic process, are part of the analytics pipeline: 

![Alt text](/pics/profile_report_2.png  "EDA example")


## About
© This library is developed at the BigData & Digital Innovation group at the EVO Bank.
