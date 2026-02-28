https://learn.microsoft.com/en-gb/azure/databricks/files/python-unit-tests

Python unit testing in the workspace
Azure Databricks provides a suite of tools to discover, run, and track Python unit tests directly in the workspace. Use the testing sidebar pane, inline execution glyphs, and a dedicated results pane to manage your tests without leaving the workspace.

Python unit testing tools are available when you have a valid Python test file open.

Valid Python test files
Azure Databricks follows pytest naming conventions to detect test files, classes and cases.

The following file naming patterns are recognized as valid test files:

test_*.py
*_test.py
The following naming conventions detect test classes and cases:

test-prefixed functions or methods outside of a class
test-prefixed functions or methods inside Test-prefixed classes (without an __init__ method)
Methods decorated with @staticmethod or @classmethod inside Test-prefixed classes
For example:

Python
class TestClass():
    def test_1(self):
        assert True

    def test_3(self):
        assert 4 == 3

def test_foo():
    assert "foo" == "bar"
Tests sidebar panel
When you open a valid Python test file, the Experiments icon. Tests sidebar pane automatically discovers tests in the current file. When you are inside an authoring context, test discovery covers all files in that context.

From the tests sidebar pane, you can:

Trigger test actions:
Play double icon. Run all tests
Refresh x icon. Run all failed tests
Refresh icon. Refresh tests
Monitor test status: View the Check circle icon. pass or X circle icon. fail status of each discovered test.
Filter tests: Filter the test list by name or status. Use the search bar to filter by text or click Filter icon. to filter by status.
Run individual tests: Execute specific tests directly from the pane. Hover over a test and click
