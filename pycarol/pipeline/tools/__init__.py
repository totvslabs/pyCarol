"""
This module hosts the class Pipe, which is used to interact with an
instantiated pipeline.
In each DS app, we expect to find a file named pipeline.py in folder app. In
this file, there will be an object named pipeline, from the class Pipe. In
order to initialize a Pipe object, we need to define a list of output tasks
and a dictionary containing the pipeline parameters. These parameters are
similar to the parameters one pass to luigi.build, however, in this case,
we use a list of non instantiated tasks.
In file test_tools.py there are some basic tests for this module.
"""
from pycarol.pipeline.tools._tools import Pipe