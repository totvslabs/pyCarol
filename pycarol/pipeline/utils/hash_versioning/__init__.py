"""
One of the main purposes of using pipeline managers like Luigi, Dask, Airflow is
to store the result of heavy computation tasks for further reuse. It happens
often that the process of developing the whole pipeline is very dynamic. The
code of a task can be explicitly changed or the task behavior can change due
to external changes, like an update of a python package. It turns out that
one cannot be certain that the stored result of the task is up-to-date with
the code. This may invalidate any test/validation process.
The solution we propose here is to generate a hash of the code of each task.
In the case of using Luigi, we assume that if the target was generated with the
same task parameters and it has the same hash code we can safely use this
target (of course, excluding hash collision hypothesis). We observe that some
pipeline managers also keep track of the data/inputs hash. This module can
cooperate with these managers as well.
Finally, the practical objective of this module is to obtain a hash code of a
given function statically, i.e., without computing the python code itself.
Due to its statical nature, some case cannot be supported, like import and
functions definitions inside if statements.
The method get_bytecode_tree performs all the hard work and return nested lists
of bytecodes that may be useful for inspection.
The method get_function_hash uses get_bytecode_tree to return a hash for a
given function.
"""

from .hash_versioning import get_bytecode_tree, get_function_hash
from pycarol.pipeline.utils.hash_versioning.inspect_bytecode import \
    get_name_of_CALL_FUNCTION
