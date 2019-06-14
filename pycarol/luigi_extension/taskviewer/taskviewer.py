from ..utils import build_dag
def luigi_get_sons(task) -> list:
    """
    Returns a list of required tasks. This is used in build_dag
    Args:
        task: luigi Task

    Returns:
        l: list of luigi Task

    """
    return task.requires()

def get_dag_from_task(task:list) -> dict:
    """
    Wrapper around generic build_dag.
    Args:
        task: list of proper luigi tasks

    Returns:
        dag: dict encoding a DAG

    """
    dag = build_dag(task,luigi_get_sons)
    return dag
