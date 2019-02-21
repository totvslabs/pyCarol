""" Interpret validation log to generate an HTML report

Input Format:
    {
        datamodel:STR,
        domain:STR,
        number_of_entries:INT,
        [validation_rule_name]:{
            success: BOOL,
            msg: STR
        },
        log:{
            fields:{
                [field_name]:{
                    found: BOOL,
                    [validation_rule_name]:{
                        success: BOOL
                        msg: STR
                    }
                    type:{
                        success: BOOL
                    }
                    *nested:{}
                }
            }
        }
    }
"""
import luigi
from ...luigi_extension.test import task_execution_debug
import missingno as msno
import matplotlib.pyplot as plt

from IPython import get_ipython
from IPython.display import display, Markdown, Javascript
import logging


# TODO: Clickable buttons that execute the function
SUCCESS_COLOR = "green"
WARNING_COLOR = "orange"
ERROR_COLOR = "red"
NULL_COLOR = "gray"


def insert_cell(text=''):
    get_ipython().set_next_input(text)


def get_tasks_with(execution, substring):
    val_tasks = {}
    for task in execution.task_history:
        if task[1] == 'DONE':
            task_name = task[0].__class__.__name__
            if substring in task_name:
                val_tasks.update({task_name: task[0]})
    return val_tasks


def add_tag(msg, color=None, tag=None, tag_options=None, style=None):
    if color is not None:
        msg = f'<font color="{color}">{msg}</font>'
    if tag is not None:
        if tag_options is None:
            tag_options = {}
        if color is not None:
            tag_options.update({'color': color})
        if style is not None:
            style = ";".join([f'{k}:{v}' for k, v in style.items()])
            style = f'style="{style};"'
        tag_options = " ".join([f'{k}="{v}"' for k, v in tag_options.items()])
        msg = f'<{tag} {tag_options} {style}>{msg}</{tag}>'
    return msg


def print_m(msg, color=None, tag=None, tag_options=''):
    msg = add_tag(msg, color, tag, tag_options)
    display(Markdown(msg))


def print_null_data(df, columns=None):
    if columns is not None:
        df = df[columns]
    # TODO: Break columns (max = 20)
    msno.matrix(df=df)
    display(plt.show())


def print_dm_validation(logs, data):
    # TODO: Organize by fields
    log = logs['log']

    def get_validation_text(log, data, field_name, color=SUCCESS_COLOR):
        text = '<ul>'
        if 'found' in log:
            val = log.pop('found')
            if not val:
                if log['mandatory']:
                    color = ERROR_COLOR
                    text += add_tag('Mandatory field not found.', color=ERROR_COLOR, tag='li')
                else:
                    color = WARNING_COLOR
                    text += add_tag('Field not found.', color=WARNING_COLOR, tag='li')
        if 'type' in log:
            val = log.pop('type')
            if not val['success']:
                color = ERROR_COLOR
                text += add_tag('Wrong type.', color=ERROR_COLOR, tag='li')

        if 'validation' in log:
            val = log.pop('validation')[0]
            if not val['success']:
                color = ERROR_COLOR
                text += add_tag('Error with validation rule.', color=ERROR_COLOR, tag='li')
                text += add_tag('<ul>')
                text += add_tag(val['msg'], color=ERROR_COLOR, tag='li')
                text += add_tag('</ul>')

        if 'uniformity' in log:
            val = log.pop('uniformity')
            if not val['success']:
                color = ERROR_COLOR
                text += add_tag('Error with uniformity of data.', color=ERROR_COLOR, tag='li')
                text += add_tag('<ul>')
                text += add_tag(val['msg'], color=ERROR_COLOR, tag='li')
                text += add_tag('</ul>')

        if 'custom_validation' in log:
            val = log.pop('custom_validation')
            if not val['success']:
                color = ERROR_COLOR
                text += add_tag('Error with custom validation rule.', color=ERROR_COLOR, tag='li')
                text += add_tag('<ul>')
                text += add_tag(val['msg'], color=ERROR_COLOR, tag='li')
                text += add_tag('</ul>')

        text += add_tag('</ul>')
        text = '' if text == '<ul></ul>' else text

        return text, color

    def print_field(log, data, calling_field_name=None, color=NULL_COLOR):
        text = '<ul>'
        for field_name, field in log['fields'].items():
            color = SUCCESS_COLOR
            field_name = field_name.lower()  # Get Carol field name
            aux_field_name = calling_field_name + '_' + field_name if calling_field_name is not None else field_name
            if 'nested' in field:
                validation_text, color = print_field(field['nested'], data, aux_field_name, color)
            else:
                validation_text, color = get_validation_text(field.copy(), data=data, field_name=aux_field_name,
                                                             color=color)

            text += add_tag(add_tag(field_name, style={'background-color': color}, tag='button'), tag='li')
            text += validation_text

        text += '</ul>'

        return text, color

    text, _ = print_field(log, data)
    print_m(text)


def display_dm_report(val, dm_name, ingestion=True):
    """
        val = Luigi Extension Validation Task
        dm_name = str DataModel name
    """
    logs = val.load()
    # TODO: List of data models
    print_m(f'## {logs["datamodel"].capitalize()} '
            f'Data Model <a id="{dm_name}"></a>')
    df = val.requires()[0].load()
    print_m('### Validation:')
    print_dm_validation(val.load(), data=df[0:10])
    if ingestion:
        print_m(f'### Ingestion')
        print_m(f'#### Shape: {df.shape}')
        display(df.head())
        print_m('### Null data:')
        print_null_data(df)
    del df


def display_report(execution):
    print_m(f"# Carol App Validation - {execution.task.domain}")
    print_m('### Navigation')
    dm_val_tasks = get_tasks_with(execution, 'DataModelValidation')
    text = ''
    for dm in dm_val_tasks.keys():
        dm_name = dm.replace("DataModelValidation", "")
        text += f'* [{dm_name}](#{dm})\n'
    print_m(text)
    for val_task_name, val in dm_val_tasks.items():
        val = dm_val_tasks[val_task_name]
        display_dm_report(val, val_task_name)
    return dm_val_tasks


def generate_report(pipeline_task, domain=None, dm='all', mute_luigi=True):
    print('Running luigi validation pipeline...')
    if mute_luigi:
        luigi.interface.setup_interface_logging.has_run = True
    params = dict(operation='dm_validation', task=dm)
    if domain is not None:
        params.update({'domain': domain})
    execution = task_execution_debug(pipeline_task, params)
    if not execution.success:
        print('Error with pipeline.')
    # TODO: Check if any task was not suceeded

    return display_report(execution)
