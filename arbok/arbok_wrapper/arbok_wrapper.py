import os
import time
import sys
from functools import wraps
from datetime import datetime
os.environ["BASEDIR"] = os.path.dirname(os.path.realpath(__file__)) + "/"
sys.path.append(os.environ["BASEDIR"])

from default_variable import defaults_transform

from arbok_emitter import (
    arbok_emitter_inputs,
    arbok_emitter_modelruns,
    arbok_emitter_results,
)
from instrumentation import Instrument

instrument = Instrument()


def defaults_transform(inputs, defaults):
    """
    Has to ability to replace variables for defaults or change any value to another value.
    ----------
    inputs : dict
        input dict for the models
    defaults : dict
        dict of tuples for each var name that is a replacement_value and values_to_replace.
        ex: {'x':[{'replacement_value': 'cactus jane', 'values_to_replace':None}, {'replacement_value': 'cactus jack', 'values_to_replace':'NA'}]}

    """
    inputsDefaulted = inputs
    for var in defaults:
        for defvals in defaults[var]:
            if inputs[var] == defvals["values_to_replace"]:
                inputsDefaulted[var] = defvals["replacement_value"]
            elif "All" == defvals["values_to_replace"]:
                inputsDefaulted[var] = defvals["replacement_value"]
    return inputsDefaulted


def arbok_wrapper(func):
    """
    Main wrapper function to log, emit and standardize input and output format
    Parameters
    ----------
    inputs : dict
        Variables required for the model and its values
    model_name : str
        Name of model wanting to be scored
    emitter: boolean
        Allows emittion to splunk and helios DBs
    logging_level: str
        Can be various different levels:
        None:
            Logging disabled
        info:
            General info logs, such as requests received and models ran etc.
        warning:
            Logs a message with level WARNING on this logger.  Used for when something needs to be noted but is not
            causing any actual issues or errors, for example out of date version of a package
        error:
           Logs a message with level ERROR on this logger.  This is causing issues to models and or may be affecting
           the models output / performance
        critical:
            Logs a message with level CRITICAL on this logger.  Models contain broken code or syntax errors or other
            code that will cause underwriting to fail or provide inaccurate output.
        debug:
            Logs a message with level DEBUG. Debugging will turn on all logging.
    """

    @wraps(func)
    # arbok used wrap, and its super effective
    def wrapper(inputs, model_name, emitter=False, log_level=None, **kwargs):
        print(emitter)
        print(log_level)
        now = now = datetime.now()
        current_date = now.strftime("%Y-%m-%d %H:%M:%S.%f")
        emitter_tags = {"model_name": model_name}
        if "execution_uuid" in kwargs:
            execution_uuid = kwargs["execution_uuid"]
            emitter_tags["execution_uuid"] = execution_uuid
        else:
            execution_uuid = None

        if "flow_uuid" in kwargs:
            flow_uuid = kwargs["flow_uuid"]
            emitter_tags["flow_uuid"] = flow_uuid
        else:
            flow_uuid = None
        # Emit inputs to helios inputs
        if emitter:
            emitterInputsJSON = {
                "inputs": inputs,
                "created_at": current_date,
                "updated_at": current_date,
                "uuid": execution_uuid,
                "model_name": model_name,
                "created_by": "arbok",
            }
        arbok_emitter_inputs(emitterInputsJSON, emitter_tags)
        # Input pre-processing and manipulation. Any changes to inputs prior to a model run should be done here
        if "variable_defaults" in kwargs:
            inputs = defaults_transform(inputs, kwargs["variable_defaults"])
        # Function call to run a model, intent is for it to return a result and an error if applicable

        start = time.time()
        result, err = func(inputs, model_name)
        end = time.time()
        execution_time = end - start
        # result emission
        # Output of function.  If you wish to manipulate the output of the logs this is where that can be done
        outputJSON = {
            "model_name": model_name,
            "execution_time": execution_time,
            "result": result,
            "error": err,
        }

        # Formulate kapow emitter output. Some models will not have uuids so we can add those in here
        emitter_tags = {"model_name": model_name}

        if "execution_uuid" in kwargs:
            execution_uuid = kwargs["execution_uuid"]
            outputJSON["execution_uuid"] = execution_uuid
            emitter_tags["execution_uuid"] = execution_uuid
        else:
            execution_uuid = None

        if "flow_uuid" in kwargs:
            flow_uuid = kwargs["flow_uuid"]
            outputJSON["flow_uuid"] = flow_uuid
            emitter_tags["flow_uuid"] = flow_uuid
        else:
            flow_uuid = None

        # Log outputs here:
        if log_level:
            # Add sub log levels here, severe, DEBUG etc.
            instrument.Logger.error(outputJSON)

        # Kapow emitter section.
        # creating emitter data for helios
        # First element is a list of what you want logged and the second is a list of tags
        if emitter:
            emitterResultsJSON = {
                "result": result,
                "response": result,
                "created_at": current_date,
                "updated_at": current_date,
                "uuid": execution_uuid,
                "execution_time": execution_time,
                "result": result,
            }
        arbok_emitter_results(
            emitterResultsJSON, emitter_tags
        )  # need to get all columns info
        if emitter:
            emitterModelRunsJSON = {
                "model_name": model_name,
                "execution_time": execution_time,
                "result": result,
                "error": err,
            }
        arbok_emitter_modelruns(
            emitterModelRunsJSON, emitter_tags
        )  # need to get all columns info
        return result

    return wrapper
